// Included inside akkengine_latency_benchmark.cpp's anonymous namespace.
namespace sst = akkaradb::engine::sst;

class StorageProfileWindow {
    fs::path control_;
    #ifdef _WIN32
    HANDLE started_ = nullptr, finished_ = nullptr;
    #endif

    public:
        explicit StorageProfileWindow(const fs::path& control)
            : control_{control} {
            #ifdef _WIN32
            const auto name = L"Local\\AkkStorageProfile-" + std::to_wstring(GetCurrentProcessId()) + L"-" + std::to_wstring(GetCurrentThreadId()); started_ =
                CreateEventW(nullptr, TRUE, FALSE, (name + L"-started").c_str()); finished_ = CreateEventW(nullptr, TRUE, FALSE, (name + L"-finished").c_str());
            if (!started_ || !finished_) {
                if (started_) { CloseHandle(started_); }
                if (finished_) { CloseHandle(finished_); }
                throw std::runtime_error("could not create profile window events");
            } try {
                if (fs::exists(control.string() + ".ready") || fs::exists(control.string() + ".go") || fs::exists(control.string() + ".done")) {
                    throw std::runtime_error("profile control files already exist; use a fresh prefix");
                }
                if (control.has_parent_path()) { fs::create_directories(control.parent_path()); }
                std::ofstream ready{control.string() + ".ready"};
                ready.exceptions(std::ios::failbit | std::ios::badbit);
                ready << GetCurrentProcessId() << ' ' << GetCurrentThreadId() << '\n';
                ready.close();
                const auto deadline = Clock::now() + std::chrono::seconds{60};
                while (!fs::exists(control.string() + ".go")) {
                    if (Clock::now() >= deadline) { throw std::runtime_error("profile sampler did not attach within 60s"); }
                    std::this_thread::sleep_for(std::chrono::milliseconds{10});
                }
                SetEvent(started_);
            }
            catch (...) {
                CloseHandle(started_);
                CloseHandle(finished_);
                throw;
            }
            #else
            (void)control;
            throw std::runtime_error("Windows profiling is unavailable");
            #endif
        }

        StorageProfileWindow(const StorageProfileWindow&) = delete;
        StorageProfileWindow& operator=(const StorageProfileWindow&) = delete;

        ~StorageProfileWindow() {
            #ifdef _WIN32
            SetEvent(finished_); CloseHandle(started_); CloseHandle(finished_);
            #endif
        }

        void finish() {
            #ifdef _WIN32
            SetEvent(finished_);
            #endif
            std::ofstream done{control_.string() + ".done"};
            done.exceptions(std::ios::failbit | std::ios::badbit);
            done << "done\n";
            done.close();
        }
};

class StorageModuleStore {
    TemporaryDirectory directory_;
    std::unique_ptr<sst::SSTManager> disk_;
    std::unique_ptr<memtable::MemTable> memory_;
    uint64_t sequence_ = 0;

    public:
        double profiledFlushNs = 0;
        uint64_t profiledFlushRecords = 0;

        explicit StorageModuleStore(const Config& c, bool manualFlush = true) {
            if (c.sst) {
                sst::SSTManager::Options options;
                options.compactionMode = sst::SSTCompactionMode::DISABLED;
                disk_ = sst::SSTManager::create(directory_.path() / "sst", options);
            }
            memtable::MemTable::Options options;
            options.shardCount = 1;
            options.backendFactory = backendFactory(c.backend);
            options.sequenceOrderedVersions = true;
            options.flushMode = c.sst && !manualFlush ? memtable::MemTableFlushMode::BYTES_PER_SHARD : memtable::MemTableFlushMode::MANUAL_ONLY;
            options.thresholdBytesPerShard = c.sst ? 4 * 1024 * 1024 : 0;
            options.flushInputMode = memtable::MemTableFlushInputMode::STREAMING;
            if (disk_) {
                const bool profileFlush = c.profileOperation == "flush";
                const auto control = c.profileControl;
                options.onFlushStream = [this, profileFlush, control](size_t count, auto records) {
                    std::unique_ptr<StorageProfileWindow> window;
                    if (profileFlush) { window = std::make_unique<StorageProfileWindow>(control); }
                    const auto start = Clock::now();
                    disk_->flush(count, std::move(records));
                    if (profileFlush) {
                        profiledFlushNs = elapsedNs(start, Clock::now());
                        profiledFlushRecords = count;
                        window->finish();
                    }
                };
            }
            memory_ = memtable::MemTable::create(options);
        }

        void put(const Key& key, const std::vector<uint8_t>& value) { memory_->put(key, value, ++sequence_); }
        void remove(const Key& key) { memory_->remove(key, ++sequence_); }

        void flush() {
            memory_->forceFlush();
            memory_->throwIfFlushFailed();
            if (disk_) { disk_->throwIfBackgroundFailed(); }
        }

        void seed(const std::vector<Key>& keys, const std::vector<uint8_t>& value, bool persist = true) {
            for (const auto& key : keys) { put(key, value); }
            if (persist && disk_) { flush(); }
        }

        std::optional<std::vector<uint8_t>> get(const Key& key) const {
            sst::SSTManager::ReadSnapshot disk;
            auto memory = memory_->getPinnedWithSnapshot(
                key,
                [&] {
                    if (disk_) { disk = disk_->readSnapshot(); }
                    return UINT64_MAX;
                }
            );
            if (disk_ && (!memory || disk.maxSequence() > memory->view.seq())) {
                auto stored = disk.get(key);
                if (stored && (!memory || stored->seq > memory->view.seq())) {
                    return stored->isTombstone() ? std::nullopt : std::optional{std::move(stored->value)};
                }
            }
            if (!memory || memory->view.isTombstone()) { return std::nullopt; }
            const auto value = memory->view.value();
            return std::vector<uint8_t>{value.begin(), value.end()};
        }

        uint64_t scan(const Key& start, const Key& end, size_t expected) const {
            memtable::MemTable::KeyRange range{{start.begin(), start.end()}, {end.begin(), end.end()}};
            sst::SSTManager::ReadSnapshot disk;
            auto memory = memory_->iteratorWithSnapshot(
                range,
                [&] {
                    if (disk_) { disk = disk_->readSnapshot(); }
                    return UINT64_MAX;
                }
            );
            auto stored = disk.scanIter(start, end, UINT64_MAX, true);
            auto a = memory.next();
            auto b = stored.next();
            size_t rows = 0;
            uint64_t checksum = 0;
            while (a || b) {
                const int comparison = !a ? 1 : !b ? -1 : a->compareKey(b->key);
                const bool useMemory = comparison < 0 || (comparison == 0 && a->seq() >= b->seq);
                const bool tombstone = useMemory ? a->isTombstone() : b->isTombstone();
                if (!tombstone) {
                    const auto value = useMemory ? a->value() : std::span<const uint8_t>{b->value};
                    checksum += value.size() + value.front() + value.back();
                    ++rows;
                }
                if (comparison <= 0) { a = memory.next(); }
                if (comparison >= 0) { b = stored.next(); }
            }
            if (rows != expected) { throw std::runtime_error("module scan returned the wrong row count"); }
            return checksum;
        }

        void verify(const std::vector<Key>& keys, const std::vector<uint8_t>& value) const {
            for (const auto& key : keys) { if (get(key) != std::optional{value}) { throw std::runtime_error("module seed verification failed"); } }
            (void)scan(keys.front(), makeKey(2 * keys.size()), keys.size());
        }

        void recordState(CsvReport& report, std::string_view prefix) const {
            report.setting(std::string{prefix} + "_memory_bytes", memory_->approxSize());
            const auto state = memory_->snapshot();
            report.setting(std::string{prefix} + "_memory_puts", state.putsApplied);
            report.setting(std::string{prefix} + "_memory_removes", state.removesApplied);
            report.setting(std::string{prefix} + "_memory_flushes", state.flushesCompleted);
            report.setting(std::string{prefix} + "_memory_immutables", state.immutableTables);
            if (disk_) {
                size_t files = 0;
                uint64_t bytes = 0;
                for (const auto& level : disk_->levelStats()) {
                    files += level.fileCount;
                    bytes += level.bytes;
                }
                report.setting(std::string{prefix} + "_sst_files", files);
                report.setting(std::string{prefix} + "_sst_bytes", bytes);
            }
        }
};

void runStorageModules(const Config& c, CsvReport& report) {
    #ifdef _WIN32
    if (c.storageCpu >= 0) {
        const auto mask = DWORD_PTR{1} << static_cast<unsigned>(c.storageCpu);
        DWORD_PTR allowed = 0, system = 0;
        if (!GetProcessAffinityMask(GetCurrentProcess(), &allowed, &system) || (allowed & mask) == 0 || !SetProcessAffinityMask(GetCurrentProcess(), mask)) {
            throw std::runtime_error("requested storage benchmark CPU is unavailable");
        }
    }
    #endif
    const auto seedValue = makeValue(c.valueBytes, 0);
    const auto updatedValue = makeValue(c.valueBytes, 1);
    std::vector<Key> keys, misses, inserts, warmKeys;
    for (int i = 0; i < c.keys; ++i) {
        keys.push_back(makeKey(2 * static_cast<uint64_t>(i)));
        misses.push_back(makeKey(2 * static_cast<uint64_t>(i) + 1));
    }
    std::vector<size_t> order(keys.size());
    std::iota(order.begin(), order.end(), 0);
    std::mt19937 random{0xA44A5EED};
    std::shuffle(order.begin(), order.end(), random);
    const auto keyAt = [&](size_t i) -> const Key& { return keys[order[i % order.size()]]; };
    const auto newCount = static_cast<size_t>(c.iterations) + static_cast<size_t>(c.warmup);
    for (size_t i = 0; i < newCount; ++i) { inserts.push_back(makeKey(2ULL * keys.size() + i)); }
    std::shuffle(inserts.begin(), inserts.end(), random);
    for (int i = 0; i < c.warmup; ++i) { warmKeys.push_back(makeKey(2ULL * keys.size() + newCount + static_cast<size_t>(i))); }
    const size_t rangeCount = keys.size() - static_cast<size_t>(c.scanRows) + 1;
    std::vector<size_t> scanOrder(rangeCount);
    std::iota(scanOrder.begin(), scanOrder.end(), 0);
    std::mt19937 scanRandom{0xA44A5EEE};
    std::shuffle(scanOrder.begin(), scanOrder.end(), scanRandom);
    std::vector<Key> scanEnds;
    for (size_t i = 0; i < rangeCount; ++i) { scanEnds.push_back(makeKey(2 * (i + static_cast<size_t>(c.scanRows)))); }
    const auto baseline = timerBaseline();
    report.recordBaseline(baseline);
    report.setting("storage_get_result", "owning value copy, including result destruction");
    report.setting("storage_value_pattern", "shared fixed payload: little-endian version u64 + repeated 0x5a");
    report.setting("storage_key_pattern", "even indexes present; odd indexes absent (last odd index is beyond the upper bound)");
    report.setting("storage_write_workload", "equal active MemTables; manual flush; warmup uses disjoint keys; fresh store per pass");
    report.setting("storage_read_workload", "get/scan: active memory in both modes; *.sst: persisted warm SST only");
    report.setting("storage_flush_input", "fresh unflushed batch; input seeding excluded");
    report.setting("storage_ingest_boundary", "empty store -> apply all records -> forceFlush completion; SST auto-flush at 4 MiB");
    std::cout << "Storage contract v2: equal active MemTables; API writes exclude persistence.\n" <<
        "*.sst measures persisted reads separately; ingest batches include completion.\n" << "backend=" << c.backend << "; SST=" << c.sst << "; keys=" << c.keys
        << "; iterations=" << c.iterations << "; empty timer pair median_ns=" << baseline.median << '\n' << std::flush;

    if (c.profileOperation == "flush") {
        report.stage("profile flush worker");
        auto store = std::make_unique<StorageModuleStore>(c, true);
        store->seed(keys, seedValue, false);
        store->flush();
        report.setting("profile_scope", "SST persistence callback thread; excludes caller queue/wait/rotation");
        report.setting("profile_elapsed_ns", store->profiledFlushNs);
        report.setting("profile_operations", 1);
        report.setting("profile_records", store->profiledFlushRecords);
        store->verify(keys, seedValue);
        store.reset();
        report.finish();
        std::cout << "Verification: passed; diagnostic flush profile complete\n";
        return;
    }

    std::vector<std::string_view> names{"get", "get.miss", "scan", "put.insert", "put.update", "remove"};
    if (c.sst) { names.insert(names.end(), {"get.sst", "get.miss.sst", "scan.sst"}); }
    printTableHeader();
    bool seeded = false;
    for (const auto name : names) {
        if (!c.profileOperation.empty() && c.profileOperation != name) { continue; }
        report.stage(name);
        const bool writes = name.starts_with("put.") || name == "remove";
        const bool persisted = name.ends_with(".sst");
        std::unique_ptr<StorageModuleStore> store;
        const auto prepare = [&] {
            store.reset();
            store = std::make_unique<StorageModuleStore>(c, true);
            store->seed(keys, seedValue, false);
            if (writes) { store->seed(warmKeys, seedValue, false); }
            if (persisted) { store->flush(); }
            store->verify(keys, seedValue);
            if (!seeded) {
                store->recordState(report, "seed");
                report.setting("verified_distinct_keys", keys.size());
                seeded = true;
            }
        };
        const auto operation = [&](size_t i, bool warm) -> uint64_t {
            if (name == "get" || name == "get.sst") { return observe(store->get(keyAt(i))); }
            if (name == "get.miss" || name == "get.miss.sst") {
                if (store->get(misses[order[i % order.size()]])) { throw std::runtime_error("missing-key read unexpectedly hit"); }
                return 1;
            }
            if (name == "scan" || name == "scan.sst") {
                const auto first = scanOrder[i % scanOrder.size()];
                return store->scan(keys[first], scanEnds[first], static_cast<size_t>(c.scanRows));
            }
            if (name == "put.insert") { store->put(inserts[warm ? static_cast<size_t>(c.iterations) + i : i], updatedValue); }
            else if (name == "put.update") { store->put(warm ? warmKeys[i] : keyAt(i), updatedValue); }
            else { store->remove(warm ? warmKeys[i] : keyAt(i)); }
            return 1;
        };
        const auto verify = [&] {
            if (!writes) { return; }
            // Verification/persistence is outside the API timer. End-to-end
            // durable cost is measured independently from an empty store below.
            store->flush();
            const auto count = name == "put.insert" ? static_cast<size_t>(c.iterations) : std::min(static_cast<size_t>(c.iterations), keys.size());
            for (size_t i = 0; i < count; ++i) {
                const auto actual = store->get(name == "put.insert" ? inserts[i] : keyAt(i));
                if (name == "remove" ? actual.has_value() : actual != std::optional{updatedValue}) {
                    throw std::runtime_error("module write verification failed");
                }
            }
        };
        const auto warm = [&] { for (int i = 0; i < c.warmup; ++i) { (void)operation(static_cast<size_t>(i), true); } };
        if (!c.profileOperation.empty()) {
            prepare();
            warm();
            store->recordState(report, std::string{name} + "_profile_before");
            StorageProfileWindow window{c.profileControl};
            const auto start = Clock::now();
            uint64_t checksum = 0;
            for (int i = 0; i < c.iterations; ++i) { checksum += operation(static_cast<size_t>(i), false); }
            const auto duration = elapsedNs(start, Clock::now());
            window.finish();
            observedChecksum = checksum;
            report.setting("profile_scope", "calling thread only; no automatic flush during API operations");
            report.setting("profile_elapsed_ns", duration);
            report.setting("profile_operations", c.iterations);
            verify();
            store.reset();
            report.finish();
            std::cout << "Verification: passed; diagnostic operation profile complete\n";
            return;
        }

        akkaradb::benchmark::Measurements result{std::vector<double>(static_cast<size_t>(c.iterations), 0.0)};
        for (int pass = 0; pass < 2; ++pass) {
            prepare();
            warm();
            store->recordState(report, std::string{name} + (pass == 0 ? "_bulk_before" : "_latency_before"));
            if (pass == 0) {
                std::atomic_signal_fence(std::memory_order_seq_cst);
                const auto start = Clock::now();
                for (int i = 0; i < c.iterations; ++i) { result.checksum += operation(static_cast<size_t>(i), false); }
                std::atomic_signal_fence(std::memory_order_seq_cst);
                result.bulkNs = elapsedNs(start, Clock::now());
            }
            else {
                for (int i = 0; i < c.iterations; ++i) {
                    std::atomic_signal_fence(std::memory_order_seq_cst);
                    const auto start = Clock::now();
                    const auto checksum = operation(static_cast<size_t>(i), false);
                    std::atomic_signal_fence(std::memory_order_seq_cst);
                    const auto end = Clock::now();
                    result.samples[static_cast<size_t>(i)] = elapsedNs(start, end);
                    result.checksum += checksum;
                }
            }
            store->recordState(report, std::string{name} + (pass == 0 ? "_bulk_after" : "_latency_after"));
            verify();
        }
        observedChecksum = result.checksum;
        report.recordSamples(name, result.samples);
        const auto metrics = akkaradb::benchmark::summarize(result.samples, result.bulkNs);
        report.recordSummary(name, metrics, result.checksum, {});
        std::cout << std::left << std::setw(24) << name << std::right << std::fixed << std::setprecision(1) << std::setw(16) << metrics.meanNs << std::setw(16)
            << metrics.opsPerSecond << std::setw(16) << metrics.p50 << std::setw(16) << metrics.p90 << std::setw(16) << metrics.p99 << std::setw(16) << metrics.
            p999 << '\n' << std::flush;
    }

    report.stage("end-to-end ingest batches");
    std::ofstream batches{report.directory() / "batches.csv", std::ios::binary};
    batches.exceptions(std::ios::failbit | std::ios::badbit);
    batches.imbue(std::locale::classic());
    batches << std::setprecision(17);
    batches << "sample_index,records,apply_ns,drain_ns,total_ns,completion\r\n";
    for (int i = 0; i < c.flushSamples; ++i) {
        StorageModuleStore batch{c, false};
        const auto start = Clock::now();
        for (const auto index : order) { batch.put(keys[index], seedValue); }
        const auto applied = Clock::now();
        batch.flush();
        const auto completed = Clock::now();
        batches << i << ',' << keys.size() << ',' << elapsedNs(start, applied) << ',' << elapsedNs(applied, completed) << ',' << elapsedNs(start, completed) <<
            ',' << (c.sst ? "durable_sst" : "memory_applied") << "\r\n";
        batch.verify(keys, seedValue);
    }
    batches.close();
    if (c.sst) {
        report.stage("flush batches");
        std::ofstream samples{report.directory() / "flushes.csv", std::ios::binary};
        samples.exceptions(std::ios::failbit | std::ios::badbit);
        samples.imbue(std::locale::classic());
        samples << std::setprecision(17);
        samples << "sample_index,records,elapsed_ns,ns_per_record\r\n";
        std::vector<double> durations;
        for (int i = 0; i < c.flushSamples; ++i) {
            StorageModuleStore batch{c, true};
            batch.seed(keys, seedValue, false);
            const auto start = Clock::now();
            batch.flush();
            const auto duration = elapsedNs(start, Clock::now());
            samples << i << ',' << keys.size() << ',' << duration << ',' << duration / static_cast<double>(keys.size()) << "\r\n";
            durations.push_back(duration);
            batch.verify(keys, seedValue);
            batch.recordState(report, "flush_" + std::to_string(i));
        }
        samples.close();
        report.setting("flush_batch_mean_ns", std::accumulate(durations.begin(), durations.end(), 0.0) / durations.size());
    }
    report.finish();
    std::cout << "Verification: passed; storage module suite complete\n";
}
