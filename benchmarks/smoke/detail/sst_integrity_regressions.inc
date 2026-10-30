// The configurable backend pauses real engine operations without production test hooks.
struct SstIntegrityGate {
    enum class Boundary { NONE, MISS, ITERATOR, PUT };
    std::mutex mutex;
    std::condition_variable cv;
    Boundary boundary = Boundary::NONE;
    bool entered = false, released = false;

    void arm(Boundary value) { std::lock_guard lock{mutex}; boundary = value; entered = false; released = false; }
    void pause(Boundary value) {
        std::unique_lock lock{mutex};
        if (boundary != value) { return; }
        boundary = Boundary::NONE; entered = true; cv.notify_all();
        if (!cv.wait_for(lock, std::chrono::seconds{10}, [&] { return released; })) { throw std::runtime_error("SST test gate timed out"); }
    }
    bool wait() { std::unique_lock lock{mutex}; return cv.wait_for(lock, std::chrono::seconds{5}, [&] { return entered; }); }
    void release() { std::lock_guard lock{mutex}; released = true; cv.notify_all(); }
};

class SstIntegrityBackend final : public akkaradb::engine::memtable::IMemTable {
    using Boundary = SstIntegrityGate::Boundary;
    std::shared_ptr<SstIntegrityGate> gate_;
    akkaradb::engine::memtable::SkipListMemTable inner_;
    static akkaradb::core::ArenaGenerator<akkaradb::core::RecordView> materialized(std::vector<akkaradb::core::RecordView> rows) {
        for (const auto& row : rows) { co_yield row; }
    }
public:
    explicit SstIntegrityBackend(std::shared_ptr<SstIntegrityGate> gate) : gate_{std::move(gate)} {}
    akkaradb::core::Status put(akkaradb::core::ByteView key, akkaradb::core::ByteView value, uint64_t seq,
        uint8_t flags, uint64_t fp, uint64_t mini) override {
        gate_->pause(Boundary::PUT);
        return inner_.put(key, value, seq, flags, fp, mini);
    }
    bool get(akkaradb::core::ByteView key, uint64_t seq, akkaradb::core::RecordView* out) const override {
        const bool found = inner_.get(key, seq, out);
        if (!found) { gate_->pause(Boundary::MISS); }
        return found;
    }
    akkaradb::core::ArenaGenerator<akkaradb::core::RecordView> iterator(
        akkaradb::core::ByteView start, akkaradb::core::ByteView end, uint64_t seq) const override {
        std::vector<akkaradb::core::RecordView> rows;
        for (const auto& row : inner_.iterator(start, end, seq)) { rows.push_back(row); }
        gate_->pause(Boundary::ITERATOR);
        return materialized(std::move(rows));
    }
    void freeze() override { inner_.freeze(); }
    size_t sizeBytes() const override { return inner_.sizeBytes(); }
    size_t entryCount() const override { return inner_.entryCount(); }
};

void runSstPromotionRegression() {
    using akkaradb::engine::AkkEngine;
    for (int api = 0; api < 3; ++api) {
        for (const bool deleted : {false, true}) {
            const auto dir = fs::current_path() / ".bench-tmp" / ("sst-promotion-" + std::to_string(api) + (deleted ? "-delete" : "-put"));
            std::error_code ec; fs::remove_all(dir, ec);
            auto gate = std::make_shared<SstIntegrityGate>();
            auto options = persistentOptions(dir);
            options.runtime.sstPromoteReads = true;
            options.memtable.backendFactory = [gate] { return std::make_unique<SstIntegrityBackend>(gate); };
            auto database = AkkEngine::open(options);
            const auto key = bytes("promoted-key");
            const std::vector<uint8_t> oldValue{'o'}, newValue{'n'};
            database->put(key, oldValue); database->forceFlush();
            gate->arm(SstIntegrityGate::Boundary::MISS);
            std::exception_ptr readFailure;
            std::thread reader{[&] {
                try {
                    std::vector<uint8_t> out;
                    if (api == 0) { require(database->get(key) == std::optional{oldValue}, "paused get lost its visible SST version"); }
                    else if (api == 1) { require(database->getInto(key, out) && out == oldValue, "paused getInto returned the wrong SST version"); }
                    else {
                        akkaradb::core::BufferArena arena; std::span<const uint8_t> result;
                        require(database->getIntoArena(key, arena, result) && std::ranges::equal(result, oldValue), "paused arena read returned the wrong SST version");
                    }
                } catch (...) { readFailure = std::current_exception(); }
            }};
            const bool entered = gate->wait();
            try {
                require(entered, "reader did not reach the promotion race");
                if (deleted) { database->remove(key); } else { database->put(key, newValue); }
                database->forceFlush();
            } catch (...) { gate->release(); reader.join(); throw; }
            gate->release(); reader.join();
            if (readFailure) { std::rethrow_exception(readFailure); }
            std::vector<uint8_t> out{'x'};
            require(database->get(key) == (deleted ? std::nullopt : std::optional{newValue}), "promoted old version hid the latest SST head");
            require(database->exists(key) == !deleted, "exists disagreed with the latest SST head");
            require(database->getInto(key, out) == !deleted && (deleted ? out == std::vector<uint8_t>{'x'} : out == newValue), "getInto returned stale promoted data");
            akkaradb::core::BufferArena arena; std::span<const uint8_t> arenaOut;
            require(database->getIntoArena(key, arena, arenaOut) == !deleted && (deleted || std::ranges::equal(arenaOut, newValue)), "arena read returned stale promoted data");
            require(database->count() == (deleted ? 0u : 1u), "count resurrected a stale promoted record");
            size_t scanned = 0;
            for (const auto& row : database->scan(arena)) { require(std::ranges::equal(row.value, newValue), "scan returned stale promoted data"); ++scanned; }
            require(scanned == (deleted ? 0u : 1u), "scan resurrected a stale promoted record");
            database->close(); database.reset();
            database = AkkEngine::open(options);
            require(database->get(key) == (deleted ? std::nullopt : std::optional{newValue}), "stale promotion survived reopen as the visible head");
            database->close(); database.reset(); fs::remove_all(dir, ec);
        }
    }
}

void runSstBlobGcRegression() {
    for (const bool manifestEnabled : {true, false}) {
        for (const auto boundary : {SstIntegrityGate::Boundary::ITERATOR, SstIntegrityGate::Boundary::PUT}) {
            const auto dir = fs::current_path() / ".bench-tmp" / ("sst-blob-gc-" + std::to_string(manifestEnabled) + "-" + std::to_string(static_cast<int>(boundary)));
            std::error_code ec; fs::remove_all(dir, ec);
            auto gate = std::make_shared<SstIntegrityGate>();
            auto options = persistentOptions(dir);
            options.components.manifestEnabled = manifestEnabled;
            options.components.blobEnabled = true; options.blob.thresholdBytes = 8;
            options.memtable.backendFactory = [gate] { return std::make_unique<SstIntegrityBackend>(gate); };
            auto database = akkaradb::engine::AkkEngine::open(options);
            const std::vector<uint8_t> value(64, 'b');
            database->put(bytes("existing"), value); database->forceFlush();
            gate->arm(boundary);
            std::exception_ptr firstFailure, secondFailure;
            std::atomic<bool> secondDone{false};
            const auto write = [&] { database->put(bytes("new"), value); database->forceFlush(); };
            const auto gc = [&] { database->runBlobGc(); };
            std::thread first{[&] {
                try { if (boundary == SstIntegrityGate::Boundary::PUT) { write(); } else { gc(); } }
                catch (...) { firstFailure = std::current_exception(); }
            }};
            const bool entered = gate->wait();
            if (!entered) { gate->release(); first.join(); require(false, "GC regression did not reach its interleaving boundary"); }
            std::thread second{[&] {
                try { if (boundary == SstIntegrityGate::Boundary::PUT) { gc(); } else { write(); } }
                catch (...) { secondFailure = std::current_exception(); }
                secondDone.store(true, std::memory_order_release);
            }};
            // Give the unsafe implementation time to scan a newly written Blob
            // whose reference is outside the collector's already captured sources.
            (void)waitUntil([&] { return secondDone.load(std::memory_order_acquire); }, std::chrono::milliseconds{200});
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
            gate->release(); first.join(); second.join();
            if (firstFailure) { std::rethrow_exception(firstFailure); }
            if (secondFailure) { std::rethrow_exception(secondFailure); }
            require(!waitUntil([&] { return database->stats().blob.blobsDeleted != 0; }, std::chrono::milliseconds{200}), "GC deleted a Blob across a flush/publication boundary");
            require(database->get(bytes("existing")) == std::optional{value} && database->get(bytes("new")) == std::optional{value}, "GC lost a live SST Blob value");
            database->close(); database.reset();
            database = akkaradb::engine::AkkEngine::open(options);
            require(database->get(bytes("new")) == std::optional{value}, "GC left an SST with a dangling Blob after reopen");
            database->close(); database.reset(); fs::remove_all(dir, ec);
        }
    }
}

void runSstPointCompactionRegression() {
    for (int api = 0; api < 5; ++api) {
        const auto dir = fs::current_path() / ".bench-tmp" / ("sst-point-compaction-" + std::to_string(api));
        std::error_code ec; fs::remove_all(dir, ec);
        auto gate = std::make_shared<SstIntegrityGate>();
        auto options = persistentOptions(dir);
        options.sst.compactionMode = akkaradb::engine::sst::SSTCompactionMode::BACKGROUND;
        options.sst.compactThreads = 1; options.sst.maxL0Files = 2;
        options.memtable.backendFactory = [gate] { return std::make_unique<SstIntegrityBackend>(gate); };
        auto database = akkaradb::engine::AkkEngine::open(options);
        const auto key = bytes("a"), secondKey = bytes("z");
        const std::vector<uint8_t> oldValue{'o'}, newValue{'n'};
        database->put(key, oldValue); database->put(secondKey, oldValue); database->forceFlush();
        gate->arm(SstIntegrityGate::Boundary::MISS);
        std::exception_ptr readFailure;
        std::thread reader{[&] {
            try {
                std::vector<uint8_t> out;
                if (api == 0) { require(database->get(key) == std::optional{oldValue}, "get lost its SST version during compaction"); }
                else if (api == 1) { require(database->getInto(key, out) && out == oldValue, "getInto lost its SST version during compaction"); }
                else if (api == 2) {
                    akkaradb::core::BufferArena arena; std::span<const uint8_t> value;
                    require(database->getIntoArena(key, arena, value) && std::ranges::equal(value, oldValue), "arena read lost its SST version during compaction");
                }
                else if (api == 3) { require(database->exists(key), "exists returned false during compaction of a live key"); }
                else {
                    const std::array keys{key, secondKey};
                    const auto batch = database->getBatch(keys);
                    require(batch.size() == 2 && batch[0].found && batch[1].found && batch[0].value == oldValue && batch[1].value == oldValue,
                        "batch did not retain one SST generation across compaction");
                }
            } catch (...) { readFailure = std::current_exception(); }
        }};
        const bool entered = gate->wait();
        try {
            require(entered, "reader did not reach the compaction boundary");
            database->put(key, newValue); database->put(secondKey, newValue); database->forceFlush();
            require(waitUntil([&] { return database->stats().sst.compactionsCompleted != 0; }), "compaction did not complete while the read was paused");
        } catch (...) { gate->release(); reader.join(); throw; }
        gate->release(); reader.join();
        if (readFailure) { std::rethrow_exception(readFailure); }
        require(database->get(key) == std::optional{newValue}, "current get did not select the compacted head");
        database->close(); database.reset();
        database = akkaradb::engine::AkkEngine::open(options);
        require(database->get(key) == std::optional{newValue}, "read pin changed the recovered compacted head");
        database->close(); database.reset(); fs::remove_all(dir, ec);
    }
}

void runSstScanBlobGcRegression() {
    using Engine = akkaradb::engine::AkkEngine;
    using Generator = akkaradb::core::ArenaGenerator<Engine::ScanRecordView>;
    for (const bool manifestEnabled : {false, true}) {
        // Capture in progress, unstarted, suspended, two cancellation states,
        // and an exception after capture must all retain/release the Blob lease.
        for (int phase = 0; phase < 6; ++phase) {
            const auto dir = fs::current_path() / ".bench-tmp" / ("sst-scan-blob-gc-" + std::to_string(manifestEnabled) + "-" + std::to_string(phase));
            std::error_code ec; fs::remove_all(dir, ec);
            auto gate = std::make_shared<SstIntegrityGate>();
            auto options = persistentOptions(dir);
            options.components.manifestEnabled = manifestEnabled;
            options.components.blobEnabled = true; options.blob.thresholdBytes = 8;
            options.memtable.backendFactory = [gate] { return std::make_unique<SstIntegrityBackend>(gate); };
            auto database = Engine::open(options);
            const auto a = bytes("a"), z = bytes("z");
            const std::vector<uint8_t> oldValue(64, 'o'), newValue(64, 'n');
            database->put(a, oldValue); database->put(z, oldValue); database->forceFlush();
            fs::path firstBlob;
            for (const auto& entry : fs::recursive_directory_iterator(dir)) {
                if (entry.path().filename() == "0000000000000001.akblob") { firstBlob = entry.path(); }
            }
            require(!firstBlob.empty(), "scan GC fixture lacks its first Blob");
            akkaradb::core::BufferArena arena;
            std::optional<Generator> scan;
            const auto createScan = [&] {
                return (phase & 1) == 0 ? database->scan(arena) : database->scanLocalStorage(arena);
            };
            const auto replaceAndQueueGc = [&] {
                database->put(a, newValue); database->put(z, newValue); database->forceFlush();
                database->runBlobGc();
                require(waitUntil([&] { return database->stats().blob.gcCycles != 0; }), "scan GC fixture did not queue deletion");
                require(!waitUntil([&] { return database->stats().blob.blobsDeleted != 0; }, std::chrono::milliseconds{200}),
                    "GC deleted a Blob retained by a scan");
            };
            if (phase == 0) {
                gate->arm(SstIntegrityGate::Boundary::ITERATOR);
                std::exception_ptr failure;
                std::thread creator{[&] { try { scan.emplace(createScan()); } catch (...) { failure = std::current_exception(); } }};
                const bool entered = gate->wait();
                try {
                    require(entered, "scan did not reach the source-capture boundary");
                    replaceAndQueueGc();
                } catch (...) { gate->release(); creator.join(); throw; }
                gate->release(); creator.join();
                if (failure) { std::rethrow_exception(failure); }
            }
            else { scan.emplace(createScan()); }

            Generator::iterator position;
            size_t rows = 0;
            if (phase == 2 || phase == 4) {
                position = scan->begin();
                require(position != scan->end() && std::ranges::equal(position->value, oldValue), "scan did not start with its old Blob value");
                ++rows;
            }
            if (phase != 0) { replaceAndQueueGc(); }

            if (phase == 3 || phase == 4) { scan.reset(); }
            else if (phase == 5) {
                flipByte(firstBlob, 0);
                requireRuntimeError([&] { (void)scan->begin(); }, "corrupt scan Blob must report a read error");
            }
            else {
                if (phase == 2) { ++position; } else { position = scan->begin(); }
                for (; position != scan->end(); ++position) {
                    require(std::ranges::equal(position->value, oldValue), "scan lost or changed a captured Blob value");
                    ++rows;
                }
                require(rows == 2, "scan omitted a captured Blob row");
            }
            // Even an exhausted/failed generator still in scope must release
            // its lease so an already queued collection can finish.
            require(waitUntil([&] { return database->stats().blob.blobsDeleted == 2; }), "scan retained its Blob lease after completion or cancellation");
            require(database->get(a) == std::optional{newValue} && database->get(z) == std::optional{newValue}, "scan GC damaged the current Blob values");
            scan.reset(); database->close(); database.reset(); fs::remove_all(dir, ec);
        }
    }
}

void runSstIntegrityRegressions() {
    runSstScanBlobGcRegression();
    runSstPointCompactionRegression();
    runSstPromotionRegression();
    runSstBlobGcRegression();
}
