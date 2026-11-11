// Included inside sst_memtable_safety_smoke_test.cpp's anonymous namespace.

void testFlushCallbackSelection() {
    for (const auto mode : {mt::MemTableFlushInputMode::MATERIALIZE_VECTOR, mt::MemTableFlushInputMode::STREAMING}) {
        for (int callbacks = 1; callbacks <= 3; ++callbacks) {
            auto options = optionsFor(0);
            options.flushInputMode = mode;
            size_t vectorCalls = 0, streamCalls = 0, rows = 0;
            if (callbacks & 1) { options.onFlush = [&](auto records) { ++vectorCalls; rows += records.size(); }; }
            if (callbacks & 2) {
                options.onFlushStream = [&](size_t, auto records) {
                    ++streamCalls;
                    for (const auto& record : records) { (void)record; ++rows; }
                };
            }
            auto memory = mt::MemTable::create(options);
            memory->put(bytes("key"), bytes("value"), 1);
            memory->forceFlush();
            const bool expectStream = (callbacks & 2) && (mode == mt::MemTableFlushInputMode::STREAMING || callbacks == 2);
            require(rows == 1 && streamCalls == size_t{expectStream} && vectorCalls == size_t{!expectStream},
                "flush did not use the available/preferred callback exactly once");
            require(!memory->getPinned(bytes("key"), 1), "successful callback did not unpublish its generation");
        }
    }
}

void testSequenceOrderedFlush(const fs::path& root, int backend, bool retainAll) {
    auto options = optionsFor(backend);
    options.sequenceOrderedVersions = true;
    sst::SSTManager::Options diskOptions;
    diskOptions.compactionMode = sst::SSTCompactionMode::DISABLED;
    auto disk = sst::SSTManager::create(root, diskOptions);
    options.onFlush = [&](auto records) { disk->flush(records); };
    auto memory = retainAll ? mt::MemTable::create(options, [] { return uint64_t{3}; }) : mt::MemTable::create(options);
    for (int keyIndex = 0; keyIndex < 90; ++keyIndex) {
        const auto key = "key-" + std::to_string(keyIndex);
        for (const uint64_t seq : {6, 2, 8, 5, 9, 1, 7, 4, 3}) {
            if (seq == 9) { memory->remove(bytes(key), seq); }
            else { memory->put(bytes(key), bytes(std::to_string(seq)), seq); }
        }
        for (uint64_t seq = retainAll ? 1 : 6; seq <= 9; ++seq) {
            const auto row = memory->getPinned(bytes(key), seq);
            require(row && row->view.seq() == seq && row->view.isTombstone() == (seq == 9),
                "sequence ordering lost a retained version or resurrected a tombstone");
        }
    }
    {
        auto rows = memory->iterator({}, UINT64_MAX);
        size_t count = 0;
        while (rows.hasNext()) {
            const auto row = rows.next();
            require(row && row->seq() == 9 && row->isTombstone(), "mutable scan used application order");
            ++count;
        }
        require(count == 90, "sequence-ordered scan lost keys");
    }
    memory->forceFlush();
    for (int keyIndex = 0; keyIndex < 90; ++keyIndex) {
        const auto key = "key-" + std::to_string(keyIndex);
        for (uint64_t seq = retainAll ? 1 : 6; seq <= 9; ++seq) {
            const auto row = disk->get(bytes(key), seq);
            require(row && row->seq == seq && row->isTombstone() == (seq == 9), "flush changed sequence precedence");
        }
    }
}

void testMemorySnapshotGrowthAdmission(int backend) {
    auto options = optionsFor(backend);
    const auto factory = options.backendFactory;
    auto gate = std::make_shared<std::atomic<bool>>(false);
    std::atomic<bool> release{false};
    std::atomic<size_t> created{0};
    options.backendFactory = [&]() -> std::unique_ptr<mt::IMemTable> {
        if (++created == 2) {
            gate->store(true);
            waitFor([&] { return release.load(); });
        }
        return factory();
    };
    auto memory = mt::MemTable::create(options);
    memory->put(bytes("key"), bytes("small"), 1);
    const auto limit = memory->approxSize() + 1024;
    std::optional<mt::MemTable::RangeIterator> snapshot;
    std::exception_ptr failure;
    std::jthread capture{[&] {
        try { snapshot = memory->sealAndPinMemoryIterator({}, UINT64_MAX, false, limit, 8); }
        catch (...) { failure = std::current_exception(); }
    }};
    try {
        waitFor([&] { return gate->load(); });
        memory->put(bytes("key"), bytes(std::string(40'000, 'x')), 2);
    }
    catch (...) { release.store(true); capture.join(); throw; }
    release.store(true);
    capture.join();
    if (failure) { std::rethrow_exception(failure); }
    require(!snapshot, "snapshot admitted growth above its byte budget");
    require(memory->snapshot().immutableTables == 0, "rejected snapshot rotated a generation");
    auto oversized = memory->sealAndPinMemoryIterator({}, UINT64_MAX, true, limit, 1);
    require(oversized && oversized->get(bytes("key"))->valueSize() == 40'000,
        "completion-first did not admit one oversized snapshot");
    require(!memory->sealAndPinMemoryIterator({}, UINT64_MAX, false, 0, 1), "generation limit was not enforced");
    oversized.reset();
    auto retry = memory->sealAndPinMemoryIterator({}, UINT64_MAX, true, limit, 1);
    require(retry.has_value(), "snapshot rejection or release leaked admission capacity");
}

void testConcurrentSequenceOrder(int backend, bool retainAll) {
    auto options = optionsFor(backend);
    options.sequenceOrderedVersions = true;
    auto memory = retainAll ? mt::MemTable::create(options, [] { return UINT64_MAX; }) : mt::MemTable::create(options);
    memory->put(bytes("key"), bytes("1"), 1);
    std::atomic<uint64_t> next{2};
    std::atomic<size_t> completed{0};
    std::array<std::exception_ptr, 4> failures;
    std::vector<std::jthread> writers;
    for (size_t writer = 0; writer < failures.size(); ++writer) {
        writers.emplace_back([&, writer] {
            try {
                for (size_t i = 0; i < 800; ++i) {
                    const auto seq = next.fetch_add(1);
                    if ((seq & 7) == 0) { std::this_thread::yield(); }
                    memory->put(bytes("key"), bytes(std::to_string(seq)), seq);
                }
            }
            catch (...) { failures[writer] = std::current_exception(); }
            completed.fetch_add(1);
        });
    }
    uint64_t last = 1;
    while (completed.load() != failures.size()) {
        const auto row = memory->getPinned(bytes("key"), UINT64_MAX);
        require(row && row->view.seq() >= last && row->view.valueString() == std::to_string(row->view.seq()),
            "concurrent sequence-ordered read regressed or returned an inconsistent record");
        last = row->view.seq();
    }
    writers.clear();
    for (const auto& failure : failures) { if (failure) { std::rethrow_exception(failure); } }
    require(memory->getPinned(bytes("key"), UINT64_MAX)->view.seq() == next.load() - 1,
        "concurrent writers lost the maximum sequence");
}

void testBoundedRetentionFlush(const fs::path& root, int backend, bool streaming) {
    auto options = optionsFor(backend);
    sst::SSTManager::Options diskOptions;
    diskOptions.compactionMode = sst::SSTCompactionMode::DISABLED;
    diskOptions.targetFileSize = 4096;
    auto disk = sst::SSTManager::create(root, diskOptions);
    if (streaming) {
        options.flushInputMode = mt::MemTableFlushInputMode::STREAMING;
        options.onFlushStream = [&](size_t count, auto records) { disk->flush(count, std::move(records)); };
    }
    else { options.onFlush = [&](auto records) { disk->flush(records); }; }
    auto memory = mt::MemTable::create(options);
    for (int i = 0; i < 72; ++i) {
        const auto key = "key-" + std::to_string(i);
        for (uint64_t seq = 1; seq <= 6; ++seq) {
            if (seq == 5) { memory->remove(bytes(key), seq); }
            else { memory->put(bytes(key), bytes("value"), seq); }
        }
        require(!memory->getPinned(bytes(key), 2), "bounded retention did not evict old versions");
        require(memory->getPinned(bytes(key), 3).has_value(), "bounded retention lost its oldest retained version");
    }
    {
        auto rows = memory->iterator({}, UINT64_MAX);
        size_t count = 0;
        while (rows.hasNext()) {
            require(rows.next()->seq() == 6, "ordinary scan did not choose the newest version");
            ++count;
        }
        require(count == 72, "ordinary scan emitted multiple versions per key");
    }
    memory->forceFlush();
    const auto verify = [&] {
        for (int i = 0; i < 72; ++i) {
            const auto key = "key-" + std::to_string(i);
            require(!disk->get(bytes(key), 2), "flush recreated an evicted version");
            for (uint64_t seq = 3; seq <= 6; ++seq) {
                const auto row = disk->get(bytes(key), seq);
                require(row && row->seq == seq && row->isTombstone() == (seq == 5),
                    "flush discarded a bounded retained version or tombstone");
            }
        }
    };
    verify();
    memory.reset();
    disk.reset();
    disk = sst::SSTManager::create(root, diskOptions);
    disk->recover();
    verify();
}

void testFlushReplacementFailure(int backend, bool nullReplacement) {
    auto options = optionsFor(backend);
    auto factory = options.backendFactory;
    size_t attempts = 0;
    options.backendFactory = [&, factory]() -> std::unique_ptr<mt::IMemTable> {
        if (++attempts == 2) {
            if (nullReplacement) { return nullptr; }
            throw std::bad_alloc{};
        }
        return factory();
    };
    std::vector<uint64_t> persisted;
    options.onFlush = [&](auto records) {
        for (const auto& record : records) { persisted.push_back(record.seq()); }
    };
    auto memory = mt::MemTable::create(options);
    memory->put(bytes("key"), bytes("old"), 1);
    bool failed = false;
    try { memory->forceFlush(); }
    catch (const std::exception&) { failed = true; }
    require(failed, "replacement failure was not reported");
    require(memory->snapshot().immutableTables == 0, "failed rotation installed an immutable");
    memory->put(bytes("key"), bytes("new"), 2);
    const auto row = memory->getPinned(bytes("key"), 2);
    require(row && row->view.valueString() == "new", "replacement failure left the active table frozen");
    memory->forceFlush();
    std::sort(persisted.begin(), persisted.end());
    require(persisted == std::vector<uint64_t>{1, 2}, "retry lost or duplicated records after replacement failure");
}

void testOwnedRecordSstLookup(const fs::path& root) {
    namespace core = akkaradb::core;
    core::BufferArena arena;
    auto stringRecord = core::OwnedRecord::create("a", "string", 1, 0, arena);
    auto spanRecord = core::OwnedRecord::create(bytes("b"), bytes("span"), 2, 0, arena);
    core::OwnedRecord inplace;
    core::OwnedRecord::createInplace(inplace, bytes("c"), bytes("inplace"), 3, 0, arena);
    auto tombstone = core::OwnedRecord::tombstone(bytes("d"), 4, arena);
    auto emptyKey = core::OwnedRecord::create(bytes(""), bytes("empty-key"), 5, 0, arena);
    const auto view = [](const core::OwnedRecord& record) {
        return RecordView{record.key().data(), record.hdr.kLen, record.value().data(), record.hdr.vLen,
            record.hdr.seq, record.hdr.flags, record.keyFp64, record.miniKey};
    };
    const std::vector<RecordView> records{view(emptyKey), view(stringRecord), view(spanRecord), view(inplace), view(tombstone)};
    sst::SSTManager::Options options;
    options.compactionMode = sst::SSTCompactionMode::DISABLED;
    auto disk = sst::SSTManager::create(root, options);
    disk->flush(records);
    for (const auto& expected : records) {
        const auto actual = disk->get(expected.key());
        require(actual && actual->seq == expected.seq() && actual->flags == expected.flags() &&
            std::equal(actual->value.begin(), actual->value.end(), expected.value().begin(), expected.value().end()),
            "OwnedRecord factory output could not be found in SST");
    }
}

struct MemoryReadGate {
    std::atomic<bool> entered{false};
    std::atomic<bool> release{false};
    std::atomic<bool> destroyed{false};
    std::atomic<bool> timedOut{false};
};

class DelayedReadBackend final : public mt::IMemTable {
    mt::SkipListMemTable backend_;
    std::shared_ptr<MemoryReadGate> gate_;

public:
    explicit DelayedReadBackend(std::shared_ptr<MemoryReadGate> gate) : gate_{std::move(gate)} {}
    ~DelayedReadBackend() override { if (gate_) { gate_->destroyed.store(true); } }
    akkaradb::core::Status put(akkaradb::core::ByteView key, akkaradb::core::ByteView value,
        uint64_t seq, uint8_t flags, uint64_t fp, uint64_t mini) override {
        return backend_.put(key, value, seq, flags, fp, mini);
    }
    bool get(akkaradb::core::ByteView key, uint64_t seq, RecordView* out) const override {
        const auto gate = gate_;
        if (!gate) { return backend_.get(key, seq, out); }
        gate->entered.store(true);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
        while (!gate->release.load()) {
            if (std::chrono::steady_clock::now() >= deadline) { gate->timedOut.store(true); break; }
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
        // Do not access this backend after the gate: a broken owner may have
        // destroyed it while the call was suspended. Test that lifetime separately.
        return false;
    }
    akkaradb::core::ArenaGenerator<RecordView> iterator(akkaradb::core::ByteView start,
        akkaradb::core::ByteView end, uint64_t seq) const override { return backend_.iterator(start, end, seq); }
    akkaradb::core::ArenaGenerator<RecordView> flushIterator() const override { return backend_.flushIterator(); }
    void useSequenceOrder() override { backend_.useSequenceOrder(); }
    void freeze() override { backend_.freeze(); }
    size_t sizeBytes() const override { return backend_.sizeBytes(); }
    size_t entryCount() const override { return backend_.entryCount(); }
};

void testReadSurvivesMemoryCompaction() {
    auto gate = std::make_shared<MemoryReadGate>();
    mt::MemTable::Options options;
    options.shardCount = 1;
    options.flushMode = mt::MemTableFlushMode::MANUAL_ONLY;
    size_t created = 0;
    options.backendFactory = [&]() -> std::unique_ptr<mt::IMemTable> {
        return std::make_unique<DelayedReadBackend>(++created == 1 ? gate : nullptr);
    };
    auto memory = mt::MemTable::create(options);
    memory->put(bytes("key"), bytes("old"), 1);
    std::jthread reader{[&] { RecordView row; (void)memory->get(bytes("key"), UINT64_MAX, &row); }};
    struct ReleaseOnExit {
        std::shared_ptr<MemoryReadGate> gate;
        ~ReleaseOnExit() { gate->release.store(true); }
    } releaseOnExit{gate};
    waitFor([&] { return gate->entered.load(); });
    auto first = memory->sealAndPinMemoryIterator({}, 1, true, 0, 8);
    memory->put(bytes("key"), bytes("new"), 2);
    auto second = memory->sealAndPinMemoryIterator({}, 2, true, 0, 8);
    first.reset();
    second.reset();
    waitFor([&] { return memory->snapshot().memorySnapshotCompactionsCompleted != 0 && !memory->snapshot().memorySnapshotCompactionPending; });
    require(!gate->destroyed.load(), "memory compaction destroyed a backend during get");
    require(!gate->timedOut.load(), "delayed read timed out before lifetime verification");
    gate->release.store(true);
    reader.join();
    waitFor([&] { return gate->destroyed.load(); });
}
