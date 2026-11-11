void testSharedSstBlockCache() {
    namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst;
    const auto root = fs::current_path() / ".bench-tmp" / "sst-shared-cache";
    std::error_code error; fs::remove_all(root, error); fs::create_directories(root);
    const std::vector<uint8_t> key{'k'}, value(3000, 'v'), oversized(12000, 'x');
    for (const auto codec : {sst::SSTWriter::Codec::NONE, sst::SSTWriter::Codec::ZSTD}) {
        sst::SSTWriter::Options options; options.codec = codec; options.blockSize = 4096;
        (void)sst::SSTWriter::write(root / "one.aksst", std::vector{record(key, value, 7)}, options);
        (void)sst::SSTWriter::write(root / "large.aksst", std::vector{record(key, oversized, 8)}, options);
        for (int round = 0; round < 8; ++round) {
            auto cache = std::make_shared<sst::SSTReader::BlockCache>(8192);
            auto reader = sst::SSTReader::open(root / "one.aksst", {.sharedBlockCache = cache});
            require(reader != nullptr, "shared-cache fixture failed to open");
            std::vector<std::optional<sst::SSTReader::PinnedRecord>> pins(32);
            std::atomic<size_t> ready{0}; std::atomic<bool> go{false};
            std::vector<std::thread> threads;
            for (size_t i = 0; i < pins.size(); ++i) {
                threads.emplace_back([&, i] {
                    ++ready;
                    while (!go.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                    pins[i] = reader->getPinned(key);
                });
            }
            while (ready.load() != pins.size()) { std::this_thread::yield(); }
            go.store(true, std::memory_order_release);
            for (auto& thread : threads) { thread.join(); }
            for (const auto& pin : pins) {
                require(pin && pin->lifetime == pins.front()->lifetime && pin->toOwned().value == value,
                    "concurrent cache misses installed duplicate blocks or changed data");
            }
            const auto before = cache->stats();
            require(before.entries == 1 && before.residentBytes <= before.capacityBytes, "duplicate insertion broke cache accounting");
            require(reader->get(key)->value == value && cache->stats().hits == before.hits + 1,
                "concurrent insertion lost the cached block");
            auto other = sst::SSTReader::open(root / "large.aksst", {.sharedBlockCache = cache});
            require(other && other->get(key)->value == oversized && cache->stats().entries == 1,
                "oversized block displaced useful cached data or exceeded budget");
            reader.reset();
            require(cache->stats().entries == 0 && cache->stats().residentBytes == 0 && pins[0]->toOwned().value == value,
                "reader teardown leaked cache entries or invalidated active pins");
        }
    }
    sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED;
    options.blockCacheBytes = 8192;
    auto manager = sst::SSTManager::create(root / "managed", options);
    std::vector<std::vector<uint8_t>> keys;
    for (uint8_t i = 0; i < 32; ++i) {
        keys.push_back({i});
        manager->flush(std::vector{record(keys.back(), std::vector<uint8_t>(2000, i), i + 1)});
    }
    const auto snapshot = manager->readSnapshot();
    for (size_t pass = 0; pass < 2; ++pass) {
        for (uint8_t i = 0; i < keys.size(); ++i) {
            const auto row = snapshot.get(keys[i]);
            const auto stats = manager->blockCacheStats();
            require(row && row->value == std::vector<uint8_t>(2000, i), "shared cache confused blocks from distinct SSTs");
            require(stats.capacityBytes == 8192 && stats.residentBytes <= 8192 && stats.entries <= 4,
                "manager multiplied its block-cache budget by SST count");
        }
    }
    manager.reset();
    require(snapshot.get(keys.front())->value == std::vector<uint8_t>(2000, 0), "snapshot lost its shared cache after manager destruction");
}

void testSstRangeSelection() {
    namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst; namespace mf = akkaradb::engine::manifest;
    const auto root = fs::current_path() / ".bench-tmp" / "sst-range-selection";
    std::error_code error; fs::remove_all(root, error); fs::create_directories(root);
    for (const auto codec : {sst::SSTWriter::Codec::NONE, sst::SSTWriter::Codec::ZSTD}) {
        const auto dir = root / std::to_string(static_cast<int>(codec));
        auto manifest = mf::Manifest::create(dir / "manifest.akmf");
        sst::SSTManager::Options options; options.codec = codec; options.maxLevels = 2;
        options.maxL0Files = 2; options.targetFileSize = 1024 * 1024;
        options.compactionMode = sst::SSTCompactionMode::DISABLED;
        auto manager = sst::SSTManager::create(dir / "sst", options, manifest.get());
        manager.reset();
        const std::vector<uint8_t> value{'v'};
        uint64_t expectedBytes = 0;
        for (uint8_t key : {'a', 'c', 'e', 'g', 'i', 'k', 'm'}) {
            const auto name = "L1_" + std::to_string(key) + ".aksst";
            const auto path = dir / "sst" / name;
            sst::SSTWriter::Options writer; writer.level = 1; writer.codec = codec;
            (void)sst::SSTWriter::write(path, std::vector{record(std::vector<uint8_t>{key}, value, 1)}, writer);
            manifest->sstSeal(1, name, 1, std::nullopt, std::nullopt);
            if (key == 'c' || key == 'i') { expectedBytes += fs::file_size(path); }
        }
        manager = sst::SSTManager::create(dir / "sst", options, manifest.get()); manager->recover();
        const std::vector<uint8_t> b{'b'}, c{'c'}, d{'d'}, h{'h'}, j{'j'}, empty;
        manager->flush(std::vector{record(b, value, 2), record(c, empty, 2, sst::SST_RECORD_FLAG_TOMBSTONE), record(d, value, 2)});
        manager->flush(std::vector{record(h, value, 3), record(j, value, 3)});
        for (const auto& file : fs::directory_iterator(dir / "sst")) {
            if (file.path().filename().string().starts_with("L0_")) { expectedBytes += file.file_size(); }
        }
        const auto old = manager->readSnapshot();
        auto narrow = old.scanIter(h, j);
        require(narrow.next()->key == h && narrow.next()->key == std::vector<uint8_t>{'i'} && !narrow.hasNext(),
            "range bounds included an end key or omitted an intersecting source");
        require(narrow.get(b)->seq == 2, "narrow iterator lost point access to its captured file set");
        auto wide = narrow.fork(); size_t oldCount = 0;
        while (wide.hasNext()) { (void)wide.next(); ++oldCount; }
        require(oldCount == 10, "widened fork lost files pruned from its parent's range");
        require(!old.scanIter(j, h).hasNext() && !old.scanIter(h, h).hasNext(), "empty or reversed scan produced rows");
        manager.reset();
        options.compactionMode = sst::SSTCompactionMode::BACKGROUND; options.compactThreads = 1;
        manager = sst::SSTManager::create(dir / "sst", options, manifest.get()); manager->recover();
        waitForSst([&] { manager->throwIfBackgroundFailed(); return manager->compactionSnapshot().compactionsCompleted == 1; });
        manager->shutdown();
        const auto stats = manager->compactionSnapshot();
        require(stats.filesCompacted == 4 && stats.bytesCompactedIn == expectedBytes,
            "L0 compaction rewrote an unrelated L1 range");
        const auto live = manifest->liveSst();
        for (uint8_t key : {'a', 'e', 'g', 'k', 'm'}) {
            const auto name = "L1_" + std::to_string(key) + ".aksst";
            require(std::ranges::find(live, name) != live.end(), "unrelated L1 file was replaced");
        }
        require(manager->levelStats()[0].fileCount == 0 && manager->levelStats()[1].fileCount == 7,
            "compaction bridged an untouched L1 gap instead of splitting output");
        auto scan = manager->scanIter(); std::vector<uint8_t> actual;
        while (scan.hasNext()) { actual.push_back(scan.next()->key[0]); }
        require(actual == std::vector<uint8_t>({'a','b','d','e','g','h','i','j','k','m'}), "selective compaction changed visible rows");
        require(manager->contains(c) == std::optional{false}, "selective compaction discarded a tombstone");
        manager.reset(); options.compactionMode = sst::SSTCompactionMode::DISABLED;
        manager = sst::SSTManager::create(dir / "sst", options, manifest.get()); manager->recover();
        auto after = manager->scanIter(d, j); std::vector<uint8_t> selected;
        while (after.hasNext()) { selected.push_back(after.next()->key[0]); }
        require(selected == std::vector<uint8_t>({'d','e','g','h','i'}), "indexed higher-level scan changed after recovery");

        options.maxL0Files = 3; options.compactionMode = sst::SSTCompactionMode::BACKGROUND;
        auto coalesced = sst::SSTManager::create(dir / "empty-destination", options);
        for (uint8_t key : {'a', 'e', 'z'}) {
            coalesced->flush(std::vector{record(std::vector<uint8_t>{key}, value, 1)});
        }
        waitForSst([&] { coalesced->throwIfBackgroundFailed(); return coalesced->compactionSnapshot().compactionsCompleted == 1; });
        require(coalesced->levelStats()[0].fileCount == 0 && coalesced->levelStats()[1].fileCount == 1,
            "empty key gaps unnecessarily fragmented compaction outputs");
    }

    // Many blocks and many overlapping/disjoint L0 intervals exercise both
    // file-range pruning and the reader's lower_bound at block/record level.
    sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED;
    options.blockSize = 4096; options.blockCacheBytes = 0; options.targetFileSize = 4096;
    auto manager = sst::SSTManager::create(root / "many", options);
    std::vector<sst::SSTRecord> rows;
    for (uint64_t i = 0; i < 1000; ++i) {
        rows.push_back({.key = {static_cast<uint8_t>(i >> 8), static_cast<uint8_t>(i)}, .value = std::vector<uint8_t>(300, 'v'), .seq = i + 1});
    }
    size_t produced = 0; manager->flush(rows.size(), streamingFixture(rows, produced));
    // A broad, older interval makes naive prefix-max interval indexes degrade.
    const std::vector low{record(rows.front().key, rows.front().value, 1), record(rows.back().key, rows.back().value, 1)};
    manager->flush(low);
    const auto snapshot = manager->readSnapshot();
    for (size_t i = 0; i < rows.size(); i += 7) {
        const auto end = std::min(i + 3, rows.size());
        auto scan = snapshot.scanIter(rows[i].key, end == rows.size() ? std::span<const uint8_t>{} : std::span{rows[end].key});
        size_t seen = i;
        while (scan.hasNext()) {
            const auto row = scan.next();
            require(seen < end && row->key == rows[seen].key && row->seq == rows[seen].seq, "interval index omitted or duplicated a visible key");
            ++seen;
        }
        require(seen == end, "interval scan stopped early");
    }
    const auto direct = root / "many-blocks.aksst";
    sst::SSTWriter::Options writer; writer.blockSize = 4096;
    size_t written = 0; (void)sst::SSTWriter::write(direct, rows.size(), streamingFixture(rows, written), writer);
    auto reader = sst::SSTReader::open(direct);
    require(reader && reader->header().blockCount > 50, "block range fixture is too small");
    size_t seen = 995;
    for (const auto& row : reader->scan(rows[995].key)) {
        require(seen < rows.size() && row.key == rows[seen].key, "binary block seek changed scan output"); ++seen;
    }
    require(seen == rows.size(), "binary block seek omitted tail rows");
}
