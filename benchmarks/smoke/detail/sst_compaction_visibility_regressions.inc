void testCompactionVisibilityFrontier() {
    namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst;
    const auto root = fs::current_path() / ".bench-tmp" / "sst-compaction-visibility-frontier";
    std::error_code ec; fs::remove_all(root, ec);
    for (const auto codec : {sst::SSTWriter::Codec::NONE, sst::SSTWriter::Codec::ZSTD}) {
        const auto dir = root / std::to_string(static_cast<int>(codec));
        std::atomic<uint64_t> visible{1};
        sst::SSTManager::Options options;
        options.codec = codec; options.maxLevels = 3; options.compactThreads = 1;
        options.maxL0Files = 2; options.l1MaxBytes = 1;
        options.targetFileSize = 4096; options.blockSize = 4096;
        const auto create = [&] { return sst::SSTManager::create(dir, options, nullptr, [&] { return visible.load(std::memory_order_acquire); }); };
        auto manager = create();
        const std::vector<uint8_t> a{'a'}, b{'b'}, c{'c'}, oldValue(5000, 'o'), middleValue(5000, 'm'), newValue(5000, 'n'), empty;
        manager->flush(std::vector{record(a, oldValue, 1), record(b, oldValue, 1), record(c, oldValue, 1)});
        const auto pinned = manager->readSnapshot();
        // Future versions are deliberately unordered and span blocks. All must
        // stay in one file per key, including the tombstone between live values.
        manager->flush(std::vector{
            record(a, newValue, 5), record(a, middleValue, 3), record(a, empty, 4, sst::SST_RECORD_FLAG_TOMBSTONE),
            record(b, newValue, 5), record(b, middleValue, 3), record(b, empty, 4, sst::SST_RECORD_FLAG_TOMBSTONE),
            record(c, newValue, 5), record(c, middleValue, 3), record(c, empty, 4, sst::SST_RECORD_FLAG_TOMBSTONE)
        });
        waitForSst([&] {
            manager->throwIfBackgroundFailed();
            const auto levels = manager->levelStats();
            return levels[0].fileCount == 0 && levels[1].fileCount == 0 && levels[2].fileCount == 3;
        });
        manager->shutdown(); manager.reset();
        manager = create(); manager->recover();
        for (const auto& key : {a, b, c}) {
            verifySelectedVersion(*manager, key, 1, 1, false, oldValue);
            verifySelectedVersion(*manager, key, 3, 3, false, middleValue);
            verifySelectedVersion(*manager, key, 4, 4, true, empty);
            verifySelectedVersion(*manager, key, 5, 5, false, newValue);
        }
        auto scan = pinned.scanIter({}, {}, 1);
        size_t rows = 0;
        while (scan.hasNext()) { require(scan.next()->value == oldValue, "snapshot scan lost its pre-compaction file set"); ++rows; }
        require(rows == 3 && !sst::SSTManager::ReadSnapshot{}.scanIter().hasNext(), "snapshot scan has an incorrect source set");
        visible.store(5, std::memory_order_release);
        const auto before = manager->compactionSnapshot().compactionsCompleted;
        manager->flush(std::vector{record(a, newValue, 6)});
        manager->flush(std::vector{record(a, newValue, 7)});
        waitForSst([&] {
            manager->throwIfBackgroundFailed();
            const auto levels = manager->levelStats();
            return manager->compactionSnapshot().compactionsCompleted > before && levels[0].fileCount == 0 && levels[1].fileCount == 0;
        });
        manager->shutdown();
        require(!manager->get(a, 3), "advanced visibility frontier did not reclaim superseded versions");
        verifySelectedVersion(*manager, a, 5, 5, false, newValue);
        verifySelectedVersion(*manager, a, 6, 6, false, newValue);
        verifySelectedVersion(*manager, a, 7, 7, false, newValue);
    }
    fs::remove_all(root, ec);
}
