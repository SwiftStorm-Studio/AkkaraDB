std::vector<akkaradb::engine::sst::SSTRecord> flushSizingRows() {
    namespace sst = akkaradb::engine::sst;
    std::vector<sst::SSTRecord> rows;
    for (uint64_t i = 0; i < 64; ++i) {
        const auto text = "key-" + std::to_string(100000 + i);
        const std::vector<uint8_t> key(text.begin(), text.end());
        rows.push_back({.key = key, .value = std::vector<uint8_t>(i == 17 ? 5000 : 400, 'v'), .seq = i * 3 + 3});
        if (i == 17) {
            rows.push_back({.key = key, .value = std::vector<uint8_t>(5000, 'o'), .seq = i * 3 + 1});
            rows.push_back({.key = key, .value = {}, .seq = i * 3 + 2, .flags = sst::SST_RECORD_FLAG_TOMBSTONE});
        }
    }
    return rows;
}

void verifyFlushSizing(akkaradb::engine::sst::SSTManager& manager,
    const std::filesystem::path& dir, const std::vector<akkaradb::engine::sst::SSTRecord>& rows, uint64_t target) {
    namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst;
    require(manager.levelStats()[0].fileCount > 1 && manager.maxSequence() == rows.back().seq,
        "flush did not split its output or preserve maximum sequence");
    size_t files = 0, physicalRows = 0;
    std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> bounds;
    for (const auto& entry : fs::directory_iterator(dir)) {
        if (entry.path().extension() != ".aksst") continue;
        auto reader = sst::SSTReader::open(entry.path());
        require(reader != nullptr, "split SST failed to open");
        ++files;
        uint64_t rawBytes = 0;
        for (const auto& row : reader->scan()) {
            rawBytes += sst::alignUpU64(sizeof(akkaradb::core::SSTHdr32) + row.key.size() + row.value.size(), 8);
            ++physicalRows;
        }
        const std::vector<uint8_t> first(reader->firstKey().begin(), reader->firstKey().end());
        const std::vector<uint8_t> last(reader->lastKey().begin(), reader->lastKey().end());
        require(rawBytes <= target || first == last, "flush exceeded its record-byte target across multiple keys");
        bounds.emplace_back(first, last);
    }
    std::ranges::sort(bounds);
    for (size_t i = 1; i < bounds.size(); ++i) {
        require(bounds[i - 1].second < bounds[i].first, "flush split a retained key across files");
    }
    require(files == manager.levelStats()[0].fileCount && physicalRows == rows.size(), "split flush lost physical versions");
    auto scan = manager.scanIter();
    size_t unique = 0;
    for (const auto& row : rows) {
        const auto head = manager.get(row.key);
        const auto expected = std::ranges::max(rows | std::views::filter([&](const auto& other) { return other.key == row.key; }), {}, &sst::SSTRecord::seq);
        require(head && head->seq == expected.seq && head->value == expected.value, "split flush changed version selection");
    }
    while (scan.hasNext()) { (void)scan.next(); ++unique; }
    require(unique == 64, "split flush changed the logical scan");
}

void testFlushSizeTargets() {
    namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst; namespace mf = akkaradb::engine::manifest;
    const auto root = fs::current_path() / ".bench-tmp" / "sst-flush-size-targets";
    std::error_code error; fs::remove_all(root, error);
    const auto rows = flushSizingRows();
    for (const auto codec : {sst::SSTWriter::Codec::NONE, sst::SSTWriter::Codec::ZSTD}) {
        for (const bool external : {false, true}) {
            for (int input = 0; input < 4; ++input) {
                const auto dir = root / (std::to_string(static_cast<int>(codec)) + "-" + std::to_string(external) + "-" + std::to_string(input));
                sst::SSTManager::Options options;
                options.compactionMode = sst::SSTCompactionMode::DISABLED;
                options.codec = codec; options.targetFileSize = 4096; options.blockSize = 4096;
                auto manifest = external ? mf::Manifest::create(dir / "external.akmf", true) : nullptr;
                auto manager = sst::SSTManager::create(dir, options, manifest.get());
                if (input == 0) {
                    const auto views = rows | std::views::transform([](const auto& row) { return record(row.key, row.value, row.seq, row.flags); })
                        | std::ranges::to<std::vector>();
                    require(manager->flush(views) == rows.back().seq, "span split flush returned the wrong sequence");
                } else {
                    const size_t estimate = input == 1 ? 0 : input == 2 ? 1 : SIZE_MAX;
                    require(manager->flush(estimate, transientStreamingFixture(rows, true)) == rows.back().seq,
                        "streaming split flush returned the wrong sequence");
                }
                verifyFlushSizing(*manager, dir, rows, options.targetFileSize);
                manager.reset(); if (manifest) { manifest->close(); manifest.reset(); }
                if (external) { manifest = mf::Manifest::create(dir / "external.akmf"); }
                manager = sst::SSTManager::create(dir, options, manifest.get()); manager->recover();
                verifyFlushSizing(*manager, dir, rows, options.targetFileSize);
            }
        }
    }
    {
        const auto dir = root / "failed-input";
        sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED; options.targetFileSize = 4096;
        auto manager = sst::SSTManager::create(dir, options);
        for (int failure = 0; failure < 2; ++failure) {
            auto invalid = rows;
            if (failure == 1) { invalid.back().key = {'a'}; }
            size_t produced = 0; bool rejected = false;
            try { manager->flush(0, streamingFixture(invalid, produced, failure == 0 ? 30 : SIZE_MAX)); }
            catch (const std::exception&) { rejected = true; }
            require(rejected && manager->levelStats()[0].fileCount == 0 && manager->maxSequence() == 0,
                "failed split flush published partial output");
            for (const auto& entry : fs::directory_iterator(dir)) {
                require(entry.path().extension() != ".aksst" && entry.path().extension() != ".tmp", "failed split flush left unpublished files");
            }
        }
        manager.reset(); manager = sst::SSTManager::create(dir, options); manager->recover();
        require(!manager->scanIter().hasNext(), "failed split flush became visible after recovery");
    }
    fs::remove_all(root, error);
}

void testBloomSizingFromActualKeys() {
    namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst;
    const auto root = fs::current_path() / ".bench-tmp" / "sst-bloom-actual-keys";
    std::error_code error; fs::remove_all(root, error);
    std::vector<sst::SSTRecord> rows;
    for (uint64_t i = 0; i < 4096; ++i) {
        const auto text = "bloom-" + std::to_string(100000 + i * 2);
        rows.push_back({.key = std::vector<uint8_t>(text.begin(), text.end()), .value = {'v'}, .seq = i + 1});
    }
    for (const auto codec : {sst::SSTWriter::Codec::NONE, sst::SSTWriter::Codec::ZSTD}) {
        std::vector<uint8_t> baseline;
        for (const size_t estimate : {size_t{0}, size_t{1}, size_t{4096}, SIZE_MAX}) {
            const auto path = root / (std::to_string(static_cast<int>(codec)) + "-" + std::to_string(estimate) + ".aksst");
            sst::SSTWriter::Options options; options.codec = codec;
            const auto result = sst::SSTWriter::write(path, estimate, transientStreamingFixture(rows, true), options);
            auto reader = sst::SSTReader::open(path);
            require(reader && result.entryCount == rows.size(), "actual-key Bloom fixture failed to open");
            std::ifstream file(path, std::ios::binary); file.seekg(reader->header().bloomOffset);
            sst::SSTBloomHeaderV1 header{}; file.read(reinterpret_cast<char*>(&header), sizeof(header));
            std::vector<uint8_t> bits(header.bitsSize); file.read(reinterpret_cast<char*>(bits.data()), bits.size());
            require(bool(file) && header.numBits >= rows.size() * options.bloomBitsPerKey, "Bloom stayed sized to the input estimate");
            if (baseline.empty()) { baseline = bits; } else { require(bits == baseline, "estimate changed the final Bloom filter"); }
            size_t falsePositives = 0;
            for (size_t i = 0; i < rows.size(); ++i) {
                require(reader->contains(rows[i].key) == std::optional{true}, "resized Bloom introduced a false negative");
                const auto text = "bloom-" + std::to_string(100001 + i * 2);
                const auto key = std::span{reinterpret_cast<const uint8_t*>(text.data()), text.size()};
                const auto fp = akkaradb::core::computeKeyFp64(key);
                bool maybe = true;
                for (uint32_t hash = 0; hash < header.numHashes; ++hash) {
                    const auto bit = (static_cast<uint32_t>(fp) + hash * (static_cast<uint32_t>(fp >> 32) | 1u)) & (header.numBits - 1u);
                    maybe &= (bits[bit >> 3] & (1u << (bit & 7u))) != 0;
                }
                falsePositives += maybe;
            }
            require(falsePositives < rows.size() / 20, "actual-key Bloom is saturated");
        }
    }
    const auto key = rows.front().key;
    for (auto& row : rows) { row.key = key; }
    const auto repeated = root / "repeated.aksst";
    (void)sst::SSTWriter::write(repeated, SIZE_MAX, transientStreamingFixture(rows, true), {});
    auto reader = sst::SSTReader::open(repeated);
    require(reader && reader->header().bloomSize == sizeof(sst::SSTBloomHeaderV1) + 64 && reader->get(key)->seq == rows.size(),
        "repeated versions inflated the Bloom or changed version selection");
    reader.reset(); fs::remove_all(root, error);
}

akkaradb::core::ArenaGenerator<akkaradb::core::RecordView> pausedSplitFlush(
    std::span<const akkaradb::engine::sst::SSTRecord> rows, std::atomic<bool>& entered, std::atomic<bool>& released) {
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i == 25) {
            entered.store(true, std::memory_order_release);
            waitForSst([&] { return released.load(std::memory_order_acquire); });
        }
        const auto& row = rows[i];
        co_yield record(row.key, row.value, row.seq, row.flags);
    }
}

void testSplitFlushPublication() {
    namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst;
    const auto dir = fs::current_path() / ".bench-tmp" / "sst-split-publication";
    std::error_code error; fs::remove_all(dir, error);
    sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED; options.targetFileSize = 4096;
    auto manager = sst::SSTManager::create(dir, options);
    const auto rows = flushSizingRows();
    std::atomic<bool> entered{false}, released{false};
    std::exception_ptr failure;
    std::thread flush([&] {
        try { manager->flush(0, pausedSplitFlush(rows, entered, released)); }
        catch (...) { failure = std::current_exception(); }
    });
    try {
        waitForSst([&] { return entered.load(std::memory_order_acquire); });
        require(manager->levelStats()[0].fileCount == 0 && manager->maxSequence() == 0 &&
            !manager->get(rows.front().key) && !manager->scanIter().hasNext(), "reader observed an unfinished split flush");
        size_t staged = 0;
        for (const auto& entry : fs::directory_iterator(dir)) { staged += entry.path().extension() == ".aksst"; }
        require(staged > 1, "publication fixture did not stage multiple files before pausing");
    } catch (...) { released = true; flush.join(); throw; }
    released = true; flush.join();
    if (failure) { std::rethrow_exception(failure); }
    verifyFlushSizing(*manager, dir, rows, options.targetFileSize);
    manager.reset(); fs::remove_all(dir, error);
}

int runSplitFlushCrashChild(const std::string& mode, const std::string& point, const std::filesystem::path& dir) {
    namespace sst = akkaradb::engine::sst; namespace mf = akkaradb::engine::manifest;
    auto manifest = mode == "fast" ? mf::Manifest::create(dir / "external.akmf", true) : nullptr;
    sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED; options.targetFileSize = 4096;
    auto manager = sst::SSTManager::create(dir, options, manifest.get());
    const auto rows = flushSizingRows();
    setSstCrashPoint(point);
    manager->flush(0, transientStreamingFixture(rows, true));
    throw std::runtime_error("split flush missed crash point");
}

void testSplitFlushCrashRecovery(const std::filesystem::path& executable) {
    namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst; namespace mf = akkaradb::engine::manifest;
    const auto root = fs::current_path() / ".bench-tmp" / "sst-split-flush-crashes";
    std::error_code error; fs::remove_all(root, error);
    const auto rows = flushSizingRows();
    for (const std::string mode : {"standalone", "fast"}) {
        for (const std::string point : {"sst.flush.after_outputs", "sst.flush.after_manifest_refs", "sst.flush.after_manifest_seal"}) {
            const auto dir = root / (mode + "-" + point); fs::create_directories(dir);
            const auto args = "\"" + executable.string() + "\" --sst-split-flush-child " + mode + " " + point + " \"" + dir.string() + "\"";
#ifdef _WIN32
            require(std::system(("\"" + args + "\"").c_str()) == 86, "split flush child missed its crash point");
#else
            require(std::system(args.c_str()) == (86 << 8), "split flush child missed its crash point");
#endif
            auto manifest = mode == "fast" ? mf::Manifest::create(dir / "external.akmf") : nullptr;
            sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED; options.targetFileSize = 4096;
            auto manager = sst::SSTManager::create(dir, options, manifest.get()); manager->recover();
            if (point == "sst.flush.after_manifest_seal") { verifyFlushSizing(*manager, dir, rows, options.targetFileSize); }
            else { require(!manager->scanIter().hasNext() && manager->maxSequence() == 0, "crash recovered a partial split flush"); }
        }
    }
    fs::remove_all(root, error);
}
