void testSstDirectPointReads() {
    namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst;
    const auto root = fs::current_path() / ".bench-tmp" / "sst-direct-point-reads";
    std::error_code ec; fs::remove_all(root, ec);
    for (const auto codec : {sst::SSTWriter::Codec::NONE, sst::SSTWriter::Codec::ZSTD}) {
        for (const uint64_t cacheBytes : {uint64_t{0}, uint64_t{4096}, uint64_t{65536}}) {
            const auto dir = root / (std::to_string(static_cast<int>(codec)) + "-" + std::to_string(cacheBytes));
            sst::SSTManager::Options options;
            options.compactionMode = sst::SSTCompactionMode::DISABLED;
            options.codec = codec; options.blockCacheBytes = cacheBytes; options.blockSize = 4096;
            auto manager = sst::SSTManager::create(dir / "managed", options);
            const std::vector<uint8_t> key(64, 'k'), missing{'m'}, emptyKey{'z'}, sentinel{'!'}, empty;
            const std::vector<uint8_t> oldest(100, 'o'), middle(1000, 'm'), latest(5000, 'n');
            // The newest sequence is in the oldest file, after other candidates
            // in lookup order. Candidate selection must not replace out's buffer.
            manager->flush(std::vector{record(key, latest, 50)});
            manager->flush(std::vector{record(key, oldest, 10)});
            manager->flush(std::vector{record(key, empty, 30, sst::SST_RECORD_FLAG_TOMBSTONE)});
            manager->flush(std::vector{record(key, middle, 40), record(emptyKey, empty, 6)});
            const auto snapshot = manager->readSnapshot();
            std::vector<uint8_t> out;
            out.reserve(UINT16_MAX);
            const auto* buffer = out.data();
            const auto capacity = out.capacity();
            for (const bool pinned : {false, true}) {
                const auto read = [&](std::span<const uint8_t> target, uint64_t seq) {
                    return pinned ? snapshot.getInto(target, out, seq) : manager->getInto(target, out, seq);
                };
                for (const uint64_t seq : {uint64_t{0}, uint64_t{20}, uint64_t{35}, uint64_t{45}, UINT64_MAX}) {
                    out.assign_range(sentinel);
                    const auto result = read(key, seq);
                    const auto contains = pinned ? snapshot.contains(key, seq) : manager->contains(key, seq);
                    if (seq == 0) { require(!result && !contains && out == sentinel, "getInto snapshot miss modified output"); }
                    else if (seq == 35) { require(result && !*result && contains && !*contains && out == sentinel, "getInto tombstone modified output"); }
                    else {
                        const auto& expected = seq == 20 ? oldest : seq == 45 ? middle : latest;
                        require(result && *result && contains && *contains && out == expected, "direct point read selected the wrong version");
                    }
                    require(out.data() == buffer && out.capacity() == capacity, "getInto replaced the caller's reusable buffer");
                }
                out.assign_range(sentinel);
                require(!read(missing, UINT64_MAX) && out == sentinel, "getInto absent key modified output");
                const auto result = read(emptyKey, UINT64_MAX);
                require(result && *result && out.empty(), "getInto failed to return an empty live value");
                require(out.data() == buffer && out.capacity() == capacity, "empty getInto discarded reusable capacity");
                out.assign_range(key);
                require(read(out, UINT64_MAX) == std::optional{true} && out == latest, "getInto invalidated a key aliased into out");
            }
            out.assign_range(sentinel);
            require(!sst::SSTManager::ReadSnapshot{}.getInto(key, out) && out == sentinel &&
                !sst::SSTManager::ReadSnapshot{}.contains(key), "default snapshot changed direct-read semantics");

            const auto path = dir / "borrowed.aksst";
            sst::SSTWriter::Options writerOptions;
            writerOptions.codec = codec; writerOptions.blockSize = 4096;
            (void)sst::SSTWriter::write(path, std::vector{
                record(key, latest, 50), record(key, latest, 10), record(key, latest, 30)
            }, writerOptions);
            auto reader = sst::SSTReader::open(path, {.blockCacheBytes = cacheBytes});
            require(reader && reader->header().blockCount == 3, "borrowed record fixture must span blocks");
            auto pin = reader->getPinned(key);
            require(pin && pin->view.seq() == 50, "borrowed lookup selected the wrong version");
            auto retained = *pin;
            pin.reset(); reader.reset();
            require(std::ranges::equal(retained.view.key(), key) && std::ranges::equal(retained.view.value(), latest),
                "borrowed record did not survive cache eviction or reader destruction");
            const auto owned = retained.toOwned();
            retained = {};
            require(owned.key == key && owned.value == latest && owned.seq == 50, "materialized record retained a borrowed lifetime");
        }
    }
    fs::remove_all(root, ec);
}
