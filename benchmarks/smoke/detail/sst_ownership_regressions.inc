int runSstOwnershipChild(const std::filesystem::path& dir, bool expectLocked, bool crashOwner) {
    namespace sst = akkaradb::engine::sst;
    sst::SSTManager::Options options; options.compactThreads = 0;
    try {
        auto manager = sst::SSTManager::create(dir, options);
        manager->recover();
        if (expectLocked) { return 1; }
        if (crashOwner) {
            const std::vector<uint8_t> key{'a'}, value{'v'};
            manager->flush(std::vector{record(key, value, 1)});
            std::quick_exit(86);
        }
        return 0;
    } catch (const std::runtime_error&) { return expectLocked ? 0 : 1; }
}

void testSstExclusiveOwnership(const std::filesystem::path& executable) {
    namespace fs = std::filesystem;
    namespace sst = akkaradb::engine::sst;
    namespace mf = akkaradb::engine::manifest;
    const auto root = fs::current_path() / ".bench-tmp" / "sst-exclusive-ownership";
    std::error_code ec; fs::remove_all(root, ec);
    const std::vector<uint8_t> key{'a'}, value{'v'};
    sst::SSTManager::Options options; options.compactThreads = 0;
    for (const bool external : {false, true}) {
        const auto dir = root / (external ? "external" : "standalone");
        auto manifest = external ? mf::Manifest::create(dir / "external.akmf") : nullptr;
        auto first = sst::SSTManager::create(dir, options, manifest.get()); first->recover();
        first->flush(std::vector{record(key, value, 1)});
        const auto before = readFileBytes(dir / "L0_1.aksst");
        const auto reject = [&](const fs::path& path) {
            bool rejected = false;
            try { auto second = sst::SSTManager::create(path, options, manifest.get()); }
            catch (const std::runtime_error&) { rejected = true; }
            require(rejected, "a second SST owner was admitted");
        };
        reject(dir); reject(dir / ".");
        require(readFileBytes(dir / "L0_1.aksst") == before && first->get(key)->value == value,
            "rejected SST ownership changed live data");
        first->shutdown(); reject(dir);
        first.reset();
        auto reopened = sst::SSTManager::create(dir, options, manifest.get()); reopened->recover();
        require(reopened->get(key)->value == value, "ownership was not released on SST destruction");
    }

    const auto processDir = root / "process";
    auto owner = sst::SSTManager::create(processDir, options); owner->recover();
    const auto child = [&](const char* mode, int expected) {
        const std::string command = "\"" + executable.string() + "\" --sst-ownership-child " + mode + " \"" + processDir.string() + "\"";
        #ifdef _WIN32
        const int status = std::system(("\"" + command + "\"").c_str());
        require(status == expected, "SST ownership child returned an unexpected status");
        #else
        const int status = std::system(command.c_str());
        require(status == (expected << 8), "SST ownership child returned an unexpected status");
        #endif
    };
    child("locked", 0);
    owner.reset(); child("open", 0); child("crash", 86);
    owner = sst::SSTManager::create(processDir, options); owner->recover();
    require(owner->get(key)->value == value, "process exit did not release SST ownership with data intact");
    owner.reset();

    const auto manifestPath = root / "manifest" / "state.akmf";
    auto manifest = mf::Manifest::create(manifestPath);
    manifest->sstSeal(0, "retained.aksst", 1, {}, {});
    const auto rejectManifest = [&] {
        bool rejected = false;
        try { auto second = mf::Manifest::create(manifestPath); }
        catch (const std::runtime_error&) { rejected = true; }
        require(rejected, "a second Manifest owner was admitted");
    };
    rejectManifest(); manifest->compact(); rejectManifest();
    manifest->close();
    auto reopened = mf::Manifest::create(manifestPath);
    require(reopened->liveSst() == std::vector<std::string>{"retained.aksst"}, "Manifest close failed to release ownership");
    const auto beforeClosedWrites = readFileBytes(manifestPath);
    const auto rejectClosed = [&](auto&& operation) {
        bool rejected = false;
        try { operation(); } catch (const std::runtime_error&) { rejected = true; }
        require(rejected, "a closed Manifest mutated storage after releasing ownership");
    };
    rejectClosed([&] { manifest->sstSeal(0, "late.aksst", 1, {}, {}); });
    rejectClosed([&] { manifest->start(); });
    rejectClosed([&] { manifest->replay(); });
    rejectClosed([&] { manifest->compact(); });
    require(readFileBytes(manifestPath) == beforeClosedWrites, "closed Manifest changed its replacement owner's catalog");
    reopened.reset(); manifest.reset(); fs::remove_all(root, ec);
}

void testSstReadSnapshotAcrossCompaction() {
    namespace fs = std::filesystem;
    namespace sst = akkaradb::engine::sst;
    const auto dir = fs::current_path() / ".bench-tmp" / "sst-read-snapshot-compaction";
    std::error_code ec; fs::remove_all(dir, ec);
    const std::vector<uint8_t> key{'k'}, oldValue{'o'}, newValue{'n'};
    sst::SSTManager::Options options; options.compactThreads = 1; options.maxL0Files = 2;
    {
        auto manager = sst::SSTManager::create(dir, options);
        manager->flush(std::vector{record(key, oldValue, 1)});
        const auto snapshot = manager->readSnapshot();
        manager->flush(std::vector{record(key, newValue, 2)});
        waitForSst([&] { manager->throwIfBackgroundFailed(); return manager->compactionSnapshot().compactionsCompleted != 0; });
        require(snapshot.maxSequence() == 1 && snapshot.get(key, 1)->value == oldValue, "read snapshot lost a compacted version");
        require(manager->get(key)->value == newValue, "read snapshot changed the current SST head");
    }
    fs::remove_all(dir, ec);
}
