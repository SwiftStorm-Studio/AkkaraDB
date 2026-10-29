void appendSstSealFixture(const std::filesystem::path& path, const std::string& filename) {
    namespace mf = akkaradb::engine::manifest;
    const auto payload = mf::encodeSstSeal(1, 0, filename, 1, std::nullopt, std::nullopt);
    uint8_t header[mf::ManifestRecordHeader::SIZE];
    mf::ManifestRecordHeader::build(mf::ManifestRecordType::SST_SEAL, payload.data(), static_cast<uint32_t>(payload.size())).serialize(header);
    std::ofstream out(path, std::ios::binary | std::ios::app);
    out.write(reinterpret_cast<const char*>(header), sizeof(header));
    out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    require(static_cast<bool>(out), "cannot append rotated SST seal fixture");
}

void testManifestRotationGaps() {
    namespace fs = std::filesystem; namespace mf = akkaradb::engine::manifest; namespace sst = akkaradb::engine::sst;
    const auto root = fs::current_path() / ".bench-tmp" / "manifest-rotation-gaps";
    std::error_code ec; fs::remove_all(root, ec);
    const std::vector<uint8_t> a{'a'}, b{'b'}, value{'v'};
    for (const std::string mode : {"standalone", "sync", "fast"}) {
        for (const bool legacy : {false, true}) {
            const auto dir = root / (mode + (legacy ? "-legacy" : "-modern")); fs::create_directories(dir);
            const auto path = dir / (mode == "standalone" ? "sst-state.akmf" : "external.akmf");
            sst::SSTManager::Options opts; opts.compactionMode = sst::SSTCompactionMode::DISABLED;
            std::unique_ptr<mf::Manifest> manifest;
            if (mode != "standalone") { manifest = mf::Manifest::create(path, mode == "fast"); }
            auto manager = sst::SSTManager::create(dir, opts, manifest.get());
            const std::vector rows{record(a, value, 1)};
            manager->flush(rows); manager.reset();
            if (manifest) { manifest->close(); manifest.reset(); }
            const std::vector second{record(b, value, 2)};
            writeSstFile(dir / "L0_2.aksst", 0, second);
            writeManifestRotation(path, 1, 1); writeManifestRotation(path, 2, 2);
            auto first = dir / (path.stem().string() + "-1.akmf");
            auto last = dir / (path.stem().string() + "-2.akmf");
            if (legacy) {
                const auto legacyFirst = fs::path{path.string() + ".1"}, legacyLast = fs::path{path.string() + ".2"};
                fs::rename(first, legacyFirst); fs::rename(last, legacyLast); first = legacyFirst; last = legacyLast;
            }
            appendSstSealFixture(last, "L0_2.aksst");
            const auto firstBytes = readFileBytes(first), baseBytes = readFileBytes(path), lastBytes = readFileBytes(last);
            const auto sstBytes = readFileBytes(dir / "L0_2.aksst");
            fs::remove(first);
            const auto tmp = dir / "interrupted.tmp"; { std::ofstream out(tmp); out << "temporary"; }
            bool rejected = false;
            try {
                if (mode == "standalone") { manager = sst::SSTManager::create(dir, opts); manager->recover(); }
                else { manifest = mf::Manifest::create(path, mode == "fast"); manager = sst::SSTManager::create(dir, opts, manifest.get()); manager->recover(); }
            } catch (const std::runtime_error&) { rejected = true; }
            manager.reset(); manifest.reset();
            require(rejected, "missing middle Manifest rotation must reject recovery");
            require(readFileBytes(path) == baseBytes && readFileBytes(last) == lastBytes &&
                readFileBytes(dir / "L0_2.aksst") == sstBytes && fs::exists(tmp), "gap rejection changed the catalog or cleaned SST artifacts");
            { std::ofstream out(first, std::ios::binary); out.write(reinterpret_cast<const char*>(firstBytes.data()), static_cast<std::streamsize>(firstBytes.size())); }
            if (mode != "standalone") { manifest = mf::Manifest::create(path, mode == "fast"); }
            manager = sst::SSTManager::create(dir, opts, manifest.get()); manager->recover();
            require(manager->get(a) && manager->get(b) && !fs::exists(tmp), "restored rotation chain did not recover both SSTs");
            manager.reset(); manifest.reset();
        }
    }
    fs::remove_all(root, ec);
}

void runManifestRotationSyncChild(const std::filesystem::path& dir, bool crash) {
    namespace mf = akkaradb::engine::manifest;
    auto manifest = mf::Manifest::create(dir / "external.akmf", true);
    std::vector<mf::Manifest::SSTBlobRefsEvent::Entry> entries;
    for (uint64_t i = 0; i < 600; ++i) {
        std::vector<uint8_t> key(60000, 'k'); key[0] = static_cast<uint8_t>(i >> 8); key[1] = static_cast<uint8_t>(i);
        entries.push_back({std::move(key), i + 1, 0, std::nullopt});
    }
    manifest->sstBlobRefs("retained.aksst", entries);
    if (crash) { setSstCrashPoint("manifest.rotation.after_sync"); }
    manifest->sstSeal(0, "retained.aksst", entries.size(), {}, {});
    manifest->close();
}

void testManifestRotationSync(const std::filesystem::path& executable) {
    namespace fs = std::filesystem; namespace mf = akkaradb::engine::manifest;
    const auto root = fs::current_path() / ".bench-tmp" / "manifest-rotation-sync";
    std::error_code ec; fs::remove_all(root, ec); fs::create_directories(root / "crash");
    const std::string args = "\"" + executable.string() + "\" --manifest-rotation-sync-child \"" + (root / "crash").string() + "\"";
#ifdef _WIN32
    require(std::system(("\"" + args + "\"").c_str()) == 86, "rotation did not stop after synchronizing the old file");
#else
    require(std::system(args.c_str()) == (86 << 8), "rotation did not stop after synchronizing the old file");
#endif
    {
        auto manifest = mf::Manifest::create(root / "crash" / "external.akmf");
        require(manifest->liveSst().empty() && manifest->sstBlobRefs().size() == 1 && manifest->sstBlobRefs()[0].entries.size() == 600,
            "rotation boundary lost preceding metadata or exposed an unsealed SST");
        manifest->sstSeal(0, "retained.aksst", 600, {}, {});
        require(manifest->liveSstBlobRefs() && manifest->liveSstBlobRefs()->size() == 600, "barrier after recovered rotation lost live references");
    }
    runManifestRotationSyncChild(root / "success", false);
    {
        auto manifest = mf::Manifest::create(root / "success" / "external.akmf");
        require(manifest->liveSstBlobRefs() && manifest->liveSstBlobRefs()->size() == 600, "fast barrier across rotations lost live metadata");
        manifest->compact();
    }
    fs::remove_all(root, ec);
}

void testSstRenameFailure() {
    namespace fs = std::filesystem; namespace mf = akkaradb::engine::manifest; namespace sst = akkaradb::engine::sst;
    const auto dir = fs::current_path() / ".bench-tmp" / "sst-rename-failure";
    std::error_code ec; fs::remove_all(dir, ec);
    auto manifest = mf::Manifest::create(dir / "external.akmf");
    sst::SSTManager::Options opts; opts.compactionMode = sst::SSTCompactionMode::DISABLED;
    auto manager = sst::SSTManager::create(dir, opts, manifest.get());
    fs::create_directory(dir / "L0_1.aksst");
    const std::vector<uint8_t> key{'k'}, value{'v'}; const std::vector rows{record(key, value, 1)};
    bool rejected = false;
    try { manager->flush(rows); } catch (const std::runtime_error&) { rejected = true; }
    require(rejected && manifest->liveSst().empty() && !manager->get(key), "failed SST publication was acknowledged or sealed");
    manager.reset(); manifest.reset(); fs::remove_all(dir, ec);
}

void testLiveSstBlobSnapshot() {
    namespace fs = std::filesystem; namespace mf = akkaradb::engine::manifest;
    const auto dir = fs::current_path() / ".bench-tmp" / "sst-blob-reference-snapshot";
    std::error_code ec; fs::remove_all(dir, ec);
    auto manifest = mf::Manifest::create(dir / "external.akmf");
    require(manifest->liveSstBlobRefs() && manifest->liveSstBlobRefs()->empty(), "empty catalog must have complete empty references");
    manifest->sstBlobRefs("old.aksst", {{{'k'}, 1, 2, 11}});
    require(manifest->liveSstBlobRefs()->empty(), "unsealed SST references must not be live");
    manifest->sstSeal(0, "old.aksst", 1, {}, {});
    require(manifest->liveSstBlobRefs()->size() == 1 && manifest->liveSstBlobRefs()->front().blobId == 11, "sealed SST reference missing from snapshot");
    manifest->compactionCommit({"new.aksst"}, {"old.aksst"});
    require(!manifest->liveSstBlobRefs(), "incomplete live metadata must request the SST-scan fallback");
    manifest->sstBlobRefs("new.aksst", {{{'k'}, 2, 0, std::nullopt}});
    const auto refs = manifest->liveSstBlobRefs();
    require(refs && refs->size() == 1 && refs->front().seq == 2 && !refs->front().blobId,
        "reference snapshot included retired-file history");
    manifest->close(); manifest.reset(); fs::remove_all(dir, ec);
}
