void testSstCatalogBinding() {
    namespace fs = std::filesystem; namespace mf = akkaradb::engine::manifest; namespace sst = akkaradb::engine::sst;
    const auto root = fs::current_path() / ".bench-tmp" / "sst-catalog-binding";
    std::error_code ec; fs::remove_all(root, ec);
    const std::vector<uint8_t> key{'k'}, value{'v'};
    sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED;
    for (const bool fast : {false, true}) {
        const auto dir = root / (fast ? "fast" : "sync"); fs::create_directories(dir);
        const auto original = dir / "original.akmf", wrong = dir / "wrong.akmf";
        auto catalog = mf::Manifest::create(original, fast);
        const auto identity = catalog->catalogIdentity();
        auto manager = sst::SSTManager::create(dir, options, catalog.get());
        manager->flush(std::vector{record(key, value, 1)}); manager.reset();
        const auto sstPath = dir / "L0_1.aksst", binding = dir / "sst-catalog.akid";
        const auto sstBytes = readFileBytes(sstPath), bindingBytes = readFileBytes(binding);
        const auto temporary = dir / "interrupted.tmp";
        { std::ofstream out{temporary}; out << "retained on rejection"; }
        const auto reject = [&](mf::Manifest* candidate) {
            bool rejected = false;
            try { auto invalid = sst::SSTManager::create(dir, options, candidate); invalid->recover(); }
            catch (const std::runtime_error&) { rejected = true; }
            require(rejected && readFileBytes(sstPath) == sstBytes && fs::exists(temporary),
                    "mismatched catalog must reject before changing SSTs or temporary files");
        };
        for (int pass = 0; pass < 2; ++pass) {
            auto other = mf::Manifest::create(wrong, fast); reject(other.get());
        }
        // A nonempty unrelated catalog must not bypass the binding either.
        { auto other = mf::Manifest::create(wrong, fast); other->sstSeal(0, "L0_1.aksst", 1, {}, {}); reject(other.get()); }
        require(readFileBytes(binding) == bindingBytes, "mismatch rejection replaced the original binding");
        catalog->compact();
        require(catalog->catalogIdentity() == identity, "Manifest compaction changed its identity");
        catalog.reset();
        // Recreating the catalog at the same path is also a different catalog.
        fs::rename(original, dir / "original.saved");
        { auto replacement = mf::Manifest::create(original, fast); reject(replacement.get()); }
        fs::remove(original); fs::rename(dir / "original.saved", original);
        catalog = mf::Manifest::create(original, fast);
        require(catalog->catalogIdentity() == identity, "reopen changed the catalog identity");
        manager = sst::SSTManager::create(dir, options, catalog.get()); manager->recover();
        require(manager->get(key) && !fs::exists(temporary), "original catalog did not recover and clean its orphan");
        manager.reset();

        // A legacy database is bound after its live set has been validated.
        fs::remove(binding);
        manager = sst::SSTManager::create(dir, options, catalog.get()); manager->recover();
        require(manager->get(key) && readFileBytes(binding) == bindingBytes, "legacy catalog failed to acquire its binding");
        manager.reset();
        auto damaged = bindingBytes; damaged.back() ^= 1;
        { std::ofstream out{binding, std::ios::binary | std::ios::trunc}; out.write(reinterpret_cast<const char*>(damaged.data()), damaged.size()); }
        bool rejected = false;
        try { (void)sst::SSTManager::create(dir, options, catalog.get()); } catch (const std::runtime_error&) { rejected = true; }
        require(rejected && readFileBytes(sstPath) == sstBytes, "corrupt binding must preserve existing SSTs");
        fs::remove(binding);
        // Header-only catalogs without a binding cannot claim legacy SST files.
        const auto emptyPath = dir / "empty.akmf";
        for (int pass = 0; pass < 2; ++pass) {
            auto empty = mf::Manifest::create(emptyPath, fast); rejected = false;
            try { (void)sst::SSTManager::create(dir, options, empty.get()); } catch (const std::runtime_error&) { rejected = true; }
            require(rejected && readFileBytes(sstPath) == sstBytes && !fs::exists(binding), "empty legacy catalog claimed existing SSTs");
        }
    }
    fs::remove_all(root, ec);
}

#ifdef _WIN32
// Exercise the actual OS write/sync boundary without adding production fault hooks.
class ManifestIoFault {
    using Write = BOOL(WINAPI*)(HANDLE, LPCVOID, DWORD, LPDWORD, LPOVERLAPPED);
    using Flush = BOOL(WINAPI*)(HANDLE);
    inline static Write write_ = nullptr;
    inline static Flush flush_ = nullptr;
    inline static std::atomic<int> armed_{0};
    inline static std::atomic<HANDLE> pendingSync_{nullptr};
    ULONG_PTR *writeSlot_ = nullptr, *flushSlot_ = nullptr;

    static BOOL WINAPI write(HANDLE file, LPCVOID data, DWORD size, LPDWORD written, LPOVERLAPPED overlapped) {
        const bool commit = size >= 16 && static_cast<const uint8_t*>(data)[0] == 8;
        const int mode = commit ? armed_.exchange(0) : 0;
        if (mode == 2) {
            const auto result = write_(file, data, size / 2, written, overlapped);
            if (result) { ::SetLastError(ERROR_WRITE_FAULT); }
            return FALSE;
        }
        const auto result = write_(file, data, size, written, overlapped);
        if (result && mode == 1) { pendingSync_ = file; }
        return result;
    }
    static BOOL WINAPI flush(HANDLE file) {
        HANDLE expected = file;
        if (pendingSync_.compare_exchange_strong(expected, nullptr)) {
            (void)flush_(file); // The complete event can survive a reported sync failure.
            ::SetLastError(ERROR_WRITE_FAULT);
            return FALSE;
        }
        return flush_(file);
    }
    static void replace(ULONG_PTR* slot, ULONG_PTR value) {
        DWORD old = 0;
        require(::VirtualProtect(slot, sizeof(*slot), PAGE_READWRITE, &old) != 0, "cannot patch Manifest I/O import");
        *slot = value;
        DWORD ignored = 0;
        require(::VirtualProtect(slot, sizeof(*slot), old, &ignored) != 0, "cannot restore Manifest import protection");
    }
    static ULONG_PTR* findImport(HMODULE module, const char* name) {
        auto* base = reinterpret_cast<uint8_t*>(module);
        auto* dos = reinterpret_cast<IMAGE_DOS_HEADER*>(base);
        auto* nt = reinterpret_cast<IMAGE_NT_HEADERS*>(base + dos->e_lfanew);
        auto* imports = reinterpret_cast<IMAGE_IMPORT_DESCRIPTOR*>(base + nt->OptionalHeader.DataDirectory[IMAGE_DIRECTORY_ENTRY_IMPORT].VirtualAddress);
        for (; imports->Name; ++imports) {
            auto* names = reinterpret_cast<IMAGE_THUNK_DATA*>(base + imports->OriginalFirstThunk);
            auto* slots = reinterpret_cast<IMAGE_THUNK_DATA*>(base + imports->FirstThunk);
            for (; names->u1.AddressOfData; ++names, ++slots) {
                if (IMAGE_SNAP_BY_ORDINAL(names->u1.Ordinal)) { continue; }
                auto* entry = reinterpret_cast<IMAGE_IMPORT_BY_NAME*>(base + names->u1.AddressOfData);
                if (std::strcmp(reinterpret_cast<const char*>(entry->Name), name) == 0) { return &slots->u1.Function; }
            }
        }
        throw std::runtime_error("Manifest I/O import was not found");
    }
public:
    ManifestIoFault() {
        HMODULE module = nullptr;
        require(::GetModuleHandleExW(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                reinterpret_cast<LPCWSTR>(&akkaradb::engine::manifest::Manifest::create), &module) != 0, "cannot locate Manifest module");
        writeSlot_ = findImport(module, "WriteFile"); flushSlot_ = findImport(module, "FlushFileBuffers");
        write_ = reinterpret_cast<Write>(*writeSlot_); flush_ = reinterpret_cast<Flush>(*flushSlot_);
        replace(writeSlot_, reinterpret_cast<ULONG_PTR>(&write)); replace(flushSlot_, reinterpret_cast<ULONG_PTR>(&flush));
    }
    ~ManifestIoFault() {
        replace(writeSlot_, reinterpret_cast<ULONG_PTR>(write_)); replace(flushSlot_, reinterpret_cast<ULONG_PTR>(flush_));
    }
    void arm(bool partial) { armed_ = partial ? 2 : 1; }
};

void testManifestTerminalIoFailure() {
    namespace fs = std::filesystem; namespace mf = akkaradb::engine::manifest;
    const auto root = fs::current_path() / ".bench-tmp" / "manifest-terminal-io";
    std::error_code ec; fs::remove_all(root, ec);
    ManifestIoFault fault;
    for (const bool fast : {false, true}) {
        for (const bool partial : {false, true}) {
            const auto path = root / (std::to_string(fast) + "-" + std::to_string(partial)) / "manifest.akmf";
            auto manifest = mf::Manifest::create(path, fast);
            manifest->sstSeal(0, "old.aksst", 1, {}, {});
            fault.arm(partial);
            const auto reject = [](auto&& action) {
                bool rejected = false;
                try { action(); } catch (const std::runtime_error&) { rejected = true; }
                require(rejected, "Manifest accepted a mutation after an uncertain I/O outcome");
            };
            reject([&] { manifest->compactionCommit({"first.aksst"}, {"old.aksst"}); });
            const auto failedBytes = readFileBytes(path);
            reject([&] { manifest->compactionCommit({"retry.aksst"}, {"old.aksst"}); });
            reject([&] { manifest->sstSeal(0, "later.aksst", 1, {}, {}); });
            reject([&] { manifest->checkpoint("later", {}, 100); });
            reject([&] { manifest->advance(100); });
            reject([&] { manifest->sstBlobRefs("later.aksst", {}); });
            reject([&] { manifest->compact(); });
            reject([&] { manifest->replay(); });
            require(manifest->stripesWritten() == 0 && manifest->liveSst() == std::vector<std::string>{"old.aksst"} &&
                    readFileBytes(path) == failedBytes, "terminal Manifest mutation changed memory or durable state");
            reject([&] { manifest->close(); });
            manifest.reset(); // Destruction must remain safe after explicit failure reporting.
            manifest = mf::Manifest::create(path, fast);
            require(manifest->liveSst() == std::vector<std::string>{partial ? "old.aksst" : "first.aksst"},
                    "reopen must resolve exactly the complete or partial original commit");
            manifest->checkpoint("recovered", {}, 1);
        }
    }
    fs::remove_all(root, ec);
}

void testSstCompactionTerminalIoFailure() {
    namespace fs = std::filesystem; namespace mf = akkaradb::engine::manifest; namespace sst = akkaradb::engine::sst;
    const auto root = fs::current_path() / ".bench-tmp" / "sst-compaction-terminal-io";
    std::error_code ec; fs::remove_all(root, ec);
    ManifestIoFault fault;
    const std::vector<uint8_t> a{'a'}, z{'z'}, old{'o'}, updated{'n'};
    for (const bool fast : {false, true}) {
        const auto dir = root / std::to_string(fast);
        auto manifest = mf::Manifest::create(dir / "external.akmf", fast);
        sst::SSTManager::Options options; options.maxLevels = 2; options.maxL0Files = 2; options.compactThreads = 2;
        auto manager = sst::SSTManager::create(dir, options, manifest.get());
        manager->flush(std::vector{record(a, old, 1), record(z, old, 2)});
        fault.arm(false);
        manager->flush(std::vector{record(z, old, 3)});
        waitForSst([&] { return manager->compactionSnapshot().compactionFailures != 0; });
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        require(manager->compactionSnapshot().compactionFailures == 1 && manager->compactionSnapshot().compactionsCompleted == 0,
                "compaction retried after an uncertain commit outcome");
        bool rejected = false;
        try { manager->flush(std::vector{record(a, updated, 10)}); } catch (const std::runtime_error&) { rejected = true; }
        require(rejected, "SST flush continued after background publication failed");
        manager.reset(); manifest.reset();
        manifest = mf::Manifest::create(dir / "external.akmf", fast);
        manager = sst::SSTManager::create(dir, options, manifest.get()); manager->recover();
        require(manager->levelStats()[1].fileCount == 1 && manager->get(a)->seq == 1 && manager->get(z)->seq == 3,
                "recovery must retain exactly one committed compaction output");
        manager->flush(std::vector{record(a, updated, 10), record(z, updated, 11)});
        manager->flush(std::vector{record(z, updated, 12)});
        waitForSst([&] { manager->throwIfBackgroundFailed(); return manager->compactionSnapshot().compactionsCompleted != 0; });
        manager.reset(); manifest.reset();
        manifest = mf::Manifest::create(dir / "external.akmf", fast);
        manager = sst::SSTManager::create(dir, options, manifest.get()); manager->recover();
        require(manager->get(a)->seq == 10 && manager->get(z)->seq == 12, "old compaction output hid updated values after restart");
        auto scan = manager->scanIter();
        require(scan.next()->seq == 10 && scan.next()->seq == 12 && !scan.hasNext(), "point reads and scan disagree after commit recovery");
    }
    fs::remove_all(root, ec);
}
#endif
