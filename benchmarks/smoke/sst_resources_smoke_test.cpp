#include "TestErrorHandlers.hpp"
#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/engine/sstable/SSTManager.hpp"

#include <atomic>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <new>
#include <stdexcept>
#include <thread>

#ifdef _WIN32
#include <windows.h>
#elif defined(__linux__)
#include <sys/resource.h>
#endif

namespace {
    thread_local size_t allocationLimit = SIZE_MAX;
}

// Reject a damaged block's allocation attempt without actually reserving GiBs.
void* operator new(size_t size) {
    if (size > allocationLimit) { throw std::bad_alloc{}; }
    if (auto* memory = std::malloc(size == 0 ? 1 : size)) { return memory; }
    throw std::bad_alloc{};
}
void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, size_t) noexcept { std::free(memory); }

namespace {
    namespace fs = std::filesystem;
    namespace sst = akkaradb::engine::sst;
    namespace mf = akkaradb::engine::manifest;

    void require(bool condition, const char* message) {
        if (!condition) { throw std::runtime_error(message); }
    }

    akkaradb::core::RecordView record(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value, uint64_t seq) {
        return {key.data(), static_cast<uint16_t>(key.size()), value.data(), static_cast<uint16_t>(value.size()), seq, 0,
            akkaradb::core::computeKeyFp64(key), akkaradb::core::buildMiniKey(key)};
    }

    template <typename Predicate>
    void waitFor(Predicate ready) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
        while (!ready()) {
            require(std::chrono::steady_clock::now() < deadline, "resource regression timed out");
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }

    void testDescriptorBudgetAndRetirement(const fs::path& root) {
        fs::create_directories(root);
        const std::vector<uint8_t> key{'k'}, value(5000, 'o'), newer(5000, 'n');
        const auto source = root / "source.aksst";
        (void)sst::SSTWriter::write(source, std::vector{record(key, value, 1)});
        auto relativeReader = sst::SSTReader::open(fs::relative(source), {.blockCacheBytes = 0});
        require(relativeReader != nullptr, "cannot open relative SST fixture");
        sst::SSTManager::Options options;
        options.blockCacheBytes = 0; options.maxL0Files = 2; options.compactThreads = 1;
        auto manager = sst::SSTManager::create(root / "managed", options);
        manager->flush(std::vector{record(key, value, 1)});
        auto snapshot = manager->readSnapshot();
        manager->flush(std::vector{record(key, newer, 2)});
        waitFor([&] { manager->throwIfBackgroundFailed(); return manager->compactionSnapshot().compactionsCompleted != 0; });
        const auto retired = root / "managed" / "L0_1.aksst";
        require(fs::exists(retired), "retirement removed a snapshot's source path");

#ifdef _WIN32
        DWORD before = 0, after = 0;
#elif defined(__linux__)
        struct DescriptorLimit {
            rlimit previous{};
            DescriptorLimit() {
                require(::getrlimit(RLIMIT_NOFILE, &previous) == 0, "cannot inspect descriptor limit");
                auto limited = previous;
                limited.rlim_cur = std::min<rlim_t>(previous.rlim_cur, 128);
                require(::setrlimit(RLIMIT_NOFILE, &limited) == 0, "cannot restrict descriptor limit");
            }
            ~DescriptorLimit() { ::setrlimit(RLIMIT_NOFILE, &previous); }
        } descriptorLimit;
#endif
        std::vector<std::unique_ptr<sst::SSTReader>> readers;
        for (size_t i = 0; i < 700; ++i) {
            const auto path = root / ("copy-" + std::to_string(i) + ".aksst");
            fs::copy_file(source, path);
            auto reader = sst::SSTReader::open(path, {.blockCacheBytes = 0});
            require(reader != nullptr, "SST inventory exhausted the descriptor budget");
            readers.push_back(std::move(reader));
#ifdef _WIN32
            // Measure after lazy CRT initialization and filling the shared cache.
            if (i == 127) { require(::GetProcessHandleCount(::GetCurrentProcess(), &before), "cannot inspect handle count"); }
#endif
        }
#ifdef _WIN32
        require(::GetProcessHandleCount(::GetCurrentProcess(), &after), "cannot inspect handle count");
        require(after <= before + 8, "SST descriptors grew with the reader inventory");
#endif
        std::atomic<bool> failed{false};
        std::vector<std::thread> workers;
        for (size_t thread = 0; thread < 80; ++thread) {
            workers.emplace_back([&, thread] {
                try {
                    for (size_t i = 0; i < 8; ++i) {
                        const auto row = readers[(thread * 31 + i * 113) % readers.size()]->get(key);
                        if (!row || row->value != value) { failed = true; }
                    }
                } catch (...) { failed = true; }
            });
        }
        for (auto& worker : workers) { worker.join(); }
        require(!failed, "descriptor eviction raced an active read");
        manager->shutdown(); manager.reset();
        {
            struct RestoreDirectory {
                fs::path path = fs::current_path();
                ~RestoreDirectory() { std::error_code error; fs::current_path(path, error); }
            } restore;
            fs::current_path(root);
            require(relativeReader->get(key)->value == value, "descriptor reopen depended on the current directory");
        }
        manager = sst::SSTManager::create(root / "managed", options);
        manager->recover();
        require(fs::exists(retired) && manager->get(key)->value == newer,
            "recovery removed a retained reader's source or lost the live head");
        manager.reset();
        auto independent = sst::SSTReader::open(retired, {.blockCacheBytes = 0});
        require(independent != nullptr, "cannot retain an independent reader for a retired path");
        require(snapshot.get(key)->value == value, "snapshot could not reopen an evicted retired source");
        auto iterator = snapshot.scanIter();
        snapshot = {};
        require(fs::exists(retired) && iterator.next()->value == value, "iterator lost its retired source");
        iterator = {};
        require(fs::exists(retired) && independent->get(key)->value == value,
            "retirement ignored an independent reader of the same file");
        independent.reset();
        require(!fs::exists(retired), "last reader release did not reclaim the retired SST");
    }

    void testReferenceMetadataBound(const fs::path& root) {
        fs::create_directories(root);
        for (const bool fast : {false, true}) {
            const auto path = root / (fast ? "fast.akmf" : "sync.akmf");
            const auto open = [&] {
                auto manifest = mf::Manifest::create(path, fast);
                if (fast) { manifest->start(); }
                return manifest;
            };
            auto manifest = open();
            std::string current = "initial.aksst";
            std::vector<mf::Manifest::SSTBlobRefsEvent::Entry> refs{{std::vector<uint8_t>(4096, 'k'), 1, 2, 11}};
            manifest->sstBlobRefs(current, refs);
            manifest->sstSeal(0, current, 1, {}, {});
            for (uint64_t i = 0; i < 40; ++i) {
                const auto next = "next-" + std::to_string(i) + ".aksst";
                refs.front().seq = i + 2;
                manifest->sstBlobRefs(next, refs);
                manifest->compactionCommit({next}, {current});
                current = next;
                const auto retained = manifest->sstBlobRefs();
                require(retained.size() == 1 && retained.front().file == current && retained.front().entries.front().seq == i + 2,
                    "compaction retained obsolete per-key reference history");
            }
            refs.front().blobId = 12;
            manifest->sstBlobRefs(current, refs);
            require(manifest->sstBlobRefs().size() == 1 && manifest->sstReferencedBlobs() == std::vector<uint64_t>{12},
                "replacement metadata retained an obsolete event or Blob");
            manifest->close(); manifest.reset();
            manifest = open();
            require(manifest->sstBlobRefs().size() == 1 && manifest->liveSstBlobRefs()->front().blobId == 12,
                "replay restored obsolete reference history or lost current metadata");

            // Compaction can run between an output's metadata and its seal.
            manifest->sstBlobRefs("pending.aksst", refs);
            manifest->compact();
            manifest->close(); manifest.reset();
            manifest = open();
            require(manifest->sstBlobRefs().size() == 2 && manifest->liveSst().size() == 1,
                "Manifest compaction lost pending metadata or published its SST");
            manifest->sstSeal(0, "pending.aksst", 1, {}, {});
            manifest->compactionEnd(1, "pending.aksst", {current}, 1, {}, {});
            require(manifest->sstBlobRefs().size() == 1 && manifest->liveSstBlobRefs()->front().blobId == 12,
                "legacy compaction retained obsolete reference metadata");
            manifest->sstDelete("pending.aksst");
            require(manifest->sstBlobRefs().empty(), "SST deletion retained per-key metadata");
            manifest->close(); manifest.reset();
            manifest = open();
            require(manifest->sstBlobRefs().empty(), "replay retained deleted SST metadata");

            sst::SSTManager::Options options;
            options.compactionMode = sst::SSTCompactionMode::DISABLED;
            const auto storage = root / (fast ? "fast-sst" : "sync-sst");
            auto manager = sst::SSTManager::create(storage, options, manifest.get());
            const std::vector<uint8_t> key{'k'}, value{'v'};
            manager->flush(std::vector{record(key, value, 1)});
            manifest->sstBlobRefs("abandoned-output.aksst", refs);
            manager.reset(); manifest->close(); manifest.reset();
            manifest = open();
            manager = sst::SSTManager::create(storage, options, manifest.get());
            manager->recover();
            require(manifest->sstBlobRefs().size() == 1 && manager->get(key)->value == value,
                "SST recovery kept abandoned output metadata or lost the live file");
            manager.reset(); manifest->close(); manifest.reset();
            manifest = open();
            require(manifest->sstBlobRefs().size() == 1, "abandoned output metadata returned after replay");
        }
    }

    void testRetirementRetry(const fs::path& root) {
#ifdef _WIN32
        for (const bool reopen : {false, true}) {
            const auto dir = root / (reopen ? "reopened-disabled" : "running");
            sst::SSTManager::Options options;
            options.maxL0Files = 2; options.compactThreads = 1;
            auto manager = sst::SSTManager::create(dir, options);
            const std::vector<uint8_t> key{'k'}, oldValue{'o'}, newValue{'n'};
            manager->flush(std::vector{record(key, oldValue, 1)});
            auto snapshot = manager->readSnapshot();
            manager->flush(std::vector{record(key, newValue, 2)});
            waitFor([&] { manager->throwIfBackgroundFailed(); return manager->compactionSnapshot().compactionsCompleted != 0; });
            const auto retired = dir / "L0_1.aksst";
            struct DeleteBlocker {
                HANDLE handle;
                ~DeleteBlocker() { if (handle != INVALID_HANDLE_VALUE) { ::CloseHandle(handle); } }
            } blocker{::CreateFileW(retired.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr)};
            require(blocker.handle != INVALID_HANDLE_VALUE, "cannot block retirement deletion");
            if (reopen) {
                manager.reset();
                options.compactionMode = sst::SSTCompactionMode::DISABLED;
                manager = sst::SSTManager::create(dir, options); manager->recover();
            }
            snapshot = {};
            require(fs::exists(retired), "delete blocker did not hold the retired file");
            ::CloseHandle(blocker.handle); blocker.handle = INVALID_HANDLE_VALUE;
            waitFor([&] { manager->throwIfBackgroundFailed(); return !fs::exists(retired); });
            require(manager->get(key)->value == newValue && manager->compactionSnapshot().compactionFailures == 0,
                "retirement retry changed live data or stopped compaction");
        }
#else
        (void)root;
#endif
    }

    void testCorruptBlockAllocation(const fs::path& root) {
        fs::create_directories(root);
#ifdef _WIN32
        // A shared engine DLL uses its own operator new. Bound the process too,
        // so this test rejects GiB allocations with either linkage model.
        struct ProcessMemoryLimit {
            HANDLE job = ::CreateJobObjectW(nullptr, nullptr);
            ProcessMemoryLimit() {
                if (!job) { throw std::runtime_error("cannot create allocation test job"); }
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits{};
                limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_PROCESS_MEMORY;
                limits.ProcessMemoryLimit = 256u * 1024u * 1024u;
                if (!::SetInformationJobObject(job, JobObjectExtendedLimitInformation, &limits, sizeof(limits)) ||
                    !::AssignProcessToJobObject(job, ::GetCurrentProcess())) {
                    ::CloseHandle(job);
                    throw std::runtime_error("cannot restrict allocation test process");
                }
            }
            ~ProcessMemoryLimit() {
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION unlimited{};
                ::SetInformationJobObject(job, JobObjectExtendedLimitInformation, &unlimited, sizeof(unlimited));
                ::CloseHandle(job);
            }
        } memoryLimit;
#endif
        const std::vector<uint8_t> key(512, 'k'), other(512, 'l'), value(6000, 'v');
        for (const auto codec : {sst::SSTWriter::Codec::NONE, sst::SSTWriter::Codec::ZSTD}) {
            // Repeated keys exercise prefix reconstruction as well as raw blocks.
            for (const bool prefix : {false, true}) {
                const auto path = root / (std::to_string(static_cast<int>(codec)) + (prefix ? "-prefix.aksst" : "-raw.aksst"));
                const auto rows = prefix ? std::vector{record(key, value, 1), record(key, value, 2)} :
                    std::vector{record(key, value, 1), record(other, value, 2)};
                sst::SSTWriter::Options options; options.codec = codec;
                (void)sst::SSTWriter::write(path, rows, options);
                {
                    auto valid = sst::SSTReader::open(path);
                    require(valid && valid->get(key)->value == value, "valid block fixture failed to read");
                }
                std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
                sst::SSTFileHeaderV1 header{};
                file.read(reinterpret_cast<char*>(&header), sizeof(header));
                file.seekg(header.dataOffset);
                sst::SSTBlockHeaderV1 block{};
                file.read(reinterpret_cast<char*>(&block), sizeof(block));
                require(bool(file), "cannot read block allocation fixture");
                block.uncompressedSize ^= 1u << 30;
                file.seekp(header.dataOffset);
                file.write(reinterpret_cast<const char*>(&block), sizeof(block));
                file.close();
                auto reader = sst::SSTReader::open(path, {.blockCacheBytes = 0});
                require(reader != nullptr, "block allocation fixture failed metadata validation");
                bool corrupt = false, oversized = false;
                allocationLimit = 1024 * 1024;
                try { (void)reader->get(key); }
                catch (const std::bad_alloc&) { oversized = true; }
                catch (const std::runtime_error&) { corrupt = true; }
                allocationLimit = SIZE_MAX;
                require(corrupt && !oversized, "damaged block size reached allocation before rejection");
            }
        }
    }

    void testOptionValidation(const fs::path& root) {
        const auto reject = [&](auto change, size_t index) {
            auto options = sst::SSTManager::Options{};
            options.compactionMode = sst::SSTCompactionMode::DISABLED;
            change(options);
            const auto path = root / std::to_string(index);
            bool rejected = false;
            try { auto manager = sst::SSTManager::create(path, options); }
            catch (const std::invalid_argument&) { rejected = true; }
            require(rejected && !fs::exists(path), "invalid options started storage or were not rejected");
        };
        reject([](auto& o) { o.maxL0Files = 0; }, 0);
        reject([](auto& o) { o.maxL0Files = -1; }, 1);
        reject([](auto& o) { o.compactThreads = -1; }, 2);
        reject([](auto& o) { o.l1MaxBytes = 0; }, 3);
        reject([](auto& o) { o.targetFileSize = 0; }, 4);
        reject([](auto& o) { o.levelSizeMultiplier = std::numeric_limits<double>::quiet_NaN(); }, 5);
        reject([](auto& o) { o.levelSizeMultiplier = std::numeric_limits<double>::infinity(); }, 6);
        reject([](auto& o) { o.levelSizeMultiplier = 0.5; }, 7);
        reject([](auto& o) { o.blockSize = 4088; }, 8);
        reject([](auto& o) { o.blockSize = 4097; }, 9);
        reject([](auto& o) { o.codec = static_cast<sst::SSTWriter::Codec>(255); }, 10);
        reject([](auto& o) { o.compactionMode = static_cast<sst::SSTCompactionMode>(255); }, 11);
        for (const auto mode : {sst::SSTCompactionMode::AUTO, sst::SSTCompactionMode::BACKGROUND}) {
            auto options = sst::SSTManager::Options{};
            options.compactionMode = mode; options.compactThreads = 0; options.maxL0Files = 1;
            options.levelSizeMultiplier = std::numeric_limits<double>::max();
            auto manager = sst::SSTManager::create(root / (mode == sst::SSTCompactionMode::AUTO ? "auto" : "background"), options);
            require(manager->levelStats().back().budgetBytes == UINT64_MAX, "large level budget did not saturate");
            manager->shutdown();
        }
    }

    void testShutdownStopsSelectingWork(const fs::path& root) {
        std::mutex mutex;
        std::condition_variable changed;
        bool entered = false, released = false;
        sst::SSTManager::Options options;
        options.maxL0Files = 1; options.compactThreads = 1; options.l1MaxBytes = 1;
        auto manager = sst::SSTManager::create(root, options, nullptr, [&] {
            std::unique_lock lock{mutex};
            entered = true; changed.notify_all();
            changed.wait(lock, [&] { return released; });
            return UINT64_MAX;
        });
        const std::vector<uint8_t> key{'k'}, value{'v'};
        manager->flush(std::vector{record(key, value, 1)});
        {
            std::unique_lock lock{mutex};
            if (!changed.wait_for(lock, std::chrono::seconds{10}, [&] { return entered; })) {
                released = true; changed.notify_all();
                throw std::runtime_error("compaction did not reach shutdown boundary");
            }
        }
        std::atomic<bool> stopping{false};
        std::thread closer([&] { stopping = true; manager->shutdown(); });
        waitFor([&] { return stopping.load(); });
        // The active compaction is paused, keeping shutdown in its join phase.
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
        { std::lock_guard lock{mutex}; released = true; }
        changed.notify_all(); closer.join();
        manager->throwIfBackgroundFailed();
        require(manager->compactionSnapshot().compactionsCompleted == 1,
            "shutdown selected additional work after finishing its active compaction");
        require(manager->get(key)->value == value, "shutdown lost the active compaction's output");
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        const auto root = fs::current_path() / ".bench-tmp" / "sst-resources";
        std::error_code error;
        fs::remove_all(root, error);
        testDescriptorBudgetAndRetirement(root / "descriptors");
        testRetirementRetry(root / "retirement-retry");
        testReferenceMetadataBound(root / "references");
        testCorruptBlockAllocation(root / "allocation");
        testOptionValidation(root / "options");
        testShutdownStopsSelectingWork(root / "shutdown");
        fs::remove_all(root, error);
        std::puts("SST descriptor, reference metadata, corrupt allocation and option regressions passed");
        return 0;
    } catch (const std::exception& error) {
        allocationLimit = SIZE_MAX;
        std::fprintf(stderr, "SST resource regression failed: %s\n", error.what());
        return 1;
    }
}
