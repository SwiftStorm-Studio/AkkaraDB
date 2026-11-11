#include "TestErrorHandlers.hpp"
#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"

#include <cstdio>
#include <filesystem>
#include <string>
#include <thread>

namespace {
    namespace mt = akkaradb::engine::memtable;
    namespace fs = std::filesystem;
    using Options = akkaradb::engine::AkkEngineOptions;
    void require(bool condition, const char* message) { if (!condition) { throw std::runtime_error(message); } }
    std::span<const uint8_t> bytes(std::string_view value) { return {reinterpret_cast<const uint8_t*>(value.data()), value.size()}; }

    void run(const fs::path& root, int backend, bool streaming, bool persistent) {
        Options options;
        options.paths.dataDir = root;
        options.components.blobEnabled = false;
        options.components.walEnabled = false;
        options.components.versionLogEnabled = false;
        options.components.sstEnabled = persistent;
        options.components.manifestEnabled = persistent;
        options.memtable.shardCount = 1;
        options.memtable.flushMode = mt::MemTableFlushMode::MANUAL_ONLY;
        options.memtable.flushInputMode = streaming ? mt::MemTableFlushInputMode::STREAMING : mt::MemTableFlushInputMode::MATERIALIZE_VECTOR;
        options.memtable.backendFactory = [backend]() -> std::unique_ptr<mt::IMemTable> {
            if (backend == 0) { return std::make_unique<mt::SkipListMemTable>(); }
            if (backend == 1) { return std::make_unique<mt::BPTreeMemTable>(); }
            return std::make_unique<mt::ARTMemTable>();
        };
        options.runtime.writeAdmission = Options::WriteAdmissionMode::PARALLEL;
        options.runtime.writeVisibility = Options::WriteVisibilityMode::APPLIED;
        options.runtime.sequence.allocation = Options::SequenceAllocationMode::THREAD_LOCAL_RANGES;
        options.runtime.sequence.threadLocalRangeSize = 2;
        auto engine = akkaradb::engine::AkkEngine::open(options);
        // Main thread reserves [1,2], the second writer reserves [3,4].
        engine->put(bytes("seed"), bytes("seed"));
        std::exception_ptr failure;
        std::jthread writer{[&] {
            try { engine->put(bytes("key"), bytes("new")); }
            catch (...) { failure = std::current_exception(); }
        }};
        writer.join();
        if (failure) { std::rethrow_exception(failure); }
        engine->put(bytes("key"), bytes("old"));
        const std::vector<uint8_t> expected = persistent ? std::vector<uint8_t>{'n','e','w'} : std::vector<uint8_t>{'o','l','d'};
        const auto verify = [&] {
            require(engine->get(bytes("key")) == std::optional{expected}, "point lookup used inconsistent version precedence");
            std::vector<uint8_t> out;
            require(engine->getInto(bytes("key"), out) && out == expected, "getInto disagreed with point lookup");
            akkaradb::core::BufferArena arena;
            size_t count = 0;
            for (const auto& row : engine->scan(arena)) {
                if (std::ranges::equal(row.key, bytes("key"))) {
                    require(std::ranges::equal(row.value, expected), "scan disagreed with point lookup");
                    ++count;
                }
            }
            require(count == 1, "scan lost or duplicated the sequence-ordered key");
        };
        verify();
        engine->forceFlush();
        verify();
        engine->close();
        if (persistent) {
            engine = akkaradb::engine::AkkEngine::open(options);
            verify();
            engine->close();
        }
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        const auto root = fs::current_path() / ".bench-tmp" / "sst-sequence-order";
        fs::remove_all(root);
        for (int backend = 0; backend < 3; ++backend) {
            for (const bool streaming : {false, true}) {
                run(root / (std::to_string(backend) + (streaming ? "-stream" : "-vector")), backend, streaming, true);
            }
            run(root / ("memory-" + std::to_string(backend)), backend, false, false);
        }
        fs::remove_all(root);
        return 0;
    }
    catch (const std::exception& error) { std::fprintf(stderr, "SST sequence order: %s\n", error.what()); return 1; }
}
