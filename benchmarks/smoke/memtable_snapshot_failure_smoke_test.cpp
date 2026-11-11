#include "TestErrorHandlers.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"

#include <cstdlib>
#include <cstdio>
#include <mutex>
#include <new>
#include <string>
#include <vector>

namespace {
    thread_local int failAllocationAfter = -1;
    thread_local bool failAfterFreeze = false;
}

void* operator new(size_t size) {
    if (failAllocationAfter == 0) { failAllocationAfter = -1; throw std::bad_alloc{}; }
    if (failAllocationAfter > 0) { --failAllocationAfter; }
    if (auto* memory = std::malloc(size == 0 ? 1 : size)) { return memory; }
    throw std::bad_alloc{};
}
void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, size_t) noexcept { std::free(memory); }

namespace {
    namespace mt = akkaradb::engine::memtable;
    using namespace akkaradb::core;

    void require(bool condition, const char* message) { if (!condition) { throw std::runtime_error(message); } }
    std::span<const uint8_t> bytes(std::string_view value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    class FreezeBackend final : public mt::IMemTable {
        std::unique_ptr<mt::IMemTable> inner_;
    public:
        explicit FreezeBackend(int backend) {
            if (backend == 0) { inner_ = std::make_unique<mt::SkipListMemTable>(); }
            else if (backend == 1) { inner_ = std::make_unique<mt::BPTreeMemTable>(); }
            else { inner_ = std::make_unique<mt::ARTMemTable>(); }
        }
        Status put(ByteView key, ByteView value, uint64_t seq, uint8_t flags, uint64_t fp, uint64_t mini) override {
            return inner_->put(key, value, seq, flags, fp, mini);
        }
        bool get(ByteView key, uint64_t seq, RecordView* out) const override { return inner_->get(key, seq, out); }
        ArenaGenerator<RecordView> iterator(ByteView start, ByteView end, uint64_t seq) const override { return inner_->iterator(start, end, seq); }
        ArenaGenerator<RecordView> flushIterator() const override { return inner_->flushIterator(); }
        void freeze() override {
            inner_->freeze();
            if (failAfterFreeze) { failAfterFreeze = false; failAllocationAfter = 0; }
        }
        size_t sizeBytes() const override { return inner_->sizeBytes(); }
        size_t entryCount() const override { return inner_->entryCount(); }
    };

    bool exerciseFailure(int backend, bool memoryOnly, int failAt, bool afterFreeze) {
        mt::MemTable::Options options;
        options.shardCount = 2;
        options.flushMode = mt::MemTableFlushMode::MANUAL_ONLY;
        options.backendFactory = [backend] { return std::make_unique<FreezeBackend>(backend); };
        std::mutex persistedMutex;
        size_t persistedOriginals = 0;
        if (!memoryOnly) {
            options.onFlush = [&](auto records) {
                std::lock_guard lock{persistedMutex};
                for (const auto& row : records) { if (row.seq() == 1) { ++persistedOriginals; } }
            };
        }
        auto memory = mt::MemTable::create(options);
        std::vector<std::string> keys;
        for (int i = 0; i < 16; ++i) {
            keys.push_back("key-" + std::to_string(i));
            memory->put(bytes(keys.back()), bytes("old"), 1);
        }
        bool failed = false;
        failAllocationAfter = failAt;
        failAfterFreeze = afterFreeze;
        try {
            if (memoryOnly) { auto snapshot = memory->sealAndPinMemoryIterator({}, 1, false, 0, 1); require(snapshot.has_value(), "snapshot admission leaked"); }
            else { auto snapshot = memory->sealAndPinIterator({}, 1); }
        }
        catch (const std::bad_alloc&) { failed = true; }
        failAllocationAfter = -1;
        failAfterFreeze = false;
        memory->drainFlushes();
        for (const auto& key : keys) {
            memory->put(bytes(key), bytes("new"), 2);
            const auto row = memory->getPinned(bytes(key), 2);
            require(row && row->view.valueString() == "new", "snapshot failure left an active generation frozen");
        }
        if (memoryOnly) {
            auto retry = memory->sealAndPinMemoryIterator({}, 2, true, 0, 1);
            require(retry.has_value(), "failed snapshot leaked its lease capacity");
        }
        else {
            memory->forceFlush();
            if (persistedOriginals != keys.size()) {
                std::fprintf(stderr, "backend=%d memory=%d failAt=%d afterFreeze=%d originals=%zu expected=%zu\n",
                    backend, memoryOnly, failAt, afterFreeze, persistedOriginals, keys.size());
                throw std::runtime_error("partial snapshot failure lost or duplicated flush work");
            }
        }
        return failed;
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        for (int backend = 0; backend < 3; ++backend) {
            for (const bool memoryOnly : {false, true}) {
                require(exerciseFailure(backend, memoryOnly, -1, true), "post-freeze allocation failure was not exercised");
                bool reachedSuccess = false;
                for (int failAt = 0; failAt < 128; ++failAt) {
                    if (!exerciseFailure(backend, memoryOnly, failAt, false)) { reachedSuccess = true; break; }
                }
                require(reachedSuccess, "allocation failure sweep never reached a successful snapshot");
            }
        }
        return 0;
    }
    catch (const std::exception& error) {
        failAllocationAfter = -1;
        failAfterFreeze = false;
        std::fprintf(stderr, "MemTable snapshot failure: %s\n", error.what());
        return 1;
    }
}
