#include "TestErrorHandlers.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"

#include <cstdio>
#include <cstdlib>
#include <new>
#include <stdexcept>
#include <string>

#ifdef _WIN32
#include <malloc.h>
#endif

namespace {
    thread_local int failAlignedAllocationAfter = -1;
}

void* operator new(size_t size, std::align_val_t alignment) {
    if (failAlignedAllocationAfter == 0) {
        failAlignedAllocationAfter = -1;
        throw std::bad_alloc{};
    }
    if (failAlignedAllocationAfter > 0) { --failAlignedAllocationAfter; }
    const auto align = static_cast<size_t>(alignment);
    #ifdef _WIN32
    if (auto* memory = _aligned_malloc(size == 0 ? 1 : size, align)) { return memory; }
    #else
    void* memory = nullptr;
    if (posix_memalign(&memory, align, size == 0 ? 1 : size) == 0) { return memory; }
    #endif
    throw std::bad_alloc{};
}

void operator delete(void* memory, std::align_val_t) noexcept {
    #ifdef _WIN32
    _aligned_free(memory);
    #else
    std::free(memory);
    #endif
}

void operator delete(void* memory, size_t, std::align_val_t alignment) noexcept { ::operator delete(memory, alignment); }

namespace {
    namespace mt = akkaradb::engine::memtable;
    using namespace akkaradb::core;

    void require(bool condition, const char* message) { if (!condition) { throw std::runtime_error(message); } }
    ByteView bytes(std::string_view value) { return {reinterpret_cast<const std::byte*>(value.data()), value.size()}; }

    std::string key(size_t index) {
        char value[32];
        std::snprintf(value, sizeof(value), "key-%08zu", index);
        return value;
    }

    bool exerciseFailure(size_t count, int failAt) {
        // Small arena blocks expose every node/chain reservation to injection.
        mt::BPTreeMemTable tree{64, 64};
        for (size_t i = 0; i < count; ++i) { require(tree.put(bytes(key(i)), bytes("old"), 1, 0).ok(), "initial insertion failed"); }
        bool failed = false;
        failAlignedAllocationAfter = failAt;
        try { require(tree.put(bytes(key(count)), bytes("inserted"), 2, 0).ok(), "split insertion failed"); }
        catch (const std::bad_alloc&) { failed = true; }
        failAlignedAllocationAfter = -1;
        require(tree.entryCount() == count + size_t{!failed}, "failed insertion changed the logical record count");
        RecordView row;
        require(tree.get(bytes(key(count)), UINT64_MAX, &row) == !failed, "failed insertion published a partial record");
        for (size_t i = 0; i < count; ++i) {
            require(tree.get(bytes(key(i)), 1, &row) && row.valueString() == "old", "allocation failure lost an existing key");
        }
        const auto updatedKey = key(count - 3);
        require(tree.put(bytes(updatedKey), bytes("updated"), 3, 0).ok(), "update after allocation failure failed");
        require(tree.get(bytes(updatedKey), 1, &row) && row.valueString() == "old", "update after failed split lost a retained version");
        require(tree.get(bytes(updatedKey), 3, &row) && row.valueString() == "updated", "update after failed split returned an old version");
        if (failed) { require(tree.put(bytes(key(count)), bytes("retried"), 2, 0).ok(), "retry after allocation failure failed"); }
        tree.freeze();
        size_t scanned = 0;
        for (const auto& record : tree.iterator({}, {}, UINT64_MAX)) {
            require(record.keyString() == key(scanned++), "failed split changed ordered scan results");
        }
        require(scanned == count + 1, "failed split lost or duplicated a scan key");
        size_t flushed = 0;
        std::string previous;
        for (const auto& record : tree.flushIterator()) {
            require(previous <= record.keyString(), "failed split produced unsorted SST input");
            previous = record.keyString();
            ++flushed;
        }
        require(flushed == count + 2, "failed split lost or duplicated retained flush records");
        return failed;
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        // Leaf-root split, internal-root split, and internal split below a root.
        constexpr size_t maxKeys = mt::BPTreeMemTable::MAX_KEYS;
        constexpr size_t half = (maxKeys + 1) / 2;
        for (const size_t count : {maxKeys, half * (maxKeys + 2) - 1, half * (maxKeys + 2 + half + 1) - 1}) {
            bool failed = false, succeeded = false;
            for (int failAt = 0; failAt < 16; ++failAt) {
                if (exerciseFailure(count, failAt)) { failed = true; }
                else {
                    succeeded = true;
                    break;
                }
            }
            require(failed && succeeded, "allocation sweep did not reach both failed and successful insertion");
        }
        std::puts("BPTree insertion failure smoke: passed");
        return 0;
    }
    catch (const std::exception& error) {
        failAlignedAllocationAfter = -1;
        std::fprintf(stderr, "BPTree insertion failure: %s\n", error.what());
        return 1;
    }
}
