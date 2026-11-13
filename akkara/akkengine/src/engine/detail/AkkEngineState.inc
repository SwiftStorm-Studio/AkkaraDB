/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineState.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
using BatchPutEntry = detail::BulkPutEntry;

template <typename T>
class StorageSlot {
    public:
        explicit StorageSlot(std::unique_ptr<T>* slot) : slot_{slot} {}
        void bind(std::unique_ptr<T>* slot) noexcept { slot_ = slot; }
        [[nodiscard]] T* get() const noexcept { return slot_ ? slot_->get() : nullptr; }
        [[nodiscard]] explicit operator bool() const noexcept { return get() != nullptr; }
        [[nodiscard]] T* operator->() const noexcept { return get(); }
        [[nodiscard]] T& operator*() const noexcept { return *get(); }
        [[nodiscard]] bool operator==(std::nullptr_t) const noexcept { return get() == nullptr; }
        [[nodiscard]] bool operator!=(std::nullptr_t) const noexcept { return get() != nullptr; }
        void reset() noexcept { slot_->reset(); }

        StorageSlot& operator=(std::unique_ptr<T> value) noexcept {
            *slot_ = std::move(value);
            return *this;
        }

    private:
        std::unique_ptr<T>* slot_;
};

struct StorageState {
    std::unique_ptr<manifest::Manifest> manifest;
    std::unique_ptr<sst::SSTManager> sstManager;
    std::unique_ptr<memtable::MemTable> memtable;
    std::unique_ptr<wal::WalWriter> walWriter;
    std::unique_ptr<blob::BlobManager> blobManager;
    std::unique_ptr<vlog::VersionLog> versionLog;
};

class OperationGuard {
    public:
        explicit OperationGuard(Impl& owner, bool rejectClosed = true)
            : owner_{&owner} {
            if (!owner_->tryBeginOperation()) {
                if (rejectClosed) { throw std::runtime_error("AkkEngine: engine is closed"); }
                return;
            }
            active_ = true;
        }

        ~OperationGuard() {
            if (!active_) { return; }
            owner_->endOperation();
        }

        OperationGuard(const OperationGuard&) = delete;
        OperationGuard& operator=(const OperationGuard&) = delete;

        [[nodiscard]] explicit operator bool() const noexcept { return active_; }

    private:
        Impl* owner_;
        bool active_{false};
};

explicit Impl(AkkEngineOptions optionsIn)
    : opts{std::move(optionsIn)},
      writeBackpressureEnabled{opts.runtime.backpressure.maxMemtableImmutableTables != 0 || opts.runtime.backpressure.maxSstL0Files != 0},
      commitWindowSize{resolveCommitWindowSize(opts.runtime.sequence.commitWindowSize)},
      storage{std::make_shared<StorageState>()},
      manifest{&storage->manifest},
      sstManager{&storage->sstManager},
      memtable{&storage->memtable},
      walWriter{&storage->walWriter},
      blobManager{&storage->blobManager},
      versionLog{&storage->versionLog},
      keySequenceOrderMu{
          opts.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE
              ? std::make_unique<std::array<std::mutex, KEY_SEQUENCE_ORDER_STRIPES>>()
              : nullptr
      } { if (opts.components.clusterEnabled) { cluster::detail::ensureNodeResourceBudget(opts.cluster.runtime); } }

static constexpr size_t KEY_SEQUENCE_ORDER_STRIPES = 256;

AkkEngineOptions opts;
const bool writeBackpressureEnabled;
const uint32_t commitWindowSize;
std::atomic<bool> closed{false};
uint64_t nodeId = 0;

std::shared_ptr<StorageState> storage;
StorageSlot<manifest::Manifest> manifest;
StorageSlot<sst::SSTManager> sstManager;
StorageSlot<memtable::MemTable> memtable;
StorageSlot<wal::WalWriter> walWriter;
StorageSlot<blob::BlobManager> blobManager;
StorageSlot<vlog::VersionLog> versionLog;
std::unique_ptr<cluster::IClusterRuntime> clusterRuntime;
std::unique_ptr<server::IAkkApiServer> apiServer;
cluster::ClusterConfig clusterConfig;
cluster::ReplicationMode clusterReplicationMode = cluster::ReplicationMode::STANDALONE;
uint64_t clusterConfiguredNodeCount = 0;
cluster::AckTimeoutAction primaryAckTimeoutAction = cluster::AckTimeoutAction::ACCEPT_LOCAL;
cluster::detail::KeyedMutex stripeKeys;
mutable std::mutex stripeStateMu;
std::shared_mutex stripeGcEpochMu;
std::shared_ptr<std::atomic<uint64_t>> stripeQueryPins = std::make_shared<std::atomic<uint64_t>>(0);
mutable std::mutex stripeRepairStatsMu;
EngineStats::StripeReadRepairStats stripeRepairStats;
mutable std::mutex stripeRebuildStatsMu;
EngineStats::StripeRebuildStats stripeRebuildStats;
std::exception_ptr stripeFailure;
std::jthread stripeGcThread;
std::map<std::vector<uint8_t>, std::vector<uint8_t>> stripePending;
bool stripeGcLoaded = false;
std::vector<uint8_t> stripeRebuildCursor;
std::unique_ptr<cluster::detail::BoundedExecutor> stripeExecutor;

struct DeferredRollbackTask {
    std::array<uint8_t, 16> taskId{};
    std::array<uint8_t, 16> operationId{};
    std::array<uint8_t, 16> createdStartupId{};
    std::array<uint8_t, 16> clusterId{};
    uint64_t configEpoch = 0;
    cluster::StripeControlAction action = cluster::StripeControlAction::ROLLBACK_KEY;
    bool clusterTask = false;
    uint64_t targetNodeId = 0;
    uint64_t targetSeq = 0;
    uint64_t plannedWatermark = 0;
    RollbackConflictPolicy conflict = RollbackConflictPolicy::FAIL_IF_CHANGED;
    std::vector<uint8_t> key;
};

std::array<uint8_t, 16> rollbackStartupId{};
mutable std::mutex rollbackJournalMu;
std::vector<DeferredRollbackTask> deferredRollbacks;
std::jthread rollbackRecoveryThread;

mutable std::mutex blobGcPlanMu;
mutable std::mutex writeMu;
std::mutex sstCheckpointMu;
uint64_t sstCheckpointSeq = 0;
std::mutex partitionSnapshotMu;
mutable std::shared_mutex mutationEpochMu;
std::unique_ptr<std::array<std::mutex, KEY_SEQUENCE_ORDER_STRIPES>> keySequenceOrderMu;
mutable std::shared_mutex storageMu;
mutable std::mutex lifecycleMu;
std::condition_variable lifecycleCv;
static constexpr uint64_t LIFECYCLE_CLOSED = 1ULL << 63u;
static constexpr uint64_t LIFECYCLE_OPERATION_MASK = ~LIFECYCLE_CLOSED;
std::atomic<uint64_t> lifecycleState{0};
bool closing{false};
std::atomic<uint64_t> putsTotal{0};
std::atomic<uint64_t> removesTotal{0};
std::atomic<uint64_t> getsTotal{0};
std::atomic<uint64_t> getsMemtableHit{0};
std::atomic<uint64_t> getsSstHit{0};
std::atomic<uint64_t> getsMiss{0};
std::atomic<uint64_t> existsTotal{0};
std::atomic<uint64_t> scansTotal{0};
std::atomic<uint64_t> blobPutsTotal{0};
mutable std::atomic<uint64_t> backpressureBlockedWrites{0};
mutable std::atomic<uint64_t> backpressureRejectedWrites{0};
mutable std::atomic<uint64_t> backpressureTimedOutWrites{0};
mutable std::atomic<uint64_t> backpressureMemtableStalls{0};
mutable std::atomic<uint64_t> backpressureSstStalls{0};
mutable std::atomic<uint64_t> backpressureWaitMicrosTotal{0};
mutable std::atomic<uint64_t> backpressureWaitMicrosMax{0};
std::atomic<uint64_t> committedSeq{0};
// Static PARTITIONED replication excludes allocator gaps and read visibility.
// Writers hold writeMu; peer handshakes sample the stored cut atomically.
std::atomic<uint64_t> replicationStorageSeq{0};
mutable std::mutex commitMu;
mutable std::condition_variable commitCv;
// A failed write after it reserved a sequence leaves a permanent
// prefix gap. KEY_SEQUENCE reads must fail instead of waiting for
// that gap forever.
std::exception_ptr keySequenceFailure;
std::vector<uint64_t> completedSeqRing;
std::unordered_set<uint64_t> completedSeqOverflow;

bool snapshotInProgress = false;
uint64_t pendingSnapshotSeq = 0;
uint64_t pendingSnapshotEntryCount = 0;
uint64_t pendingSnapshotAppliedEntries = 0;
std::unordered_set<std::string> pendingSnapshotKeys;
std::ofstream pendingSnapshotOut;
bool pendingSnapshotEntryInProgress = false;
uint64_t pendingSnapshotEntryValueSize = 0;
uint64_t pendingSnapshotEntryValueOffset = 0;
uint32_t pendingSnapshotEntryValueCrc32c = 0;
Crc32cStream pendingSnapshotEntryRecordCrc;
Crc32cStream pendingSnapshotEntryValueCrc;
uint64_t durableReplicaSnapshotSeq = 0;
std::weak_ptr<ClusterSnapshotExportFile> clusterSnapshotExportCache;

struct AppliedWrite {
    uint64_t seq = 0;
    std::span<const uint8_t> key;
    std::vector<uint8_t> stored;
    uint8_t flags = MemHdr16::FLAG_NORMAL;
    cluster::ReplOpType op = cluster::ReplOpType::PUT;
    uint64_t sourceNodeId = 0;
};
