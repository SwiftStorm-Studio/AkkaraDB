/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/cluster/RaftConsensusRuntime.cpp
#include <akk/engine/cluster/detail/ReconfigurationDeadline.hpp>
#include "akk/engine/cluster/detail/NodeResourceBudget.hpp"
#include "akk/engine/cluster/detail/DiskResults.hpp"
#include "akk/engine/cluster/detail/TransactionProtocol.hpp"
#include "akk/engine/cluster/detail/RaftConsensusRuntime.hpp"
#include "akk/engine/cluster/detail/FaultDomains.hpp"
#include "akk/engine/cluster/detail/RaftLogIndex.hpp"
#include "akk/engine/cluster/detail/ForwardDeadline.hpp"
#include "akk/engine/cluster/detail/ClusterConnection.hpp"
#include "akk/core/record/MemHdr16.hpp"
#include "akk/cpu/CRC32C.hpp"
#include "akk/crypto/Random.hpp"
#include "akk/crypto/SecureChannel.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <deque>
#include <future>
#include <functional>
#include <limits>
#include <mutex>
#include <map>
#include <optional>
#include <random>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include <utility>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <fcntl.h>
#include <io.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster {
    namespace {
        #include "detail/RaftSupport.inc"
        #include "detail/RaftTransport.inc"
        #include "detail/RaftProtocol.inc"
    }

    class RaftConsensusRuntime::Impl {
        private:
            struct PendingSnapshotInstall {
                // Reads hold applyMutex_ or mutex_; writes hold both, in that order.
                bool active = false;
                uint64_t term = 0;
                uint64_t snapshotSeq = 0;
                uint64_t lastIncludedIndex = 0;
                uint64_t lastIncludedTerm = 0;
                uint64_t entryCount = 0;
                uint64_t configurationGeneration = 0;
                std::vector<NodeInfo> committedMembers;
                std::optional<std::vector<NodeInfo>> jointOldMembers;
                std::optional<std::vector<NodeInfo>> jointNewMembers;
                uint64_t nextEntryIndex = 0;
                bool hasCurrentEntry = false;
                uint64_t currentEntryIndex = 0;
                uint64_t currentValueSize = 0;
                uint64_t currentValueOffset = 0;
                uint32_t currentValueCrc32c = 0;
                std::vector<uint8_t> currentKey;
                Crc32cStream currentValueCrc;
                std::optional<detail::DiskResults::Import> results;
                uint64_t resultFloor = 0, resultSourceGeneration = 0;
            };

            struct SnapshotInstallTransaction {
                RequestState requestState;
                uint64_t snapshotSeq = 0;
                uint64_t lastIncludedIndex = 0;
                uint64_t lastIncludedTerm = 0;
                uint64_t entryCount = 0;
                uint64_t configurationGeneration = 0;
                std::vector<NodeInfo> committedMembers;
                std::optional<std::vector<NodeInfo>> jointOldMembers;
                std::optional<std::vector<NodeInfo>> jointNewMembers;
            };

            struct DurableLogState {
                RequestState requestState;
                RequestState journalState;
                uint64_t requestJournalGeneration = 0;
                uint64_t requestJournalBytes = 0;
                uint64_t requestJournalRecords = 0;
                uint64_t commitIndex = 0;
                uint64_t lastApplied = 0;
                uint64_t lastIncludedIndex = 0;
                uint64_t lastIncludedTerm = 0;
                uint64_t lastIncludedStateMachineSeq = 0;
                uint64_t configurationGeneration = 0;
                size_t logPrefixSize = 0;
                bool learnerBootstrapPending = false;
                std::vector<RaftLogEntry> log;
                std::vector<NodeInfo> committedMembers;
                std::optional<std::vector<NodeInfo>> jointOldMembers;
                std::optional<std::vector<NodeInfo>> jointNewMembers;
                std::vector<NodeInfo> peers;
                std::vector<PeerReplicationState> peerReplication;
                std::optional<SnapshotInstallTransaction> durableSnapshotInstall;
            };

            struct DurableHardState {
                uint64_t currentTerm = 0;
                uint64_t votedFor = 0;
            };

            RequestState requestState_;
            std::shared_ptr<detail::DiskResults> resultStore_;
            std::timed_mutex mutationAdmissionMutex_;
            std::filesystem::path dbDir_;
            std::filesystem::path statePath_;
            std::filesystem::path logPath_;
            std::filesystem::path requestJournalBasePath_;
            ClusterConfig config_;
            ClusterRouter router_;
            uint64_t selfNodeId_ = 0;
            const NodeInfo* self_ = nullptr;
            bool learnerBootstrapPending_ = false;
            std::vector<NodeInfo> peers_;
            ClusterEngineCallbacks callbacks_;
            ClusterRuntimeOptions runtimeOptions_;
            std::shared_ptr<RaftReceiveBudget> receiveBudget_;
            std::shared_ptr<RaftReceiveBudget> forwardReceiveBudget_;
            std::shared_ptr<RaftReceiveBudget> controlReceiveBudget_;
            std::shared_ptr<RaftReceiveBudget> handshakeReceiveBudget_;
            std::array<uint8_t, 32> groupIdentity_{};
            std::array<uint8_t, 32> transactionApplyFingerprint_{};
            std::array<uint8_t, 32> compatibilityFingerprint_{};
            crypto::NodeIdentity localIdentity_{};

            mutable std::mutex mutex_;
            std::recursive_mutex lifecycleMutex_;
            std::condition_variable cv_;
            bool running_ = false;
            bool requestClientOnly_ = false;
            std::optional<NodeInfo> requestClientSelf_;
            std::vector<uint64_t> requestForwardClients_;
            uint64_t currentTerm_ = 0;
            uint64_t votedFor_ = 0;
            uint64_t commitIndex_ = 0;
            uint64_t lastApplied_ = 0;
            uint64_t lastIncludedIndex_ = 0;
            uint64_t lastIncludedTerm_ = 0;
            uint64_t lastIncludedStateMachineSeq_ = 0;
            uint64_t lastAssignedStateMachineSeq_ = 0;
            std::vector<RaftLogEntry> log_;
            using LogLocation = detail::RaftLogIndex::Location;
            using LogExtent = detail::RaftLogIndex::Extent;
            detail::RaftLogIndex persistedLogLocations_;
            uint64_t changedLogFrom_ = UINT64_MAX;

            struct SequenceState {
                uint64_t sequence;
                bool hasMutation;
            };

            std::vector<SequenceState> validatedLog_;
            uint64_t nextLogSegment_ = 1;
            RequestState journalState_;
            uint64_t requestJournalGeneration_ = 0;
            uint64_t requestJournalBytes_ = 0;
            uint64_t requestJournalRecords_ = 0;
            bool logIoFailed_ = false;
            static constexpr uint64_t LOG_SEGMENT_BYTES = 16ull * 1024ull * 1024ull;
            static constexpr uint64_t REQUEST_JOURNAL_COMPACT_BYTES = 1024ull * 1024ull;
            std::vector<NodeInfo> committedMembers_;
            uint64_t lastConfigurationGeneration_ = 0;
            std::optional<std::vector<NodeInfo>> jointOldMembers_;
            std::optional<std::vector<NodeInfo>> jointNewMembers_;
            std::vector<PeerReplicationState> peerReplication_;
            std::condition_variable peerReplicationCv_;
            Clock::time_point electionDeadline_{};
            std::optional<Clock::time_point> lastLeaderContact_;
            uint64_t leaderContactGeneration_ = 0;
            bool forceElection_ = false;
            std::atomic<RaftRole> role_{RaftRole::FOLLOWER};
            std::mutex applyMutex_;
            std::atomic<bool> applyFailed_{false};
            // Staging fields are protected by applyMutex_; active is also read by consensus.
            PendingSnapshotInstall pendingSnapshotInstall_;
            std::optional<SnapshotInstallTransaction> durableSnapshotInstall_;
            uint64_t observedLeaderTerm_ = 0;
            uint64_t observedLeaderId_ = 0;
            Clock::time_point nextCompactionCheck_{};
            Clock::time_point nextCompactionDeadline_{};

            SocketHandle listenSock_ = BAD_SOCKET;
            std::thread acceptThread_;
            std::thread timerThread_;
            std::thread snapshotThread_;
            std::atomic<size_t> activeClientHandlers_{0};
            std::atomic<size_t> pendingClientHandlers_{0};
            mutable std::mutex clientHandlersMutex_;
            std::condition_variable clientHandlersCv_;
            std::unordered_set<SocketHandle> clientSockets_;
            std::unordered_set<uint64_t> incomingConsensusPeers_;
            size_t incomingForwardChannels_ = 0, incomingControlChannels_ = 0;

            struct IncomingChannel {
                Impl& owner;
                std::optional<RaftChannelKind> kind;
                uint64_t peer = 0;
                std::shared_ptr<void> nodeLease;
                std::shared_ptr<void> handshakeLease;
                explicit IncomingChannel(Impl& runtime) : owner{runtime} {}

                ~IncomingChannel() {
                    std::lock_guard lock{owner.clientHandlersMutex_};
                    if (!kind) { owner.pendingClientHandlers_.fetch_sub(1, std::memory_order_acq_rel); }
                    else if (*kind == RaftChannelKind::CONSENSUS) { owner.incomingConsensusPeers_.erase(peer); }
                    else if (*kind == RaftChannelKind::FORWARD) { --owner.incomingForwardChannels_; }
                    else { --owner.incomingControlChannels_; }
                }

                bool admit(const RaftPeerHello& hello) {
                    std::lock_guard lock{owner.clientHandlersMutex_};
                    if (hello.channelKind == RaftChannelKind::CONSENSUS) { if (!owner.incomingConsensusPeers_.insert(hello.nodeId).second) { return false; } }
                    else if (hello.channelKind == RaftChannelKind::FORWARD) {
                        if (owner.incomingForwardChannels_ >= owner.runtimeOptions_.raftMaxIncomingForwardRequests) { return false; }
                        nodeLease = owner.runtimeOptions_.nodeResourceBudget->incomingForward->tryReserve(1);
                        if (!nodeLease) { return false; }
                        ++owner.incomingForwardChannels_;
                    }
                    else {
                        if (owner.incomingControlChannels_ >= MAX_RAFT_CONTROL_CHANNELS) { return false; }
                        nodeLease = owner.runtimeOptions_.nodeResourceBudget->incomingControl->tryReserve(1);
                        if (!nodeLease) { return false; }
                        ++owner.incomingControlChannels_;
                    }
                    kind = hello.channelKind;
                    peer = hello.nodeId;
                    owner.pendingClientHandlers_.fetch_sub(1, std::memory_order_acq_rel);
                    handshakeLease.reset();
                    return true;
                }
            };

            struct PeerWorker {
                std::mutex mutex;
                std::condition_variable cv;
                bool stopping = false;
                bool replicate = false;
                bool heartbeat = false;
                uint64_t target = 0;
                uint64_t readRound = 0;
                std::deque<std::function<void()>> tasks;
                std::thread thread;
                std::timed_mutex ioMutex;
                std::mutex socketMutex;
                SocketHandle socket = BAD_SOCKET;
                NodeInfo endpoint;
                std::unique_ptr<crypto::SecureSession> secure;
                std::atomic<uint64_t> lastSuccessfulContactAtUs{0};
                std::atomic<uint64_t> roundTripsSucceeded{0};
                std::atomic<uint64_t> roundTripsFailed{0};
                std::atomic<uint64_t> consecutiveRoundTripFailures{0};
                std::atomic<uint64_t> lastRoundTripAtUs{0};
                std::atomic<uint64_t> lastRoundTripFailureAtUs{0};
                AtomicLatencyHistogram roundTripLatencyUs;
                ~PeerWorker() { closeSocket(socket); }
            };

            struct PeerRoundTripObservation {
                std::shared_ptr<PeerWorker> worker;
                Clock::time_point started = Clock::now();
                bool succeeded = false;

                ~PeerRoundTripObservation() {
                    const auto nowUs = observationNowUs();
                    worker->lastRoundTripAtUs.store(nowUs, std::memory_order_relaxed);
                    if (succeeded) {
                        ++worker->roundTripsSucceeded;
                        worker->consecutiveRoundTripFailures.store(0, std::memory_order_relaxed);
                        const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - started).count();
                        worker->roundTripLatencyUs.observe(static_cast<uint64_t>(std::max<int64_t>(0, elapsed)));
                    }
                    else {
                        ++worker->roundTripsFailed;
                        ++worker->consecutiveRoundTripFailures;
                        worker->lastRoundTripFailureAtUs.store(nowUs, std::memory_order_relaxed);
                    }
                }
            };

            mutable std::mutex workersMutex_;
            std::atomic<uint64_t> outboundConnections_{0}, peerWorkersStarted_{0}, proposalBatches_{0};
            std::atomic<uint64_t> peerPolicyMismatchRejects_{0}, foreignClusterRejects_{0}, endpointStartFailures_{0};
            std::atomic<uint64_t> expiredRequests_{0}, rejectedRequests_{0};
            std::atomic<uint64_t> requestJournalBytesWritten_{0}, requestJournalCompactions_{0};
            std::atomic<ClusterHealthState> health_{ClusterHealthState::HEALTHY};
            std::atomic<ClusterFailureCode> lastFailure_{ClusterFailureCode::NONE};
            std::atomic<uint64_t> lastFailureAtUs_{0};
            std::atomic<uint64_t> runtimeStartedAtUs_{0};
            bool workersStopping_ = false;
            std::unordered_map<uint64_t, std::shared_ptr<PeerWorker>> workers_;
            std::vector<std::weak_ptr<PeerWorker>> forwardChannels_;
            std::vector<std::weak_ptr<PeerWorker>> placementReadChannels_;
            std::condition_variable forwardChannelsCv_;
            uint64_t nextReadRound_ = 0;
            std::unordered_map<uint64_t, std::pair<uint64_t, uint64_t>> readAcks_;

            void recordFailure(ClusterFailureCode failure, ClusterHealthState health) noexcept {
                lastFailureAtUs_.store(observationNowUs(), std::memory_order_relaxed);
                lastFailure_.store(failure, std::memory_order_relaxed);
                if (health == ClusterHealthState::FAILED || health_.load(std::memory_order_relaxed) != ClusterHealthState::FAILED) {
                    health_.store(health, std::memory_order_relaxed);
                }
            }

            struct RequestCompletion {
                ClusterRequestId id;
                std::array<uint8_t, 32> fingerprint{};
                uint64_t sequence = 0;
                std::promise<ClusterRequestResult> promise;
                std::shared_future<ClusterRequestResult> future = promise.get_future().share();
            };

            struct Proposal {
                std::shared_ptr<RequestCompletion> request;
                uint64_t entryTerm = 0;
                std::vector<RaftLogEntry> entries;
                uint64_t term = 0;
                uint64_t index = 0;
                bool localDurable = false;
                Clock::time_point deadline;
                std::promise<void> completion;
                size_t bytes = 0;
                std::exception_ptr error;
            };

            std::deque<std::shared_ptr<Proposal>> proposalQueue_;
            std::vector<std::shared_ptr<Proposal>> proposals_;
            std::thread proposalThread_;
            bool administrativeChange_ = false;
            std::timed_mutex administrationMutex_;

            bool isValidStateMachineEntry(const RaftLogEntry& entry) const {
                if (!isValidRaftLogEntryShape(entry)) { return false; }
                if (runtimeOptions_.raftGroupKind == RaftGroupKind::PLACEMENT && callbacks_.reclaimStripeGeneration &&
                    entry.kind == RaftEntryKind::MUTATION && entry.op == ReplOpType::PUT && !entry.key.empty() &&
                    (entry.key[0] == 'M' || entry.key[0] == 'H')) {
                    // Authority keys omit the local namespace and, for history
                    // aliases, the two sequence fields. Validate the applied record.
                    const size_t overhead = entry.key[0] == 'M' ? 5 : 21;
                    return entry.key.size() <= UINT16_MAX - overhead && entry.value.size() <= UINT16_MAX - overhead - entry.key.size();
                }
                if (runtimeOptions_.raftGroupKind != RaftGroupKind::DATA && runtimeOptions_.raftGroupKind != RaftGroupKind::PARTITION_DATA) {
                    return true;
                }
                if (entry.kind == RaftEntryKind::BLOB) {
                    return runtimeOptions_.raftBlobPolicy == RaftBlobPolicy::RAFT_LOG && hasRaftBlobStorage(callbacks_);
                }
                if (entry.kind != RaftEntryKind::MUTATION || entry.op == ReplOpType::TRANSACTION) { return true; }
                return entry.key.size() <= UINT16_MAX && entry.value.size() <= UINT16_MAX - entry.key.size() &&
                    ((entry.flags & core::MemHdr16::FLAG_BLOB) == 0 || runtimeOptions_.raftBlobPolicy != RaftBlobPolicy::RAFT_LOG || hasRaftBlobStorage(callbacks_));
            }

            #include "detail/RaftPeerTransport.inc"
            #include "detail/RaftProposals.inc"
            #include "detail/RaftMembership.inc"
            #include "detail/RaftLog.inc"
            #include "detail/RaftPersistence.inc"
            #include "detail/RaftElection.inc"
            #include "detail/RaftSnapshots.inc"
            #include "detail/RaftReplication.inc"
            #include "detail/RaftServer.inc"
            #include "detail/RaftApplication.inc"

        public:
            Impl(std::filesystem::path dbDir, ClusterConfig config, uint64_t selfNodeId, ClusterEngineCallbacks callbacks, ClusterRuntimeOptions runtimeOptions,
                 std::vector<uint64_t> forwardClients = {}, std::optional<NodeInfo> clientSelf = {})
                : dbDir_{std::move(dbDir)},
                  statePath_{dbDir_.empty() ? std::filesystem::path{"cluster-raft.state"} : dbDir_ / "cluster-raft.state"},
                  logPath_{dbDir_.empty() ? std::filesystem::path{"cluster-raft.log"} : dbDir_ / "cluster-raft.log"},
                  requestJournalBasePath_{dbDir_.empty() ? std::filesystem::path{"cluster-raft.requests"} : dbDir_ / "cluster-raft.requests"},
                  config_{std::move(config)},
                  router_{config_},
                  selfNodeId_{selfNodeId},
                  callbacks_{std::move(callbacks)},
                  runtimeOptions_{
                      [&] {
                          detail::ensureNodeResourceBudget(runtimeOptions);
                          return std::move(runtimeOptions);
                      }()
                  },
                  receiveBudget_{
                      std::make_shared<RaftReceiveBudget>(runtimeOptions_.raftMaxReceiveMemoryBytes, runtimeOptions_.nodeResourceBudget->raftReceive)
                  },
                  forwardReceiveBudget_{
                      std::make_shared<RaftReceiveBudget>(runtimeOptions_.raftMaxForwardReceiveMemoryBytes, runtimeOptions_.nodeResourceBudget->forwardReceive)
                  },
                  controlReceiveBudget_{std::make_shared<RaftReceiveBudget>(MIN_RAFT_RECEIVE_BYTES, runtimeOptions_.nodeResourceBudget->controlReceive)},
                  handshakeReceiveBudget_{std::make_shared<RaftReceiveBudget>(64 * 1024, runtimeOptions_.nodeResourceBudget->handshakeReceive)} {
                requestClientSelf_ = std::move(clientSelf); requestClientOnly_ = requestClientSelf_.has_value();
                requestForwardClients_ = std::move(forwardClients);
                if (requestClientOnly_ && runtimeOptions_.raftGroupKind != RaftGroupKind::REQUEST_AUTHORITY) { throw std::invalid_argument("Raft: forwarding client requires request authority"); }
                config_.validate();
                if (runtimeOptions_.raftGroupKind > RaftGroupKind::REQUEST_AUTHORITY || (runtimeOptions_.raftGroupKind != RaftGroupKind::PARTITION_DATA &&
                        runtimeOptions_.raftPartitionIndex != 0) || runtimeOptions_.raftMaxIncomingForwardRequests == 0 || runtimeOptions_.
                    raftMaxIncomingForwardRequests > 256 || runtimeOptions_.raftMaxForwardReceiveMemoryBytes < MIN_RAFT_RECEIVE_BYTES || runtimeOptions_.
                    raftMaxForwardReceiveMemoryBytes > 64ull * 1024 * 1024 * 1024) {
                    throw std::invalid_argument("RaftConsensusRuntime: invalid group or forwarding receive limits");
                }
                groupIdentity_ = raftGroupIdentity(config_, runtimeOptions_);
                transactionApplyFingerprint_ = raftTransactionApplyFingerprint(runtimeOptions_, callbacks_);
                compatibilityFingerprint_ = raftCompatibilityFingerprint(config_, runtimeOptions_, callbacks_);
                if (runtimeOptions_.transactions.mode != ClusterTransactionMode::DISABLED && (callbacks_.transactionMaxJournalBytes < 1024 || callbacks_.
                    transactionMaxJournalBytes > 1024ull * 1024 * 1024 * 1024)) {
                    throw std::invalid_argument("RaftConsensusRuntime: transaction apply journal limit is required");
                }
                if (runtimeOptions_.raftBlobChunkSizeBytes == 0 || runtimeOptions_.raftBlobChunkSizeBytes > maxRaftBlobChunkSizeBytes()) {
                    throw std::invalid_argument("RaftConsensusRuntime: invalid Raft Blob chunk size");
                }
                if (runtimeOptions_.raftHeartbeatIntervalMs < 10 || runtimeOptions_.raftHeartbeatIntervalMs > 1'000) {
                    throw std::invalid_argument("RaftConsensusRuntime: Raft heartbeat interval must be in [10, 1000] ms");
                }
                constexpr uint64_t minReceiveMemoryBytes = 3ull * (ReplFrameHeader::SIZE + ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE);
                if (runtimeOptions_.raftMaxReceiveMemoryBytes < minReceiveMemoryBytes || runtimeOptions_.raftMaxReceiveMemoryBytes > 64ull * 1024 * 1024 *
                    1024) { throw std::invalid_argument("RaftConsensusRuntime: invalid receive-memory limit"); }
                self_ = requestClientOnly_ ? &*requestClientSelf_ : config_.findById(selfNodeId_);
                if (self_ == nullptr || !self_->dataBearing()) { throw std::invalid_argument("RaftConsensusRuntime: local node must be data-bearing"); }
                learnerBootstrapPending_ = self_->raftLearner();
                if (runtimeOptions_.transportMode == TransportMode::SECURE) { localIdentity_ = loadSecureIdentity(runtimeOptions_); }
                committedMembers_ = config_.dataNodes();
                refreshPeersFromMembershipLocked();
                if (config_.dataNodes().empty()) { throw std::invalid_argument("RaftConsensusRuntime: no data-bearing nodes"); }
                if (requestClientOnly_) { return; }
                if (!requestClientOnly_ && (runtimeOptions_.requests.enabled || std::filesystem::exists(dbDir_.empty() ? std::filesystem::path{"cluster-raft-results"} : dbDir_ / "raft-results"))) { ensureResultStore(); }
                recoverState();
                recoverLog();
                validateRecoveredStateMachine();
                lastAssignedStateMachineSeq_ = lastIncludedStateMachineSeq_;
                for (const auto& entry : log_) {
                    if (isStateMachineEntry(entry.kind)) { lastAssignedStateMachineSeq_ = std::max(lastAssignedStateMachineSeq_, entry.clientSeq); }
                }
                if (callbacks_.getLastSeq) { lastAssignedStateMachineSeq_ = std::max(lastAssignedStateMachineSeq_, callbacks_.getLastSeq()); }
                this->replayCommittedMembership();
                rebuildRequestResultsLocked();
                validateLogPrefixLocked(log_.size());
                validatePeerConfiguration(committedMembers_, true);
                if (jointOldMembers_) { validatePeerConfiguration(*jointOldMembers_, true); }
                if (jointNewMembers_) { validatePeerConfiguration(*jointNewMembers_, true); }
                validatePeerConfiguration(replicationTargetsLocked());
            }

            ~Impl() { try { this->close(); } catch (...) {} }

            void start() {
                std::lock_guard lifecycle{lifecycleMutex_};
                if (applyFailed_.load()) { throw std::runtime_error("Raft: reopen required after state-machine application failure"); }
                {
                    std::lock_guard lock{workersMutex_};
                    workersStopping_ = false;
                }
                if (logIoFailed_) { throw std::runtime_error("Raft log: reopen required after persistence failure"); }
                if (resultStore_ && resultStore_->isClosed()) { resultStore_.reset(); ensureResultStore(); observeResultStore(); }
                this->recoverDurableSnapshotInstall();
                {
                    std::lock_guard lock{mutex_};
                    if (running_) { return; }
                    if (timerThread_.joinable() || proposalThread_.joinable() || snapshotThread_.joinable()) {
                        throw std::runtime_error("RaftConsensusRuntime: close the stopped runtime before restarting");
                    }
                    running_ = true;
                    electionDeadline_ = this->nextElectionDeadline();
                    nextCompactionCheck_ = Clock::now();
                    nextCompactionDeadline_ = Clock::now() + std::chrono::milliseconds{runtimeOptions_.raftSnapshot.maxIntervalMs};
                }
                if (requestClientOnly_) { return; }
                bool endpointStarted = false;
                runtimeStartedAtUs_.store(observationNowUs(), std::memory_order_relaxed);
                health_.store(ClusterHealthState::HEALTHY, std::memory_order_relaxed);
                try {
                    // Finish recovery before peers can read the state machine or
                    // advance consensus over a partially replayed application.
                    this->applyCommitted();
                    const SocketHandle listenSock = listenOn(runtimeOptions_.replBindHost, self_->replPort);
                    {
                        std::lock_guard lock{mutex_};
                        listenSock_ = listenSock;
                    }
                    acceptThread_ = std::thread([this, listenSock] { this->acceptLoop(listenSock); });
                    timerThread_ = std::thread([this] { this->timerLoop(); });
                    proposalThread_ = std::thread([this] { this->proposalLoop(); });
                    if (runtimeOptions_.raftSnapshot.enabled && callbacks_.exportSnapshot) {
                        snapshotThread_ = std::thread([this] {
                            try {
                                for (;;) {
                                    std::unique_lock lock{mutex_};
                                    if (cv_.wait_for(lock, std::chrono::milliseconds{200}, [this] { return !running_; })) { return; }
                                    lock.unlock();
                                    maybeCompactLog();
                                }
                            } catch (...) {
                                std::lock_guard lock{mutex_};
                                recordFailure(ClusterFailureCode::RAFT_PERSISTENCE, ClusterHealthState::FAILED);
                                stopRuntimeLocked();
                            }
                        });
                    }
                    endpointStarted = true;
                    bool shouldBecomeSingleNodeLeader = false;
                    uint64_t singleNodeTerm = 0;
                    {
                        std::lock_guard lock{mutex_};
                        shouldBecomeSingleNodeLeader = peers_.empty() && self_->coordinatorEligible() && isVotingMemberLocked(selfNodeId_);
                        singleNodeTerm = currentTerm_ == 0 ? 1 : currentTerm_;
                    }
                    if (shouldBecomeSingleNodeLeader) { this->becomeLeader(singleNodeTerm); }
                }
                catch (...) {
                    if (!endpointStarted && !applyFailed_.load()) {
                        ++endpointStartFailures_;
                        recordFailure(ClusterFailureCode::ENDPOINT_START, ClusterHealthState::FAILED);
                    }
                    close();
                    throw;
                }
            }

            void close() {
                std::lock_guard lifecycle{lifecycleMutex_};
                SocketHandle listener = BAD_SOCKET;
                {
                    std::lock_guard lock{mutex_};
                    stopRuntimeLocked();
                    listener = listenSock_;
                    listenSock_ = BAD_SOCKET;
                    finishProposalsLocked();
                    proposalQueue_.clear();
                }
                shutdownSocket(listener);
                closeSocket(listener);
                std::vector<std::shared_ptr<PeerWorker>> workers;
                {
                    std::lock_guard lock{workersMutex_};
                    workersStopping_ = true;
                    for (auto& worker : workers_ | std::views::values) { workers.push_back(worker); }
                    for (auto& weak : forwardChannels_) { if (auto worker = weak.lock()) { workers.push_back(std::move(worker)); } }
                    for (auto& weak : placementReadChannels_) { if (auto worker = weak.lock()) { workers.push_back(std::move(worker)); } }
                    forwardChannelsCv_.notify_all();
                }
                for (auto& worker : workers) {
                    {
                        std::lock_guard lock{worker->mutex};
                        worker->stopping = true;
                        worker->tasks.clear();
                        worker->cv.notify_all();
                    }
                    std::lock_guard socketLock{worker->socketMutex};
                    shutdownSocket(worker->socket);
                }
                if (acceptThread_.joinable()) { acceptThread_.join(); }
                {
                    std::lock_guard lock{clientHandlersMutex_};
                    for (auto socket : clientSockets_) { shutdownSocket(socket); }
                }
                if (timerThread_.joinable()) { timerThread_.join(); }
                if (proposalThread_.joinable()) { proposalThread_.join(); }
                if (snapshotThread_.joinable()) { snapshotThread_.join(); }
                for (auto& worker : workers) { if (worker->thread.joinable()) { worker->thread.join(); } }
                {
                    std::lock_guard lock{workersMutex_};
                    workers_.clear();
                }
                std::unique_lock clientLock{clientHandlersMutex_};
                clientHandlersCv_.wait(clientLock, [this] { return activeClientHandlers_.load() == 0; });
                if (resultStore_) { resultStore_->close(); }
            }

            RaftRuntimeStats stats() const {
                RaftRuntimeStats out;
                out.enabled = true;
                runtimeOptions_.nodeResourceBudget->observe(out);
                out.sampledAtUs = observationNowUs();
                out.runtimeStartedAtUs = runtimeStartedAtUs_.load(std::memory_order_relaxed);
                out.clusterGroupId = runtimeOptions_.clusterGroupId;
                out.clusterGroupEpoch = runtimeOptions_.clusterGroupEpoch;
                out.health = health_.load(std::memory_order_relaxed);
                out.lastFailure = lastFailure_.load(std::memory_order_relaxed);
                out.lastFailureAtUs = lastFailureAtUs_.load(std::memory_order_relaxed);
                out.outboundConnections = outboundConnections_.load(std::memory_order_relaxed);
                out.peerWorkers = peerWorkersStarted_.load(std::memory_order_relaxed);
                out.proposalBatches = proposalBatches_.load(std::memory_order_relaxed);
                out.expiredRequests = expiredRequests_.load(std::memory_order_relaxed);
                out.rejectedRequests = rejectedRequests_.load(std::memory_order_relaxed);
                out.peerPolicyMismatchRejects = peerPolicyMismatchRejects_.load(std::memory_order_relaxed);
                out.foreignClusterRejects = foreignClusterRejects_.load(std::memory_order_relaxed);
                out.endpointStartFailures = endpointStartFailures_.load(std::memory_order_relaxed);
                out.requestJournalBytesWritten = requestJournalBytesWritten_.load(std::memory_order_relaxed);
                out.requestJournalCompactions = requestJournalCompactions_.load(std::memory_order_relaxed);
                out.transferMemoryBytes = receiveBudget_->used();
                out.forwardReceiveMemoryBytes = forwardReceiveBudget_->used();
                out.controlReceiveMemoryBytes = controlReceiveBudget_->used();
                {
                    std::lock_guard lock{clientHandlersMutex_};
                    out.incomingConsensusConnections = incomingConsensusPeers_.size();
                    out.incomingForwardConnections = incomingForwardChannels_;
                    out.incomingControlConnections = incomingControlChannels_;
                }
                {
                    std::lock_guard lock{mutex_};
                    out.currentTerm = currentTerm_;
                    out.configurationGeneration = lastConfigurationGeneration_;
                    out.jointConsensus = jointNewMembers_.has_value();
                    out.leaderNodeId = role_.load() == RaftRole::LEADER ? selfNodeId_ : (observedLeaderTerm_ == currentTerm_ ? observedLeaderId_ : 0);
                    out.commitIndex = commitIndex_;
                    out.appliedIndex = lastApplied_;
                    out.appliedStateMachineSeq = lastIncludedStateMachineSeq_;
                    const auto appliedCount = lastApplied_ <= lastIncludedIndex_
                                                  ? size_t{0}
                                                  : static_cast<size_t>(std::min<uint64_t>(lastApplied_ - lastIncludedIndex_, log_.size()));
                    if (appliedCount != 0 && appliedCount <= validatedLog_.size()) {
                        out.appliedStateMachineSeq = std::max(out.appliedStateMachineSeq, validatedLog_[appliedCount - 1].sequence);
                    }
                    else {
                        // A failed persistence rollback can invalidate the cache.
                        for (size_t i = appliedCount; i != 0; --i) {
                            const auto& entry = log_[i - 1];
                            if (isStateMachineEntry(entry.kind)) {
                                out.appliedStateMachineSeq = std::max(out.appliedStateMachineSeq, entry.clientSeq);
                                break;
                            }
                        }
                    }
                    out.lastLogIndex = lastLogIndex();
                    out.snapshotIndex = lastIncludedIndex_;
                    const auto members = replicationTargetsLocked();
                    for (const auto& member : members) {
                        if (isVotingMemberLocked(member.nodeId)) { ++out.voterCount; }
                        else { ++out.learnerCount; }
                    }
                    out.localLearner = isReplicationMemberLocked(selfNodeId_) && !isVotingMemberLocked(selfNodeId_);
                    using Availability = ClusterOperationAvailability;
                    const auto now = Clock::now();
                    const auto freshness = std::chrono::milliseconds{baselineElectionTimeoutMs()};
                    if (!running_ || out.health == ClusterHealthState::FAILED) {
                        out.quorumAvailability = out.writeAvailability = out.linearizableReadAvailability = Availability::UNAVAILABLE;
                        if (out.health != ClusterHealthState::FAILED) { out.health = ClusterHealthState::DEGRADED; }
                    }
                    else if (role_.load() == RaftRole::LEADER) {
                        std::vector<uint64_t> reachable{selfNodeId_};
                        for (const auto& peer : peerReplication_) {
                            if (peer.observedTerm == currentTerm_ && peer.successfulAppend && now - *peer.successfulAppend < freshness) {
                                reachable.push_back(peer.node.nodeId);
                            }
                        }
                        // Reuse voting rules, including appended joint membership and learner exclusion.
                        const auto quorum = hasElectionQuorumLocked(reachable);
                        out.quorumAvailability = quorum ? Availability::AVAILABLE : Availability::UNAVAILABLE;
                        const auto established = commitIndex_ != 0 && termAt(commitIndex_) == currentTerm_;
                        const auto ready = quorum && established && !logIoFailed_ && !durableSnapshotInstall_ && !pendingSnapshotInstall_.active;
                        out.writeAvailability = ready && !administrativeChange_ ? Availability::AVAILABLE : Availability::UNAVAILABLE;
                        out.linearizableReadAvailability = ready && lastApplied_ >= commitIndex_ && !pendingJointMembershipLocked()
                                                               ? Availability::AVAILABLE
                                                               : Availability::UNAVAILABLE;
                        const auto allPeers = std::ranges::all_of(
                            members,
                            [&](const NodeInfo& member) { return std::ranges::contains(reachable, member.nodeId); }
                        );
                        if (!quorum || !established || !allPeers) { out.health = ClusterHealthState::DEGRADED; }
                    }
                    else {
                        const auto leader = role_.load() == RaftRole::FOLLOWER && observedLeaderTerm_ == currentTerm_ && observedLeaderId_ != 0 &&
                            lastLeaderContact_ && now - *lastLeaderContact_ < freshness;
                        // A follower observes leader contact, not the leader's majority or forwarding readiness.
                        if (!leader) { out.health = ClusterHealthState::DEGRADED; }
                        if (!leader) { out.writeAvailability = out.linearizableReadAvailability = Availability::UNAVAILABLE; }
                    }
                    out.proposalQueueDepth = proposalQueue_.size();
                    out.pendingProposals = proposals_.size();
                    out.retainedRequestResults = requestState_.count;
                    out.retainedRequestResultBytes = requestState_.bytes;
                    out.requestResultByteLimit = runtimeOptions_.requests.maxRetainedResultBytes;
                    out.requestCapacity = runtimeOptions_.requests.maxPendingRequests;
                    out.requestJournalBytes = requestJournalBytes_;
                    out.requestJournalRecords = requestJournalRecords_;
                    for (const auto& proposal : proposals_) { if (proposal->request && !proposal->error) { ++out.pendingRequests; } }
                    for (const auto& proposal : proposalQueue_) { out.replicationQueueBytes += proposal->bytes; }
                    out.peers.reserve(members.size());
                    for (const auto& member : members) {
                        if (member.nodeId == selfNodeId_) { continue; }
                        RaftPeerStats peerStats;
                        peerStats.nodeId = member.nodeId;
                        peerStats.learner = !isVotingMemberLocked(member.nodeId);
                        const auto peer = std::ranges::find_if(peerReplication_, [&](const auto& state) { return state.node.nodeId == member.nodeId; });
                        if (peer != peerReplication_.end()) {
                            peerStats.matchIndex = peer->matchIndex;
                            peerStats.nextIndex = peer->nextIndex;
                            peerStats.replicationLag = out.lastLogIndex > peer->matchIndex ? out.lastLogIndex - peer->matchIndex : 0;
                        }
                        out.peers.push_back(peerStats);
                    }
                }
                std::lock_guard workersLock{workersMutex_};
                for (const auto& [nodeId, worker] : workers_) {
                    {
                        std::lock_guard workLock{worker->mutex};
                        out.replicationQueueFrames += worker->tasks.size() + (worker->replicate ? 1u : 0u);
                    }
                    const auto peer = std::ranges::find(out.peers, nodeId, &RaftPeerStats::nodeId);
                    if (peer != out.peers.end()) {
                        std::lock_guard socketLock{worker->socketMutex};
                        peer->connected = socketOk(worker->socket);
                        peer->lastSuccessfulContactAtUs = worker->lastSuccessfulContactAtUs.load(std::memory_order_relaxed);
                        peer->roundTripsSucceeded = worker->roundTripsSucceeded.load(std::memory_order_relaxed);
                        peer->roundTripsFailed = worker->roundTripsFailed.load(std::memory_order_relaxed);
                        peer->consecutiveRoundTripFailures = worker->consecutiveRoundTripFailures.load(std::memory_order_relaxed);
                        peer->lastRoundTripAtUs = worker->lastRoundTripAtUs.load(std::memory_order_relaxed);
                        peer->lastRoundTripFailureAtUs = worker->lastRoundTripFailureAtUs.load(std::memory_order_relaxed);
                        peer->roundTripLatencyUs = worker->roundTripLatencyUs.snapshot();
                    }
                }
                out.sampledAtUs = observationNowUs();
                return out;
            }

            NodeRole role() const noexcept {
                const auto role = role_.load();
                if (role == RaftRole::LEADER) { return NodeRole::PRIMARY; }
                return NodeRole::REPLICA;
            }

            uint64_t leaderNodeId() const {
                std::lock_guard lock{mutex_};
                return role_.load() == RaftRole::LEADER ? selfNodeId_ : observedLeaderTerm_ == currentTerm_ ? observedLeaderId_ : 0;
            }

            std::vector<NodeInfo> activeNodes() const {
                std::lock_guard lock{mutex_};
                return this->replicationTargetsLocked();
            }

            const ClusterRouter& router() const noexcept { return router_; }

            uint64_t linearizableReadBarrier() {
                uint64_t term = 0, readIndex = 0, round = 0;
                std::vector<NodeInfo> targets, voters;
                std::optional<std::vector<NodeInfo>> oldMembers, newMembers;
                // A newly elected leader must commit its current-term NOOP once.
                bool established = false;
                {
                    std::lock_guard lock{mutex_};
                    established = commitIndex_ != 0 && termAt(commitIndex_) == currentTerm_;
                }
                if (!established && !commitOutstandingEntry(std::chrono::milliseconds{config_.consistency().ackTimeoutMs})) {
                    throw std::runtime_error("RaftConsensusRuntime: current-term commit required for ReadIndex");
                }
                {
                    std::lock_guard lock{mutex_};
                    if (!running_ || role_.load() != RaftRole::LEADER || termAt(commitIndex_) != currentTerm_) {
                        throw std::runtime_error("RaftConsensusRuntime: local node has no established leadership");
                    }
                    if (pendingJointMembershipLocked()) {
                        throw std::runtime_error("RaftConsensusRuntime: membership change already in progress during ReadIndex");
                    }
                    term = currentTerm_;
                    readIndex = commitIndex_;
                    if (nextReadRound_ == UINT64_MAX) { throw std::runtime_error("Raft read rounds exhausted"); }
                    round = ++nextReadRound_;
                    targets = peers_;
                    voters = committedMembers_;
                    oldMembers = jointOldMembers_;
                    newMembers = jointNewMembers_;
                }
                const auto deadline = detail::ReconfigurationDeadline::cap(Clock::now() + std::chrono::milliseconds{config_.consistency().ackTimeoutMs});
                for (;;) {
                    detail::ReconfigurationDeadline::check();
                    for (const auto& peer : targets) { scheduleReplication(peer, readIndex, true, round); }
                    std::unique_lock lock{mutex_};
                    const auto confirmed = [&] {
                        std::vector<uint64_t> acknowledgers{selfNodeId_};
                        for (const auto& [id, ack] : readAcks_) {
                            const auto& [ackTerm, ackRound] = ack;
                            if (ackTerm == term && ackRound >= round) { acknowledgers.push_back(id); }
                        }
                        return oldMembers && newMembers
                                   ? votedByMajority(*oldMembers, acknowledgers) && votedByMajority(*newMembers, acknowledgers)
                                   : votedByMajority(voters, acknowledgers);
                    };
                    peerReplicationCv_.wait_until(
                        lock,
                        std::min(deadline, Clock::now() + std::chrono::milliseconds{50}),
                        [&] { return !running_ || currentTerm_ != term || role_.load() != RaftRole::LEADER || confirmed(); }
                    );
                    if (!running_ || currentTerm_ != term || role_.load() != RaftRole::LEADER || pendingJointMembershipLocked() || !
                        sameNodeSet(voters, committedMembers_) || !sameOptionalNodeSet(oldMembers, jointOldMembers_) || !sameOptionalNodeSet(
                            newMembers,
                            jointNewMembers_
                        )) { throw std::runtime_error("RaftConsensusRuntime: leadership or membership changed during ReadIndex"); }
                    if (confirmed()) { break; }
                    if (Clock::now() >= deadline) { throw std::runtime_error("RaftConsensusRuntime: ReadIndex quorum timed out"); }
                }
                applyCommitted();
                std::lock_guard lock{mutex_};
                if (!running_ || currentTerm_ != term || role_.load() != RaftRole::LEADER || lastApplied_ < readIndex) {
                    throw std::runtime_error("RaftConsensusRuntime: ReadIndex is not applied under current leadership");
                }
                return term;
            }

            ForwardResponse forwardTo(uint64_t nodeId, ForwardRequest request) {
                const auto nodes = activeNodes();
                const auto it = std::ranges::find(nodes, nodeId, &NodeInfo::nodeId);
                if (it == nodes.end() || nodeId == selfNodeId_) { throw ClusterRoutingError(ClusterRoutingCode::FORWARD_UNAVAILABLE); }
                request.requestId = nextForwardId_.fetch_add(1);
                const auto wire = encodeForwardRequest(request);
                if (wire.empty()) { throw ClusterRoutingError(ClusterRoutingCode::PAYLOAD_TOO_LARGE); }
                DecodedFrame frame;
                ForwardResponse response;
                bool submitted = false;
                bool completed = false;
                try {
                    completed = exchangeRpc(*it, wire, ReplMsgType::FORWARD_RESPONSE, frame, static_cast<int>(request.timeoutMs), &submitted, &request) &&
                        decodeForwardResponse(frame.payload, response) && response.requestId == request.requestId;
                }
                catch (...) {
                    // Allocation/transport failures after send are just as
                    // ambiguous as a lost response; never lose this distinction.
                }
                if (!completed) {
                    throw ClusterRoutingError(
                        submitted && forwardMayMutate(request.operation) ? ClusterRoutingCode::OUTCOME_UNKNOWN : ClusterRoutingCode::FORWARD_UNAVAILABLE,
                        {},
                        "Forward RPC failed; an operation sent to the destination must not be replayed blindly"
                    );
                }
                response.memoryReservation = std::move(frame.memoryReservation);
                return response;
            }
            void setRequestForwardClients(std::vector<uint64_t> nodes) {
                if (runtimeOptions_.raftGroupKind != RaftGroupKind::REQUEST_AUTHORITY) { throw std::logic_error("Raft: not a request authority"); }
                std::lock_guard lock{mutex_}; requestForwardClients_ = std::move(nodes);
            }
            void setRequestForwardTargets(std::vector<NodeInfo> nodes) {
                if (!requestClientOnly_) { return; }
                std::lock_guard lock{mutex_}; committedMembers_ = std::move(nodes); refreshPeersFromMembershipLocked();
            }

            std::atomic<uint64_t> nextForwardId_{1};

            std::shared_future<ClusterRequestResult> submitRequest(
                const ClusterRequestId& id,
                const std::array<uint8_t, 32>& fingerprint,
                const ClusterMutationFactory& prepare,
                uint64_t expectedTerm
            ) {
                if (!prepare) { throw std::invalid_argument("Raft: retry-safe mutation factory is required"); }
                auto admission = detail::ReconfigurationDeadline::lock(mutationAdmissionMutex_);
                auto proposal = std::make_shared<Proposal>();
                proposal->request = std::make_shared<RequestCompletion>();
                proposal->request->id = id;
                proposal->request->fingerprint = fingerprint;
                uint64_t now = 0, term = 0, sequence = 0;
                {
                    std::unique_lock lock{mutex_};
                    waitForAdministrationLocked(lock);
                    if (!running_ || role_.load() != RaftRole::LEADER || administrativeChange_) {
                        throw std::runtime_error("Raft: retry-safe write requires an active leader");
                    }
                    if (expectedTerm != 0 && currentTerm_ != expectedTerm) {
                        throw ClusterRoutingError(ClusterRoutingCode::WRITE_REJECTED, {}, "Raft: validation term changed before request admission", id);
                    }
                    now = validateRequestLocked(id);
                    if (id.expiresAtUnixMs <= now) { return readyRequest({ClusterRequestStatus::EXPIRED}); }
                    const auto checkFingerprint = [&](const auto& value) {
                        if (value != fingerprint) { throw std::invalid_argument("Raft: request id reused for a different mutation"); }
                    };
                    if (const auto record = retainedRequest(id)) {
                        checkFingerprint(record->fingerprint); return readyRequest({ClusterRequestStatus::APPLIED, record->sequence, record->index});
                    }
                    for (const auto& pending : proposals_) {
                        if (pending->request && pending->request->id == id && !pending->error) {
                            checkFingerprint(pending->request->fingerprint);
                            return pending->request->future;
                        }
                    }
                    for (const auto& entry : log_) {
                        if (entry.requestId != id) { continue; }
                        checkFingerprint(entry.requestFingerprint);
                        proposal->index = entry.index;
                        proposal->entryTerm = entry.term;
                        proposal->request->sequence = entry.clientSeq;
                        proposal->term = currentTerm_;
                        proposal->deadline = detail::ReconfigurationDeadline::cap(Clock::now() + std::chrono::milliseconds{config_.consistency().ackTimeoutMs});
                        proposals_.push_back(proposal);
                        cv_.notify_all();
                        return proposal->request->future;
                    }
                    size_t tracked = 0;
                    for (const auto& entry : log_) {
                        if (entry.index > lastApplied_ && entry.requestId && entry.requestId->expiresAtUnixMs > now) { ++tracked; }
                    }
                    for (const auto& pending : proposalQueue_) { if (pending->request && !pending->error) { ++tracked; } }
                    ensureResultStore();
                    if (tracked >= runtimeOptions_.requests.maxPendingRequests || !resultStore_->canReserve((tracked + 1) * detail::DiskResults::recordCharge(24, 48), now)) {
                        rejectedRequests_.fetch_add(1, std::memory_order_relaxed);
                        throw std::runtime_error("Raft: pending-request or retained-result disk budget exhausted; unexpired results are never evicted");
                    }
                    term = currentTerm_;
                    sequence = reserveStateMachineSequenceLocked();
                }
                auto entries = mutationEntries(sequence, prepare(sequence));
                auto& mutationEntry = entries.back();
                mutationEntry.requestId = id;
                mutationEntry.requestFingerprint = fingerprint;
                mutationEntry.requestTime = now;
                proposal->request->sequence = sequence;
                // Preparation can include Blob I/O. Never admit its mutation in a
                // different leadership term than the deduplication check.
                {
                    std::lock_guard lock{mutex_};
                    if (!running_ || role_.load() != RaftRole::LEADER || currentTerm_ != term) {
                        throw std::runtime_error("Raft: leadership changed during request preparation");
                    }
                }
                proposal->term = term;
                (void)submitProposal(std::move(entries), proposal);
                return proposal->request->future;
            }

            ClusterRequestResult queryRequest(const ClusterRequestId& id) {
                linearizableReadBarrier();
                std::lock_guard lock{mutex_};
                const auto now = validateRequestLocked(id);
                if (id.expiresAtUnixMs <= now) { return {ClusterRequestStatus::EXPIRED}; }
                if (const auto record = retainedRequest(id)) { return {ClusterRequestStatus::APPLIED, record->sequence, record->index}; }
                if (std::ranges::contains(log_, id, &RaftLogEntry::requestId)) { return {ClusterRequestStatus::PENDING}; }
                for (const auto& proposal : proposalQueue_) {
                    if (proposal->request && proposal->request->id == id && !proposal->error) { return {ClusterRequestStatus::PENDING}; }
                }
                return {ClusterRequestStatus::NOT_FOUND};
            }

            ClusterMutationSubmission submitMutation(const ClusterMutationFactory& prepare, uint64_t expectedTerm = 0) {
                if (!prepare) { throw std::invalid_argument("RaftConsensusRuntime: mutation factory is required"); }
                auto admission = detail::ReconfigurationDeadline::lock(mutationAdmissionMutex_);
                const auto localCompletion = config_.consistency().mode != ConsistencyMode::RAFT_QUORUM;
                if (localCompletion) { linearizableReadBarrier(); }
                uint64_t sequence = 0;
                uint64_t term = 0;
                {
                    std::unique_lock lock{mutex_};
                    waitForAdministrationLocked(lock);
                    if (!running_ || role_.load() != RaftRole::LEADER || administrativeChange_) {
                        throw std::runtime_error("RaftConsensusRuntime: mutation requires an active leader");
                    }
                    if (expectedTerm != 0 && currentTerm_ != expectedTerm) {
                        throw ClusterRoutingError(ClusterRoutingCode::WRITE_REJECTED, {}, "Raft: validation term changed before mutation admission");
                    }
                    sequence = reserveStateMachineSequenceLocked();
                    term = currentTerm_;
                }
                auto entries = mutationEntries(sequence, prepare(sequence));
                auto proposal = std::make_shared<Proposal>();
                proposal->term = term;
                auto completion = submitProposal(std::move(entries), proposal);
                if (localCompletion) {
                    {
                        std::unique_lock lock{mutex_};
                        cv_.wait_until(
                            lock,
                            proposal->deadline,
                            [&] { return proposal->localDurable || proposal->error || !running_ || currentTerm_ != term; }
                        );
                        if (proposal->error) { std::rethrow_exception(proposal->error); }
                        if (!proposal->localDurable || !running_ || currentTerm_ != term || role_.load() != RaftRole::LEADER) {
                            throw ClusterRoutingError(ClusterRoutingCode::OUTCOME_UNKNOWN, {}, "RaftConsensusRuntime: local journal acceptance interrupted");
                        }
                    }
                    // Confirm the writer with a fresh quorum after durable local acceptance.
                    // Data replication continues independently; only committed entries become visible.
                    try { linearizableReadBarrier(); }
                    catch (const std::exception& error) { throw ClusterRoutingError(ClusterRoutingCode::OUTCOME_UNKNOWN, {}, error.what()); }
                    std::lock_guard lock{mutex_};
                    if (!running_ || currentTerm_ != term || role_.load() != RaftRole::LEADER) {
                        throw ClusterRoutingError(ClusterRoutingCode::OUTCOME_UNKNOWN, {}, "RaftConsensusRuntime: leadership changed during local acceptance");
                    }
                    std::promise<void> accepted;
                    completion = accepted.get_future();
                    accepted.set_value();
                }
                return ClusterMutationSubmission{.sequence = sequence, .completion = std::move(completion)};
            }

            std::future<void> submitEntry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t flags,
                uint64_t source
            ) {
                if (seq == 0) { throw std::invalid_argument("RaftConsensusRuntime: state-machine sequence must be non-zero"); }
                auto admission = detail::ReconfigurationDeadline::lock(mutationAdmissionMutex_);
                RaftLogEntry entry;
                entry.clientSeq = seq;
                entry.kind = RaftEntryKind::MUTATION;
                entry.op = op;
                entry.flags = flags;
                entry.sourceNodeId = source;
                entry.timestampNs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::system_clock::now().time_since_epoch()
                ).count());
                entry.key.assign(key.begin(), key.end());
                entry.value.assign(value.begin(), value.end());
                if (!isValidStateMachineEntry(entry)) { throw std::invalid_argument("RaftConsensusRuntime: entry exceeds frame or data storage limits"); }
                {
                    std::lock_guard lock{mutex_};
                    lastAssignedStateMachineSeq_ = std::max(lastAssignedStateMachineSeq_, seq);
                }
                return submitProposal({std::move(entry)});
            }

            void shipEntry(uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
                submitEntry(seq, op, key, value, flags, source).get();
            }

            void shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
                if (seq == 0) { throw std::invalid_argument("RaftConsensusRuntime: Blob state-machine sequence must be non-zero"); }
                if (runtimeOptions_.raftBlobPolicy == RaftBlobPolicy::PRIMARY_SIDE_ONLY) {
                    if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
                    return;
                }
                if (runtimeOptions_.raftBlobPolicy != RaftBlobPolicy::RAFT_LOG) {
                    throw std::runtime_error("RaftConsensusRuntime: RAFT_QUORUM does not support Blob payload replication");
                }
                std::vector<RaftLogEntry> entries;
                const auto contentCrc32c = crcBytes(content);
                const size_t chunkSize = runtimeOptions_.raftBlobChunkSizeBytes;
                size_t offset = 0;
                do {
                    const auto remaining = content.size() - offset;
                    const auto currentChunkSize = std::min(chunkSize, remaining);
                    RaftLogEntry entry;
                    entry.term = 0;
                    entry.index = entries.size() + 1;
                    entry.clientSeq = seq;
                    entry.kind = RaftEntryKind::BLOB;
                    entry.op = ReplOpType::PUT;
                    entry.flags = 0;
                    entry.sourceNodeId = selfNodeId_;
                    entry.key = encodeBlobChunkKey(
                        RaftBlobChunk{
                            .blobId = blobId,
                            .offset = offset,
                            .totalSize = content.size(),
                            .contentCrc32c = contentCrc32c,
                        }
                    );
                    entry.value.assign_range(content.subspan(offset, currentChunkSize));
                    entries.push_back(std::move(entry));
                    offset += currentChunkSize;
                }
                while (offset < content.size());

                submitProposal(std::move(entries)).get();
            }

            void addVotingNode(NodeInfo node, bool promotion = false) {
                AdministrativeGuard admission{*this};
                if (!node.dataBearing()) { throw std::invalid_argument("RaftConsensusRuntime: Raft voting node must be data-bearing"); }
                if (node.raftLearner()) { throw std::invalid_argument("RaftConsensusRuntime: voter cannot have RAFT_LEARNER capability"); }
                if (!onlineMembershipChangeEnabled()) { throw std::runtime_error("RaftConsensusRuntime: online Raft membership change is disabled"); }
                {
                    std::lock_guard lock{mutex_};
                    auto targets = replicationTargetsLocked();
                    if (!containsNode(targets, node.nodeId)) { targets.push_back(node); }
                    validatePeerConfiguration(targets);
                }
                (void)this->commitOutstandingEntry(std::chrono::milliseconds{20000});
                std::vector<NodeInfo> oldMembers;
                std::vector<NodeInfo> newMembers;
                bool finishExistingJoint = false;
                std::optional<RaftLogEntry> pendingConfigEntry;
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
                    const auto member = std::ranges::find(committedMembers_, node.nodeId, &NodeInfo::nodeId);
                    if (promotion) {
                        if (!config_.raft().membership.allowLearners) { throw std::runtime_error("RaftConsensusRuntime: learners are disabled"); }
                        if (member == committedMembers_.end() || !member->raftLearner()) {
                            throw std::invalid_argument("RaftConsensusRuntime: node is not a learner");
                        }
                        const auto* state = peerState(node.nodeId);
                        if (node.nodeId != selfNodeId_ && (state == nullptr || state->matchIndex < lastLogIndex())) {
                            throw std::runtime_error("RaftConsensusRuntime: learner has not caught up to the leader log");
                        }
                        node = *member;
                        node.capabilities &= ~static_cast<uint32_t>(RAFT_LEARNER);
                    }
                    else if (member != committedMembers_.end() && member->raftLearner()) {
                        throw std::invalid_argument("RaftConsensusRuntime: use promoteLearner for an existing learner");
                    }
                    if (commitIndex_ != lastLogIndex() && !log_.empty() && (log_.back().kind == RaftEntryKind::CONFIG_JOINT || log_.back().kind ==
                        RaftEntryKind::CONFIG_FINAL)) {
                        std::vector<NodeInfo> pendingNewMembers;
                        if (decodeNodeSet(log_.back().value, pendingNewMembers) && std::ranges::any_of(
                            pendingNewMembers,
                            [&](const NodeInfo& member) { return member.nodeId == node.nodeId && !member.raftLearner(); }
                        )) { pendingConfigEntry = log_.back(); }
                        else { throw std::runtime_error("RaftConsensusRuntime: cannot change membership while a prior entry is uncommitted"); }
                    }
                    else if (jointOldMembers_ || jointNewMembers_) {
                        if (jointNewMembers_ && std::ranges::any_of(
                            *jointNewMembers_,
                            [&](const NodeInfo& member) { return member.nodeId == node.nodeId && !member.raftLearner(); }
                        ) && (promotion || !containsNode(committedMembers_, node.nodeId))) {
                            oldMembers = jointOldMembers_.value_or(committedMembers_);
                            newMembers = *jointNewMembers_;
                            finishExistingJoint = true;
                        }
                        else { throw std::runtime_error("RaftConsensusRuntime: membership change already in progress"); }
                    }
                    else if (containsNode(committedMembers_, node.nodeId) && !promotion) {
                        throw std::invalid_argument("RaftConsensusRuntime: node is already a voting member");
                    }
                    else {
                        oldMembers = committedMembers_;
                        newMembers = committedMembers_;
                        std::erase_if(newMembers, [&](const NodeInfo& existing) { return existing.nodeId == node.nodeId; });
                        newMembers.push_back(node);
                        newMembers = sortedUniqueMembers(std::move(newMembers));
                    }
                }
                if (pendingConfigEntry) {
                    this->appendReplicateAndCommitConfig(*pendingConfigEntry, false);
                    if (pendingConfigEntry->kind == RaftEntryKind::CONFIG_JOINT) {
                        std::vector<NodeInfo> oldFinalMembers;
                        std::vector<NodeInfo> finalMembers;
                        if (!decodeNodeSet(pendingConfigEntry->key, oldFinalMembers) || !decodeNodeSet(pendingConfigEntry->value, finalMembers)) {
                            throw std::runtime_error("RaftConsensusRuntime: corrupt joint membership entry");
                        }
                        RaftLogEntry final = this->makeConfigEntry(RaftEntryKind::CONFIG_FINAL, oldFinalMembers, finalMembers);
                        this->appendReplicateAndCommitConfig(final);
                    }
                    return;
                }
                if (finishExistingJoint) {
                    RaftLogEntry final = this->makeConfigEntry(RaftEntryKind::CONFIG_FINAL, oldMembers, newMembers);
                    this->appendReplicateAndCommitConfig(final);
                    return;
                }
                this->proposeMembershipChange(oldMembers, newMembers);
            }

            void addLearner(const NodeInfo& node) { changeLearner(node, false); }

            void removeLearner(uint64_t nodeId) {
                NodeInfo node;
                node.nodeId = nodeId;
                changeLearner(std::move(node), true);
            }

            void promoteLearner(uint64_t nodeId) {
                NodeInfo node;
                {
                    std::lock_guard lock{mutex_};
                    const auto member = std::ranges::find(committedMembers_, nodeId, &NodeInfo::nodeId);
                    if (member == committedMembers_.end() || !member->raftLearner()) {
                        throw std::invalid_argument("RaftConsensusRuntime: node is not a learner");
                    }
                    node = *member;
                    node.capabilities &= ~static_cast<uint32_t>(RAFT_LEARNER);
                }
                addVotingNode(std::move(node), true);
            }

            void removeVotingNode(uint64_t nodeId) {
                AdministrativeGuard admission{*this};
                if (!onlineMembershipChangeEnabled()) { throw std::runtime_error("RaftConsensusRuntime: online Raft membership change is disabled"); }
                (void)this->commitOutstandingEntry(std::chrono::milliseconds{20000});
                if (nodeId == selfNodeId_) { throw std::runtime_error("RaftConsensusRuntime: removing the local leader is not supported by this API"); }
                std::vector<NodeInfo> oldMembers;
                std::vector<NodeInfo> newMembers;
                bool finishExistingJoint = false;
                std::optional<RaftLogEntry> pendingConfigEntry;
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
                    if (commitIndex_ != this->lastLogIndex() && !log_.empty() && (log_.back().kind == RaftEntryKind::CONFIG_JOINT || log_.back().kind ==
                        RaftEntryKind::CONFIG_FINAL)) {
                        std::vector<NodeInfo> pendingNewMembers;
                        if (decodeNodeSet(log_.back().value, pendingNewMembers) && !containsNode(pendingNewMembers, nodeId)) {
                            pendingConfigEntry = log_.back();
                        }
                        else { throw std::runtime_error("RaftConsensusRuntime: cannot change membership while a prior entry is uncommitted"); }
                    }
                    else if (jointOldMembers_ || jointNewMembers_) {
                        if (jointOldMembers_ && jointNewMembers_ && containsNode(*jointOldMembers_, nodeId) && !containsNode(*jointNewMembers_, nodeId)) {
                            oldMembers = *jointOldMembers_;
                            newMembers = *jointNewMembers_;
                            finishExistingJoint = true;
                        }
                        else { throw std::runtime_error("RaftConsensusRuntime: membership change already in progress"); }
                    }
                    else if (!isVotingMemberLocked(nodeId)) { throw std::invalid_argument("RaftConsensusRuntime: node is not a voting member"); }
                    else {
                        oldMembers = committedMembers_;
                        for (const auto& node : committedMembers_) { if (node.nodeId != nodeId) { newMembers.push_back(node); } }
                        if (std::ranges::none_of(newMembers, [](const NodeInfo& node) { return !node.raftLearner(); })) {
                            throw std::invalid_argument("RaftConsensusRuntime: cannot remove the last voting member");
                        }
                    }
                }
                if (pendingConfigEntry) {
                    this->appendReplicateAndCommitConfig(*pendingConfigEntry, false);
                    if (pendingConfigEntry->kind == RaftEntryKind::CONFIG_JOINT) {
                        std::vector<NodeInfo> oldFinalMembers;
                        std::vector<NodeInfo> finalMembers;
                        if (!decodeNodeSet(pendingConfigEntry->key, oldFinalMembers) || !decodeNodeSet(pendingConfigEntry->value, finalMembers)) {
                            throw std::runtime_error("RaftConsensusRuntime: corrupt joint membership entry");
                        }
                        RaftLogEntry final = this->makeConfigEntry(RaftEntryKind::CONFIG_FINAL, oldFinalMembers, finalMembers);
                        this->appendReplicateAndCommitConfig(final);
                    }
                    return;
                }
                if (finishExistingJoint) {
                    RaftLogEntry final = this->makeConfigEntry(RaftEntryKind::CONFIG_FINAL, oldMembers, newMembers);
                    this->appendReplicateAndCommitConfig(final);
                    return;
                }
                this->proposeMembershipChange(oldMembers, newMembers);
            }

            void campaignLeadership() {
                std::lock_guard lock{mutex_};
                if (!running_ || !self_->coordinatorEligible() || !isVotingMemberLocked(selfNodeId_)) {
                    throw std::runtime_error("RaftConsensusRuntime: campaign requires an eligible active voter");
                }
                if (role_.load() == RaftRole::LEADER) { return; }
                forceElection_ = true;
                cv_.notify_all();
            }

            void transferLeadership(uint64_t targetNodeId) {
                AdministrativeGuard admission{*this};
                transferLeadershipAdmitted(targetNodeId);
            }

            void reconfigure(const ClusterConfig& target, uint64_t generation) {
                detail::ReconfigurationDeadline budget{runtimeOptions_.reconfiguration.timeoutMs, runtimeOptions_.reconfiguration.cancelled};
                detail::ReconfigurationDeadline::check();
                target.validate();
                if (raftCompatibilityFingerprint(target, runtimeOptions_, callbacks_) != compatibilityFingerprint_) {
                    throw std::invalid_argument("Raft reconfiguration: online changes must preserve cluster identity and consensus policy");
                }
                if (!onlineMembershipChangeEnabled()) { throw std::runtime_error("Raft reconfiguration: enable joint consensus and online voter changes"); }
                auto desired = sortedUniqueMembers(target.dataNodes());
                if (desired.empty() || !hasVoter(desired)) { throw std::invalid_argument("Raft reconfiguration: at least one voter is required"); }
                if (std::ranges::none_of(desired, [](const auto& node) { return !node.raftLearner() && node.coordinatorEligible(); })) {
                    throw std::invalid_argument("Raft reconfiguration: at least one eligible voter is required");
                }
                if (role_.load() != RaftRole::LEADER) {
                    ClusterRouteTarget destination;
                    const auto state = stats();
                    destination.nodeId = state.leaderNodeId;
                    destination.raftTerm = state.currentTerm;
                    throw ClusterRoutingError(ClusterRoutingCode::NOT_OWNER, destination);
                }
                AdministrativeGuard admission{*this, false};
                const auto same = [](const auto& left, const auto& right) { return encodeNodeSet(left) == encodeNodeSet(right); };
                std::vector<NodeInfo> old;
                bool completeJoint = false;
                bool retireLeader = false;
                std::vector<NodeInfo> catchup;
                uint64_t catchupIndex = 0;
                uint64_t migrationTerm = 0;
                {
                    std::lock_guard lock{mutex_};
                    migrationTerm = currentTerm_;
                    if (generation < lastConfigurationGeneration_) { throw std::runtime_error("Raft reconfiguration: stale placement generation"); }
                    old = committedMembers_;
                    if (!containsNode(desired, selfNodeId_)) {
                        if (!config_.raft().membership.allowLearners) {
                            throw std::invalid_argument("Raft reconfiguration: removing the leader requires allowLearners or prior leadership transfer");
                        }
                        auto retiring = *self_;
                        retiring.capabilities |= RAFT_LEARNER;
                        desired.push_back(retiring);
                        desired = sortedUniqueMembers(std::move(desired));
                        retireLeader = true;
                    }
                    if (same(old, desired) && !jointNewMembers_ && !retireLeader && generation == lastConfigurationGeneration_) { return; }
                    auto all = old;
                    all.insert(all.end(), desired.begin(), desired.end());
                    all = sortedUniqueMembers(std::move(all));
                    validatePeerConfiguration(all);
                    ensurePeerReplicationTargetsLocked(&all);
                    catchup = desired;
                    catchupIndex = lastLogIndex();
                    if (jointNewMembers_) {
                        if (!same(*jointNewMembers_, desired)) { throw std::runtime_error("Raft reconfiguration: another membership transition is pending"); }
                        old = *jointOldMembers_;
                        completeJoint = true;
                    }
                }
                // Every new holder receives the complete committed prefix before
                // it can vote or placement can advertise the copy as available.
                const auto needsCatchup = [&](const NodeInfo& holder) {
                    const auto existing = std::ranges::find(old, holder.nodeId, &NodeInfo::nodeId);
                    return existing == old.end() || (existing->raftLearner() && !holder.raftLearner());
                };
                for (const auto& holder : catchup) {
                    if (holder.nodeId != selfNodeId_ && needsCatchup(holder) && !replicatePeerTo(holder.nodeId, catchupIndex, true, 0, true)) {
                        throw std::runtime_error("Raft reconfiguration: new holder did not catch up");
                    }
                }
                // Bulk transfer leaves write admission open. Only the final tail
                // and joint membership commit need a stable journal boundary.
                admission.pauseWrites();
                (void)commitOutstandingEntry(std::chrono::milliseconds{runtimeOptions_.reconfiguration.timeoutMs});
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER || currentTerm_ != migrationTerm) {
                        ClusterRouteTarget destination;
                        destination.nodeId = observedLeaderTerm_ == currentTerm_ ? observedLeaderId_ : 0;
                        destination.raftTerm = currentTerm_;
                        throw ClusterRoutingError(ClusterRoutingCode::NOT_OWNER, destination);
                    }
                    catchupIndex = lastLogIndex();
                }
                for (const auto& holder : catchup) {
                    if (holder.nodeId != selfNodeId_ && needsCatchup(holder) && !replicatePeerTo(holder.nodeId, catchupIndex, true, 0, true)) {
                        throw std::runtime_error("Raft reconfiguration: new holder did not catch up to the final write boundary");
                    }
                }
                if (completeJoint) { appendReplicateAndCommitConfig(makeConfigEntry(RaftEntryKind::CONFIG_FINAL, old, desired, generation)); }
                else { proposeMembershipChange(old, desired, generation); }
                if (retireLeader) {
                    const auto successor = std::ranges::find_if(
                        desired,
                        [this](const auto& node) { return node.nodeId != selfNodeId_ && !node.raftLearner() && node.coordinatorEligible(); }
                    );
                    if (successor == desired.end()) { throw std::invalid_argument("Raft reconfiguration: no eligible successor"); }
                    transferLeadershipAdmitted(successor->nodeId);
                    ClusterRouteTarget destination;
                    destination.nodeId = successor->nodeId;
                    destination.host = successor->host;
                    destination.replPort = successor->replPort;
                    throw ClusterRoutingError(ClusterRoutingCode::NOT_OWNER, destination, "Raft reconfiguration: finalize removal on the successor");
                }
            }
    };

    std::unique_ptr<RaftConsensusRuntime> RaftConsensusRuntime::create(
        std::filesystem::path dbDir,
        ClusterConfig config,
        uint64_t selfNodeId,
        ClusterEngineCallbacks callbacks,
        ClusterRuntimeOptions runtimeOptions,
        std::vector<uint64_t> forwardClients
    ) {
        config.validateRuntime(selfNodeId, runtimeOptions);
        return std::unique_ptr<RaftConsensusRuntime>(
            new RaftConsensusRuntime(std::make_unique<Impl>(std::move(dbDir), std::move(config), selfNodeId, std::move(callbacks), std::move(runtimeOptions), std::move(forwardClients)))
        );
    }
    std::unique_ptr<RaftConsensusRuntime> RaftConsensusRuntime::createRequestClient(ClusterConfig config, NodeInfo self, ClusterRuntimeOptions options) {
        const auto id = self.nodeId;
        return std::unique_ptr<RaftConsensusRuntime>(new RaftConsensusRuntime(std::make_unique<Impl>(
            std::filesystem::path{}, std::move(config), id, ClusterEngineCallbacks{}, std::move(options), std::vector<uint64_t>{}, std::move(self))));
    }
    void RaftConsensusRuntime::setRequestForwardClients(std::vector<uint64_t> nodes) { impl_->setRequestForwardClients(std::move(nodes)); }
    void RaftConsensusRuntime::setRequestForwardTargets(std::vector<NodeInfo> nodes) { impl_->setRequestForwardTargets(std::move(nodes)); }

    RaftConsensusRuntime::RaftConsensusRuntime(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    RaftConsensusRuntime::~RaftConsensusRuntime() = default;

    std::shared_future<ClusterRequestResult> RaftConsensusRuntime::submitRequest(
        const ClusterRequestId& id,
        const std::array<uint8_t, 32>& fingerprint,
        const ClusterMutationFactory& prepare,
        uint64_t expectedTerm
    ) { return impl_->submitRequest(id, fingerprint, prepare, expectedTerm); }

    ClusterRequestResult RaftConsensusRuntime::queryRequest(const ClusterRequestId& id) { return impl_->queryRequest(id); }
    ForwardResponse RaftConsensusRuntime::forwardTo(uint64_t nodeId, ForwardRequest request) { return impl_->forwardTo(nodeId, std::move(request)); }

    void RaftConsensusRuntime::start() { impl_->start(); }
    void RaftConsensusRuntime::reconfigure(const ClusterConfig& config, uint64_t generation) { impl_->reconfigure(config, generation); }

    void RaftConsensusRuntime::close() { impl_->close(); }
    RaftRuntimeStats RaftConsensusRuntime::stats() const { return impl_->stats(); }

    NodeRole RaftConsensusRuntime::role() const noexcept { return impl_->role(); }
    uint64_t RaftConsensusRuntime::leaderNodeId() const { return impl_->leaderNodeId(); }

    std::vector<NodeInfo> RaftConsensusRuntime::activeNodes() const { return impl_->activeNodes(); }

    const ClusterRouter& RaftConsensusRuntime::router() const noexcept { return impl_->router(); }

    void RaftConsensusRuntime::shipEntry(
        uint64_t seq,
        ReplOpType op,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint8_t recordFlags,
        uint64_t sourceNodeId
    ) { impl_->shipEntry(seq, op, key, value, recordFlags, sourceNodeId); }

    ClusterMutationSubmission RaftConsensusRuntime::submitMutation(const ClusterMutationFactory& prepare, uint64_t expectedTerm) {
        return impl_->submitMutation(prepare, expectedTerm);
    }

    std::future<void> RaftConsensusRuntime::submitEntry(
        uint64_t seq,
        ReplOpType op,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint8_t flags,
        uint64_t source
    ) { return impl_->submitEntry(seq, op, key, value, flags, source); }

    void RaftConsensusRuntime::shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) { impl_->shipBlob(seq, blobId, content); }

    uint64_t RaftConsensusRuntime::linearizableReadBarrier() { return impl_->linearizableReadBarrier(); }

    void RaftConsensusRuntime::addVotingNode(const NodeInfo& node) { impl_->addVotingNode(node); }
    void RaftConsensusRuntime::addLearner(const NodeInfo& node) { impl_->addLearner(node); }
    void RaftConsensusRuntime::promoteLearner(uint64_t nodeId) { impl_->promoteLearner(nodeId); }
    void RaftConsensusRuntime::removeLearner(uint64_t nodeId) { impl_->removeLearner(nodeId); }

    void RaftConsensusRuntime::removeVotingNode(uint64_t nodeId) { impl_->removeVotingNode(nodeId); }

    void RaftConsensusRuntime::campaignLeadership() { impl_->campaignLeadership(); }

    void RaftConsensusRuntime::transferLeadership(uint64_t targetNodeId) { impl_->transferLeadership(targetNodeId); }
}
