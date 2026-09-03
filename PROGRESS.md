# Progress

## VersionLog

Status: production-ready candidate

Overall progress: 90%

Completed:

- Rejected append-after-close before commit visibility can advance.
- Checked write durability failures across `fflush`, `fdatasync` / `_commit`, and write-handle close paths.
- Replaced tail and sidecar publication with atomic file replacement instead of remove-then-rename.
- Made the VLog binary format explicitly little-endian for file headers, entries, indexes, and durable tails.
- Removed packed-struct disk decoding from the hot format paths in favor of explicit encode/decode helpers.
- Added real `createdNs` timestamps for new segment headers.
- Split the large `VersionLog.cpp` implementation into focused `detail/*.hpp` units.
- Added serial ASYNC crash recovery for interrupted active-segment tails, including valid-prefix truncation.
- Kept immutable/closed segment scans strict during read fallback, rollback collection, and retention compaction.
- Repaired crash-time partial active segment headers after rotation before accepting new appends.
- Propagated parallel worker persistence failures through explicit `close()`.
- Serialized async error publication so concurrent lane failures cannot race on `exception_ptr`.
- Renamed the internal live byte counter to `knownWrittenBytes_` while preserving the public `durableBytes` stats field.
- Added VLog smoke coverage for recovery, concurrency, sidecar fallback, and fault-injection crash boundaries.

Verified:

- `akkaradb_version_log_fault_injection_smoke_test.exe`
- `akkaradb_version_log_recovery_concurrency_smoke_test.exe`
- `akkaradb_version_log_admission_visibility_smoke_test.exe`
- `akkaradb_version_log_tool_smoke_test.exe`
- `git diff --check`

Remaining before calling it fully production-ready:

- Add true OS/file-system fault injection for ENOSPC, write failure, flush failure, sync failure, close failure, and atomic replace failure.
- Add long-running soak tests for parallel append/read/forceSync/close under rotation and retention.
- Add crash matrix coverage around tail temp write, tail replace, index replace, parent directory sync, and segment rotation windows.
- Decide whether the public stats field should remain `durableBytes` long-term or be renamed in a breaking API cleanup.
- Document operational recovery expectations for `.akvlog`, `.akvtail`, and `.akvidx` together with backup/restore guidance.

Later split candidates:

- Move fault-injection helpers into a reusable VLog test utility once more persisted-storage tests need the same byte-level corruption tools.
- Split crash-boundary tests by area if the single fault-injection smoke grows too large: tail publication, segment rotation, sidecar index, and retention.
- Add a dedicated durability-fault harness behind a test-only build flag if production code needs injectable file operations.

## Cluster

Status: pre-release architecture candidate

Overall progress: 88%

Completed:

- Split non-Raft and Raft responsibilities clearly at the runtime boundary.
- Kept `RAFT_QUORUM` on the dedicated Raft consensus runtime path for leader election, quorum commit, membership changes, and leader transfer.
- Defined `PRIMARY_ACK` and `ASYNC` as primary-owned replication group modes rather than self-electing consensus clusters.
- Added non-Raft group identity to replication handshakes through `groupId` and `groupEpoch`.
- Changed non-Raft `groupId` from config-derived identity to primary-created group instance identity.
- Made non-Raft PRIMARY startup load or create persisted group state in `cluster.membership`.
- Made non-Raft REPLICA startup persist joined group membership and reject implicit switches to a different group, primary, or epoch.
- Added explicit `resetClusterMembership` override for intentional replica rejoin or failover recovery.
- Added primary-side filtering for configured data-bearing replica node ids when the server is created through `ClusterRuntime`.
- Rejected duplicate live replica connections for the same node id on a primary.
- Preserved direct low-level `ReplicationServer::create` usage by leaving membership filtering disabled when no configured replica ids are supplied.
- Added non-Raft per-replica outbound queue frame/byte limits and fail/lag handling when a replica cannot accept catch-up or live frames.
- Replaced non-Raft `cluster.membership` text state with CRC32C-protected `AKCG2` little-endian binary state.
- Replaced Raft state/log persistence with `AKRS2` and `AKRL1` little-endian binary formats; Raft log entries now carry per-record CRC32C for optional uncommitted-tail truncation, and the log header persists applied progress to avoid replaying already durable committed entries.
- Added explicit corrupt-state and Raft-log recovery policies to `ClusterRuntimeOptions`, defaulting to fail-fast startup rejection.
- Tightened `ClusterConfig` binary loading by rejecting oversized host names and trailing bytes.
- Enabled non-Raft native `PARTITIONED` and `STRIPE` runtime startup with data-bearing nodes acting as per-key owners in the same cluster group. Writes are fixed owner-only and reject on non-owner nodes; remote owner/shard entries are remapped onto local storage sequences on receipt.
- Added `ClusterRuntimeOptions::readMode` with `LOCAL_STALE_OK`, `OWNER_ONLY`, and `OWNER_LINEARIZABLE`; `OWNER_LINEARIZABLE` sends non-Raft reads to the current key owner and makes Raft leaders commit a quorum read barrier before serving the local read.
- Added active native `STRIPE` value storage using internal metadata plus RS data/parity shards. `stripeWriteCommitMode` chooses all-shard versus data-shard commit, `stripeReadCoordinatorMode` chooses owner-routed versus local-coordinator shard assembly, and `stripeReadRepair` repairs missing shards best-effort after successful degraded reads.
- Added engine-level STRIPE reconfiguration/rebalance through `AkkEngine::reconfigureCluster`, rewriting still-readable old-owner values under the new shard placement.
- Removed expired-manifest-lease auto-promotion in non-Raft mode and reject explicit PRIMARY startup while a valid foreign primary lease exists.
- Made engine-level non-Raft native cluster startup require Blob storage to be disabled; engine Blob replication is kept on the Raft policy path.
- Made non-Raft `FAIL_WRITE` writes commit and sync the primary-local state before waiting for replica acknowledgements, so acknowledgement failure cannot leave replicas ahead of an unrecorded primary write.
- Removed engine-level Raft leader double-apply by relying on the cluster Raft runtime apply callback for state-machine mutation application and keeping the engine-side Raft log commit marker/stat accounting separate.
- Made `RAFT_QUORUM` `activeNodes()` report the committed Raft membership view, using old/new voter union during joint consensus and stable node-id ordering.
- Exposed engine-level Raft voter add/remove and leader-transfer management calls.
- Made `RAFT_QUORUM` reject Blob payload replication by default through `raftBlobPolicy=REJECT`; explicit `PRIMARY_SIDE_ONLY` allows primary-local Blob payloads outside Raft quorum and `RAFT_LOG` automatically chunk-commits Blob payload entries before Blob-reference mutations. `RAFT_LOG` and snapshot Blob refs use reserved Blob-id namespaces to avoid leader-churn and multi-entry snapshot collisions, and Raft snapshot installation uses the same chunk boundary while streaming completed entries into CRC32C-protected receiver staging instead of buffering the full snapshot in memory. Snapshot finish externalizes large staged values before writing WAL snapshot record/commit markers, so recovery ignores uncommitted snapshot batches and replays committed batches atomically.
- Added smoke coverage for handshake group identity roundtrip, implicit group switch rejection, explicit reset-based rejoin, multiple primary-created group instances from the same config, corrupt primary `cluster.membership` rejection, primary group state reuse after restart, active `STRIPE` runtime startup, partitioned owner-only replication, partitioned engine `OWNER_LINEARIZABLE` reads, STRIPE owner-only writes, owner/local-coordinator reads, data-shard commit with a missing parity node, STRIPE reconfigure/rebalance readability, engine rejection of unsafe native cluster configurations, non-Raft replica queue-limit catch-up rejection, and `ClusterRuntime` ACK quorum rejection for unknown or duplicate replica node ids.
- Added smoke coverage for corrupt membership backup-and-recreate, Raft hard-state corruption fail-fast, no replay of already applied Raft entries after restart, Raft log trailing-byte fail-fast/truncate recovery policies, WAL snapshot transaction recovery, Raft active membership reporting across online voter add/remove and restart-after-compaction, chunked snapshot installation, default Raft Blob rejection, explicit primary-side-only Raft Blob handling, chunked Raft-log Blob replication/recovery, engine-level Raft Blob externalization, engine-level leadership transfer, engine E2E large-Blob snapshot catch-up, snapshot staging CRC corruption rejection/retry, secure transport pin enforcement, and large-Blob writes across leader churn. Raft smoke enables deterministic node-id-ordered election timeouts and leader-sensitive writes/membership changes use retrying single-leader discovery instead of stale role snapshots.

Verified:

- `cmake --build . --target akkaradb_cluster_smoke_test --config Debug`
- `builds/debug/bin/akkaradb_cluster_smoke_test.exe`
- Cluster-related `git diff --check`

Remaining before calling it fully production-ready:

- Make the non-Raft group state filename distinct from replica membership state if the shared name becomes confusing operationally.
- Decide the public recovery UX for `resetClusterMembership`, including CLI/JNI/API exposure and explicit warnings.
- Add durable per-owner replication progress for `PARTITIONED`/`STRIPE` restart catch-up; the current owner/target sequence watermark is kept in runtime memory.
- Add broader STRIPE failure-matrix coverage around multiple missing shards, tombstone rebalance, and repeated membership churn.

Later split candidates:

- Move non-Raft group state and membership persistence helpers out of `ClusterRuntime.cpp` and `ReplicationClient.cpp` once the format settles.
- Consider a small explicit `ClusterGroupState` format/version helper if the same state is exposed to CLI, JNI, or recovery tooling.
- Add an external-lock primary claim backend only if a real deployment target needs coordinated non-Raft primary selection.
