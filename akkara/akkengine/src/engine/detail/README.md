# AkkEngine implementation boundaries

`AkkEngine.cpp` contains the public low-level API entry points. It checks whether
the engine exists and delegates to the private runtime. Public declarations,
object layout, exception behavior, and storage formats remain unchanged.

`AkkEngineImpl.hpp` composes the private runtime state and implementation
fragments. The runtime owns operation admission, storage pins, visibility locks,
write coordination, background work, and shutdown. Public `const` read APIs may
delegate to non-const runtime operations because reads also update statistics,
promote records, and manage cluster cursors.

The separately compiled operation sources are organized by responsibility:

| Source | Responsibility |
| --- | --- |
| `AkkEngineLifecycle.cpp` | Construction, maintenance entry points, and ordered shutdown |
| `AkkEngineStorageRecovery.cpp` | Storage initialization, flush callbacks, and recovery |
| `AkkEngineClusterBootstrap.cpp` | Cluster callbacks and runtime startup |
| `AkkEngineRead.cpp` | Point, batch, range, and history reads |
| `AkkEngineWrite.cpp` | Point and protocol bulk writes |
| `AkkEngineRequests.cpp` | Retry-safe request operations |
| `AkkEngineRollback.cpp` | Checkpoints, rollback planning, and rollback result helpers |
| `AkkEngineCluster.cpp` | Membership, leadership, and cluster administration |
| `AkkEngineTransactions.cpp` | Transaction sessions and transaction entry operations |
| `AkkEngineStats.cpp` | Statistics collection |

The `AkkEngine*.inc` fragments group runtime implementation by responsibility,
following the existing transaction, query, placement, and Raft implementation
pattern. They are included by private headers and are not independent translation
units. Keep state ownership in the runtime rather than adding forwarding state
to the public object.

`AkkEngineSupport.hpp` provides shared implementation helpers in the named
`detail::runtime` namespace. Header-defined helpers are inline so their types,
constants, and local static state remain consistent across operation sources.

Preserve operation admission and storage/visibility lock ordering when moving
work between these files. Startup must finish recovery before admitting ordinary
operations; shutdown must drain admitted work and queued flushes before releasing
their storage dependencies.
