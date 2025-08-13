package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.locks.LockSupport

/**
 * Write‑Ahead Log writer with **group commit (N or T)**.
 *
 * Semantics:
 *  • `append()` returns only after the WAL bytes are **durably** on disk.
 *    Durability is achieved by `FileChannel.force(true)` executed on the
 *    current group when either:
 *      – the group has accumulated ≥ [groupCommitN] appends, or
 *      – the oldest record in the group has waited ≥ [groupCommitMicros].
 *  • `sealSegment()` / `checkpoint(...)` always force immediately.
 *
 * Thread‑safety: not thread‑safe (use from a single writer thread).
 */
class WalWriter(
    path: Path,
    private val pool: BufferPool = Pools.io(),
    initCap: Int = 32 * 1024,
    private val groupCommitN: Int = 32,
    private val groupCommitMicros: Long = 500
) : Closeable {

    companion object {
        private const val MAX_RECORD_BYTES = 1 shl 20 // 1 MiB
        private const val NANOS_PER_MICRO: Long = 1_000
    }

    private val ch = FileChannel.open(path, WRITE, CREATE, APPEND)

    /** Reusable scratch buffer (direct). */
    private var scratch: ByteBuffer = pool.get(initCap)

    // ---- group commit state ----
    private var groupCount: Int = 0
    private var groupStartNanos: Long = 0L

    /* ---------- public API ---------- */

    /** Append a KV payload (already Akk-encoded) and durably commit per group policy. */
    fun append(record: ByteBuffer) =
        writeRecord(WalRecord.Add(record.asReadOnlyBuffer()), forceNow = false)

    /** Mark end of segment; forces immediately. */
    fun sealSegment() =
        writeRecord(WalRecord.Seal, forceNow = true)

    /** Write a checkpoint {stripeIdx, seqNo}; forces immediately. */
    fun checkpoint(stripeIdx: Long, seqNo: Long) =
        writeRecord(WalRecord.CheckPoint(stripeIdx, seqNo), forceNow = true)

    /* ---------- core ---------- */

    private fun writeRecord(r: WalRecord, forceNow: Boolean) {
        val estimated = r.estimateSize()
        require(estimated <= MAX_RECORD_BYTES) {
            "WAL record too large: $estimated bytes (limit ${MAX_RECORD_BYTES}B)"
        }

        // ensure scratch capacity
        if (scratch.capacity() < estimated) {
            pool.release(scratch)
            var newCap = scratch.capacity()
            while (newCap < estimated) newCap = newCap * 2
            scratch = pool.get(newCap)
        }

        // encode + write
        scratch.clear()
        r.writeTo(scratch)
        scratch.flip()
        while (scratch.hasRemaining()) ch.write(scratch)

        if (forceNow) {
            // immediate durability barrier (and reset group)
            ch.force(true)
            groupCount = 0
            groupStartNanos = 0L
            return
        }

        // group commit path: N or T µs from first record in group
        val now = System.nanoTime()
        if (groupCount == 0) groupStartNanos = now
        groupCount++

        val dueByCount = groupCount >= groupCommitN
        val dueByTime = (now - groupStartNanos) >= groupCommitMicros * NANOS_PER_MICRO

        if (dueByCount || dueByTime) {
            ch.force(true)
            groupCount = 0
            groupStartNanos = 0L
        } else {
            // Bound the ACK latency by waiting until the time window elapses,
            // then force() the group so the caller observes durability.
            val nanosLeft = groupCommitMicros * NANOS_PER_MICRO - (now - groupStartNanos)
            if (nanosLeft > 0) LockSupport.parkNanos(nanosLeft)
            ch.force(true)
            groupCount = 0
            groupStartNanos = 0L
        }
    }

    /* ---------- lifecycle ---------- */

    override fun close() {
        // If a group is open, force it before closing to avoid silent loss
        if (groupCount > 0) ch.force(true)
        pool.release(scratch)
        ch.close()
    }
}

private fun WalRecord.estimateSize(): Int =
    when (this) {
        is WalRecord.Seal -> 1 // tag only
        is WalRecord.CheckPoint -> 1 + 10 + 10 // tag + VarLong×2
        is WalRecord.Add -> 1 + 5 + payload.remaining() // tag + VarInt(len) + data
    }
