package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.io.Closeable
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max

/**
 * Write-Ahead Log writer with a dedicated flusher thread implementing
 * true "group commit (N or T)" and **durable-before-ACK** semantics.
 *
 * Fast mode support:
 *  - beginFastMode(): delay fsync until the next barrier
 *  - endFastMode():   ensure durability at the next barrier, then return to durable mode
 *
 * Extended fast mode (background force):
 *  - Even in fast mode, periodically forces (fsync) by time or bytes thresholds
 *    to cap data-loss window and avoid long exit latency.
 *
 * Thread safety: public APIs are safe to call from multiple threads.
 * Encoding: fixed-length LE as defined in WalRecord.
 */
class WalWriter(
    path: Path,
    private val pool: BufferPool,
    initCap: Int,
    private val groupCommitN: Int,
    private val groupCommitMicros: Long,
    private val fastForceMicros: Long,      // force at least every 5ms when active/idle
    private val fastForceBytes: Long    // force every ~1MiB written
) : Closeable {

    companion object {
        private const val MAX_RECORD_BYTES = 1 shl 20 // 1 MiB
    }

    private val ch = FileChannel.open(path, WRITE, CREATE, APPEND)

    private var scratch: ByteBufferL = pool.get(initCap)

    /* ------------ command queue (writes + control barriers) ------------ */

    private sealed interface Cmd {
        val done: CompletableFuture<Void>
    }

    private data class Write(
        val rec: WalRecord,
        val durable: Boolean,
        override val done: CompletableFuture<Void>,
        val lsn: Long
    ) : Cmd

    private data class Barrier(
        override val done: CompletableFuture<Void>,
        val captureLsn: Long
    ) : Cmd

    private val q = LinkedBlockingQueue<Cmd>()
    private val running = AtomicBoolean(true)
    private val flusher = Thread(this::flushLoop, "WalFlusher").apply {
        isDaemon = true; start()
    }

    /* ------------------------ Fast/Durable switch ----------------------- */

    /**
     * Fast mode: fsync is delayed until background/threshold force or next barrier.
     * Durable mode: fsync after every batch (default).
     */
    val forceDurable = AtomicBoolean(true)

    fun beginFastMode() {
        forceDurable.set(false)
    }

    fun endFastMode() {
        // Ensure all prior writes reach durable storage before returning to durable mode
        barrier().join()
        forceDurable.set(true)
    }

    /* --------------------------- LSN & background force --------------------------- */

    private val nextLsn = AtomicLong(1L)
    @Volatile
    private var lastFlushedLsn: Long = 0L
    @Volatile
    private var lastForcedLsn: Long = 0L

    @Volatile
    private var bytesSinceLastForce: Long = 0L
    @Volatile
    private var lastForceNano: Long = System.nanoTime()

    private fun needBackgroundForce(nowNano: Long): Boolean {
        if (bytesSinceLastForce >= fastForceBytes) return true
        val elapsed = nowNano - lastForceNano
        return elapsed >= TimeUnit.MICROSECONDS.toNanos(fastForceMicros)
    }

    private fun markForced() {
        lastForcedLsn = lastFlushedLsn
        bytesSinceLastForce = 0L
        lastForceNano = System.nanoTime()
    }

    /* --------------------------- public API --------------------------- */

    /**
     * Append an Akk-encoded KV payload.
     * @param durable true: wait until enqueued writes reach durability (Durable-ACK)
     *                false: return after enqueue (Fast-ACK; durability is deferred to bg force/barrier)
     */
    fun append(record: ByteBufferL, durable: Boolean = true) {
        val payload: ByteBufferL =
            if (durable) {
                record.duplicate().apply { rewind() }.asReadOnly()
            } else {
                val src = record.duplicate().apply { rewind() }
                val owned = ByteBufferL.allocate(src.remaining)
                owned.put(src.asReadOnlyByteBuffer()).flip().asReadOnly()
            }

        val f = enqueueWrite(WalRecord.Add(payload), durable)
        if (durable) f.join()
    }

    /** Mark end of segment; write a real Seal and ensure durability regardless of mode. */
    fun sealSegment() {
        enqueueWrite(WalRecord.Seal, durable = false).join()
        barrier().join() // ensure durable even in Fast mode
    }

    /**
     * Write a checkpoint {stripeIdx, seqNo} with strict ordering:
     *  (1) barrier (durable up to now)
     *  (2) Seal + CheckPoint
     *  (3) barrier (durable including Seal+CP)
     */
    fun checkpoint(stripeIdx: Long, seqNo: Long) {
        barrier().join()
        val f1 = enqueueWrite(WalRecord.Seal, durable = false)
        val f2 = enqueueWrite(WalRecord.CheckPoint(stripeIdx, seqNo), durable = false)
        CompletableFuture.allOf(f1, f2).join()
        barrier().join()
    }

    /** Truncate WAL after a successful checkpoint. */
    fun truncate() {
        barrier().join()
        ch.truncate(0)
        ch.force(false)
        ch.position(0)
    }

    /* --------------------------- internals ---------------------------- */

    private fun enqueueWrite(r: WalRecord, durable: Boolean): CompletableFuture<Void> {
        val est = r.estimateSize()
        require(est <= MAX_RECORD_BYTES) { "WAL record too large: $est bytes" }

        val f = CompletableFuture<Void>()
        if (!running.get()) {
            f.completeExceptionally(IllegalStateException("WAL writer is closed"))
            return f
        }
        try {
            val lsn = nextLsn.getAndIncrement()
            q.put(Write(r, durable, f, lsn))
        } catch (ie: InterruptedException) {
            f.completeExceptionally(ie)
            Thread.currentThread().interrupt()
        }
        return f
    }

    /** Inserts a control barrier (no on-disk record) and waits for fsync at the barrier point. */
    private fun barrier(): CompletableFuture<Void> {
        val f = CompletableFuture<Void>()
        if (!running.get()) {
            f.completeExceptionally(IllegalStateException("WAL writer is closed"))
            return f
        }
        try {
            val cap = nextLsn.get() - 1 // capture current high-water LSN
            q.put(Barrier(f, cap))
        } catch (ie: InterruptedException) {
            f.completeExceptionally(ie)
            Thread.currentThread().interrupt()
        }
        return f
    }

    private fun flushLoop() {
        val batch = ArrayList<Write>(groupCommitN)

        fun doForce() {
            ch.force(false)
            markForced()
        }

        fun flushBatch() {
            if (batch.isEmpty()) return
            try {
                writeBatch(batch)
                val needForceDurable = forceDurable.get() || batch.any { it.durable }
                val needForceBackground = !forceDurable.get() && needBackgroundForce(System.nanoTime())
                if (needForceDurable || needForceBackground) {
                    doForce()
                }
                batch.forEach { it.done.complete(null) }
            } catch (t: Throwable) {
                batch.forEach { it.done.completeExceptionally(t) }
            } finally {
                batch.clear()
            }
        }

        // Main loop
        while (running.get() || !q.isEmpty()) {
            try {
                // wait up to T µs for first command (start of a batch window)
                val first = q.poll(groupCommitMicros, TimeUnit.MICROSECONDS)
                if (first == null) {
                    // idle tick: in fast mode we still enforce time-threshold forcing
                    if (!forceDurable.get() && bytesSinceLastForce > 0 && needBackgroundForce(System.nanoTime())) {
                        doForce()
                    }
                    continue
                }
                when (first) {
                    is Barrier -> {
                        // close any open batch first
                        flushBatch()
                        // honor barrier: ensure durability up to captured LSN
                        if (lastForcedLsn < first.captureLsn || !forceDurable.get()) {
                            doForce()
                        }
                        first.done.complete(null)
                        continue
                    }
                    is Write -> batch += first
                }

                // From the first write, wait until deadline (T) while filling up to N
                val deadlineNanos =
                    System.nanoTime() + TimeUnit.MICROSECONDS.toNanos(groupCommitMicros)

                while (batch.size < groupCommitN) {
                    val remain = deadlineNanos - System.nanoTime()
                    if (remain <= 0) break
                    val next = q.poll(remain, TimeUnit.NANOSECONDS) ?: break
                    when (next) {
                        is Write -> batch += next
                        is Barrier -> {
                            // finalize current batch before honoring the barrier
                            flushBatch()
                            if (lastForcedLsn < next.captureLsn || !forceDurable.get()) {
                                doForce()
                            }
                            next.done.complete(null)
                            // barrier defines a hard boundary; end this cycle
                            break
                        }
                    }
                }

                // time boundary or count boundary reached → flush once
                flushBatch()
            } catch (t: Throwable) {
                // As a last resort, fail any collected but unflushed writes
                if (batch.isNotEmpty()) {
                    batch.forEach { it.done.completeExceptionally(t) }
                    batch.clear()
                }
                // keep running (best-effort resiliency)
            }
        }

        // Drain remaining commands on shutdown
        while (true) {
            val cmd = q.poll() ?: break
            when (cmd) {
                is Write -> {
                    try {
                        writeBatch(listOf(cmd))
                        doForce()
                        cmd.done.complete(null)
                    } catch (t: Throwable) {
                        cmd.done.completeExceptionally(t)
                    }
                }

                is Barrier -> {
                    try {
                        doForce()
                        cmd.done.complete(null)
                    } catch (t: Throwable) {
                        cmd.done.completeExceptionally(t)
                    }
                }
            }
        }
    }

    private fun writeBatch(batch: List<Write>) {
        var total = 0
        var maxLsn = 0L
        for (p in batch) {
            total += p.rec.estimateSize()
            maxLsn = max(maxLsn, p.lsn)
        }

        if (scratch.capacity < total) {
            pool.release(scratch)
            var newCap = scratch.capacity
            while (newCap < total) newCap = newCap * 2
            scratch = pool.get(newCap)
        }

        scratch.clear()
        for (p in batch) {
            p.rec.writeTo(scratch)
        }
        scratch.flip()

        run {
            val bb = scratch.toMutableByteBuffer()
            var wrote = 0
            while (bb.hasRemaining()) {
                val n = ch.write(bb)
                if (n <= 0) {
                    Thread.onSpinWait()
                    continue
                }
                wrote += n
            }
            if (wrote > 0) {
                scratch.advance(wrote) // pos -> limit
                bytesSinceLastForce += wrote
            }
        }

        lastFlushedLsn = max(lastFlushedLsn, maxLsn)
    }

    /* --------------------------- lifecycle ---------------------------- */

    override fun close() {
        // signal stop and make sure flusher reaches a barrier and exits
        running.set(false)
        try {
            // Wake flusher and force it to close the current window cleanly
            barrier().join()
        } catch (_: Throwable) {
            // ignore; we'll still attempt to join and close
        }
        flusher.join()

        // final fsync for safety
        try {
            ch.force(false)
        } finally {
            pool.release(scratch)
            ch.close()
        }
    }
}

/* -------- size estimator aligned with the current WalRecord encoding -------- */
private fun WalRecord.estimateSize(): Int =
    when (this) {
        is WalRecord.Seal -> 1                            // [tag]
        is WalRecord.CheckPoint -> 1 + 8 + 8              // [tag][u64][u64]
        is WalRecord.Add -> 1 + 4 + payload.remaining + 4 // [tag][u32][payload][crc]
    }
