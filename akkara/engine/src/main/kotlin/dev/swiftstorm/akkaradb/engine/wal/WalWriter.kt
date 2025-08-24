package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Write-Ahead Log writer with a dedicated flusher thread implementing
 * true "group commit (N or T)" and **durable-before-ACK** semantics.
 *
 * Fast mode support:
 *  - beginFastMode(): delay fsync until the next barrier
 *  - endFastMode():   ensure durability at the next barrier, then return to durable mode
 *
 * Thread safety: public APIs are safe to call from multiple threads.
 * Encoding: fixed-length LE as defined in WalRecord.
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
    }

    private val ch = FileChannel.open(path, WRITE, CREATE, APPEND)

    private var scratch: ByteBuffer = pool.get(initCap)

    /* ------------ command queue (writes + control barriers) ------------ */

    private sealed interface Cmd {
        val done: CompletableFuture<Void>
    }
    private data class Write(val rec: WalRecord, override val done: CompletableFuture<Void>) : Cmd
    private data class Barrier(override val done: CompletableFuture<Void>) : Cmd

    private val q = LinkedBlockingQueue<Cmd>()
    private val running = AtomicBoolean(true)
    private val flusher = Thread(this::flushLoop, "WalFlusher").apply {
        isDaemon = true; start()
    }

    /* ------------------------ Fast/Durable switch ----------------------- */

    /**
     * Fast mode: fsync is delayed until the next barrier.
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

    /* --------------------------- public API --------------------------- */

    /**
     * Append an Akk-encoded KV payload.
     * @param durable true: wait until enqueued writes reach durability (Durable-ACK)
     *                false: return after enqueue (Fast-ACK; durability is deferred to a barrier)
     */
    fun append(record: ByteBuffer, durable: Boolean = true) {
        val payload: ByteBuffer =
            if (durable) {
                record.duplicate().apply { rewind() }.asReadOnlyBuffer()
            } else {
                val src = record.duplicate().apply { rewind() }.order(ByteOrder.LITTLE_ENDIAN)
                val owned = ByteBuffer.allocate(src.remaining())
                owned.put(src)
                owned.flip()
                owned.asReadOnlyBuffer()
            }

        val f = enqueueWrite(WalRecord.Add(payload))
        if (durable) f.join()
    }

    /** Mark end of segment; write a real Seal and ensure durability regardless of mode. */
    fun sealSegment() {
        enqueueWrite(WalRecord.Seal).join()
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
        val f1 = enqueueWrite(WalRecord.Seal)
        val f2 = enqueueWrite(WalRecord.CheckPoint(stripeIdx, seqNo))
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

    private fun enqueueWrite(r: WalRecord): CompletableFuture<Void> {
        val est = r.estimateSize()
        require(est <= MAX_RECORD_BYTES) { "WAL record too large: $est bytes" }

        val f = CompletableFuture<Void>()
        if (!running.get()) {
            f.completeExceptionally(IllegalStateException("WAL writer is closed"))
            return f
        }
        try {
            q.put(Write(r, f))
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
            q.put(Barrier(f))
        } catch (ie: InterruptedException) {
            f.completeExceptionally(ie)
            Thread.currentThread().interrupt()
        }
        return f
    }

    private fun flushLoop() {
        val batch = ArrayList<Write>(groupCommitN)

        fun flushBatch() {
            if (batch.isEmpty()) return
            try {
                writeBatch(batch)
                if (forceDurable.get()) ch.force(false)
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
                val first = q.poll(groupCommitMicros, TimeUnit.MICROSECONDS) ?: continue
                when (first) {
                    is Barrier -> {
                        // close any open batch first
                        flushBatch()
                        try {
                            ch.force(false)
                            first.done.complete(null)
                        } catch (t: Throwable) {
                            first.done.completeExceptionally(t)
                        }
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
                            try {
                                ch.force(false)
                                next.done.complete(null)
                            } catch (t: Throwable) {
                                next.done.completeExceptionally(t)
                            }
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
                        ch.force(false)
                        cmd.done.complete(null)
                    } catch (t: Throwable) {
                        cmd.done.completeExceptionally(t)
                    }
                }

                is Barrier -> {
                    try {
                        ch.force(false)
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
        for (p in batch) total += p.rec.estimateSize()

        if (scratch.capacity() < total) {
            pool.release(scratch)
            var newCap = scratch.capacity()
            while (newCap < total) newCap = newCap * 2
            scratch = pool.get(newCap)
        }

        scratch.clear()
        for (p in batch) {
            p.rec.writeTo(scratch)
        }
        scratch.flip()

        while (scratch.hasRemaining()) {
            ch.write(scratch)
        }
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
        is WalRecord.Seal -> 1                    // [tag]
        is WalRecord.CheckPoint -> 1 + 8 + 8      // [tag][u64][u64]
        is WalRecord.Add -> 1 + 4 + payload.remaining() + 4 // [tag][u32][payload][crc]
    }