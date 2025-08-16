package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Write‑Ahead Log writer with a dedicated flusher thread implementing
 * true "group commit (N or T)" and **durable-before-ACK** semantics.
 *
 * Key points:
 *  - Callers enqueue records; a flusher writes them and calls force(true) once per group.
 *  - No fake Seal is written for barriers. We use an in‑memory Barrier command
 *    so WalReplay's state machine (Seal -> CheckPoint) stays valid.
 *  - sealSegment(): enqueues a real Seal record and waits for durability.
 *  - checkpoint(): enqueues {Seal, CheckPoint} in order and waits.
 *
 * Thread safety: public APIs are safe to call from multiple threads.
 * Encoding: fixed‑length LE as defined in WalRecord.
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

    /** Scratch buffer for encoding a single record (owned by flusher thread). */
    private var scratch: ByteBuffer = pool.get(initCap)

    private val writeLock = Any()

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

    /* --------------------------- public API --------------------------- */

    /** Append an Akk‑encoded KV payload and wait until it is durably on disk. */
    fun append(record: ByteBuffer) {
        synchronized(writeLock) {
            enqueueWrite(WalRecord.Add(record.asReadOnlyBuffer())).join()
        }
    }

    /** Mark end of segment; write a real Seal and ensure durability. */
    fun sealSegment() {
        synchronized(writeLock) {
            enqueueWrite(WalRecord.Seal).join()
        }
    }

    /**
     * Write a checkpoint {stripeIdx, seqNo} with the required ordering:
     * real Seal followed by CheckPoint, then force durability.
     */
    fun checkpoint(stripeIdx: Long, seqNo: Long) {
        synchronized(writeLock) {
            barrier().join()
            val f1 = enqueueWrite(WalRecord.Seal)
            val f2 = enqueueWrite(WalRecord.CheckPoint(stripeIdx, seqNo))
            CompletableFuture.allOf(f1, f2).join()
        }
    }

    /** Truncate WAL after a successful checkpoint. */
    fun truncate() {
        synchronized(writeLock) {
            barrier().join()
            ch.truncate(0)
            ch.force(true)
            ch.position(0)
        }
    }

    /* --------------------------- internals ---------------------------- */

    private fun enqueueWrite(r: WalRecord): CompletableFuture<Void> {
        val est = r.estimateSize()
        require(est <= MAX_RECORD_BYTES) { "WAL record too large: $est bytes" }
        return CompletableFuture<Void>().also { q.put(Write(r, it)) }
    }

    /** Inserts a control barrier (no on‑disk record) and waits for fsync. */
    private fun barrier(): CompletableFuture<Void> =
        CompletableFuture<Void>().also { q.put(Barrier(it)) }

    private fun flushLoop() {
        val batch = ArrayList<Write>(groupCommitN)
        fun flushBatch() {
            if (batch.isEmpty()) return
            writeBatch(batch)
            ch.force(true)
            batch.forEach { it.done.complete(null) }
            batch.clear()
        }

        // Main loop
        while (running.get() || !q.isEmpty()) {
            try {
                // wait up to T µs for first command
                val first = q.poll(groupCommitMicros, TimeUnit.MICROSECONDS) ?: continue
                when (first) {
                    is Barrier -> {              // flush what we have (if any), then ack barrier
                        flushBatch()
                        first.done.complete(null)
                        continue
                    }

                    is Write -> batch += first
                }

                // drain more until N or a Barrier arrives (without blocking)
                while (batch.size < groupCommitN) {
                    val next = q.poll() ?: break
                    when (next) {
                        is Write -> batch += next
                        is Barrier -> {
                            // flush everything before the barrier, then ack it
                            flushBatch()
                            next.done.complete(null)
                        }
                    }
                }

                // time boundary or count boundary reached → flush once
                flushBatch()
            } catch (t: Throwable) {
                // complete exceptionally and keep running (best-effort resiliency)
                while (true) {
                    val c = batch.removeLastOrNull() ?: break
                    c.done.completeExceptionally(t)
                }
            }
        }

        // Drain remaining writes on shutdown
        while (q.poll()?.let {
                when (it) {
                    is Write -> {
                        writeBatch(listOf(it)); ch.force(true); it.done.complete(null); true
                    }

                    is Barrier -> {
                        ch.force(true); it.done.complete(null); true
                    }
                }
            } == true) { /* keep draining */
        }
    }

    private fun writeBatch(batch: List<Write>) {
        for (p in batch) {
            val est = p.rec.estimateSize()
            if (scratch.capacity() < est) {
                pool.release(scratch)
                var newCap = scratch.capacity()
                while (newCap < est) newCap = newCap * 2
                scratch = pool.get(newCap)
            }
            scratch.clear()
            p.rec.writeTo(scratch)
            scratch.flip()
            while (scratch.hasRemaining()) ch.write(scratch)
        }
    }

    /* --------------------------- lifecycle ---------------------------- */

    override fun close() {
        running.set(false)
        flusher.join()
        // final fsync for safety
        ch.force(true)
        pool.release(scratch)
        ch.close()
    }
}

/* -------- size estimator aligned with the current WalRecord encoding -------- */
private fun WalRecord.estimateSize(): Int =
    when (this) {
        is WalRecord.Seal -> 1                    // [tag]
        is WalRecord.CheckPoint -> 1 + 8 + 8      // [tag][u64][u64]
        is WalRecord.Add -> 1 + 4 + payload.remaining() + 4 // [tag][u32][payload][crc]
    }