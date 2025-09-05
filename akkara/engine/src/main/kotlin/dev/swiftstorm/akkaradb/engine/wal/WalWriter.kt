package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.engine.wal.WalReplay.SEG_REGEX
import java.io.Closeable
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport
import kotlin.concurrent.thread
import kotlin.io.path.name
import kotlin.io.path.notExists

/**
 * Segmented WAL writer with explicit Seal → rotate, CheckPoint, and prune.
 *
 * Modes:
 *  - Durable mode (fastMode=false): fsync after every append (Durable-before-ACK).
 *  - Fast mode (fastMode=true): append enqueues write, fsync performed by flusher thread
 *    on time/size thresholds (ACK-before-fsync).
 *
 * fsync tuning:
 *  - If both fsyncBatchN and fsyncIntervalMicros are null (closeOnlyFsync), fsync is executed only at close().
 */
class WalWriter(
    private val dir: Path,
    private val filePrefix: String,
    private val enableLog: Boolean,
    private val fastMode: Boolean,
    private val fsyncBatchN: Int?,           // fastMode: batch size (null => closeOnlyFsync)
    private val fsyncIntervalMicros: Long?,  // fastMode: interval in µs (null => closeOnlyFsync)
    private val queueCapacity: Int,   // fastMode: op queue capacity (backpressure)
    private val backoffNanos: Long // append-side retry backoff when queue is full (1ms)
) : Closeable {

    private object Tag {
        const val ADD: Byte = 0x01
        const val SEAL: Byte = 0x02
        const val CHECKPOINT: Byte = 0x03
    }

    private object Hdr {
        const val TAG: Int = 1
        const val LSN: Int = 8
        const val LEN: Int = 4
        const val CKPT_BODY: Int = 16 // stripesWritten(8) + lastSeq(8)
    }

    private val running = AtomicBoolean(true)
    private val nextLsn = AtomicLong(0)

    private var currentSegIdx: Int
    private var lastCheckpointSegIdx: Int? = null

    private var ch: FileChannel
    private var segPath: Path

    private val writeLock = Any()
    private val io = Pools.io()

    // bounded queue to provide gentle backpressure on bursts
    private val queue = if (fastMode) LinkedBlockingQueue<() -> Unit>(queueCapacity) else null
    private val flusherThread: Thread?
    private val closeOnlyFsync = fastMode && fsyncBatchN == null && fsyncIntervalMicros == null

    init {
        // Parameter guards
        require(!fastMode || fsyncBatchN == null || fsyncBatchN > 0) { "fsyncBatchN must be > 0" }
        require(!fastMode || fsyncIntervalMicros == null || fsyncIntervalMicros > 0) { "fsyncIntervalMicros must be > 0" }
        require(!fastMode || queueCapacity > 0) { "queueCapacity must be > 0" }
        require(backoffNanos >= 0) { "backoffNanos must be >= 0" }

        if (dir.notExists()) Files.createDirectories(dir)
        currentSegIdx = scanLatestSegmentIndex(dir, filePrefix) + 1
        segPath = segmentPath(currentSegIdx)
        ch = openChannel(segPath)
        lg { "open: segment=${segPath.name}" }

        flusherThread = if (fastMode) {
            thread(name = "wal-flusher", isDaemon = true) { flusherLoop() }
        } else null
    }

    /** Append a single ADD record. */
    fun append(payload: ByteBufferL): Long {
        check(running.get()) { "WalWriter is closed" }
        val lsn = nextLsn.incrementAndGet()
        val len = payload.remaining
        val total = Hdr.TAG + Hdr.LSN + Hdr.LEN + len

        val buf = io.get(total)
        buf.put(Tag.ADD)
        buf.putLong(lsn)
        buf.putInt(len)
        buf.put(payload.asReadOnly())
        buf.flip()

        if (fastMode) {
            // Enqueue without fsync; bounded queue with light backpressure.
            val op: () -> Unit = {
                synchronized(writeLock) { writeFully(buf) }
                io.release(buf)
            }
            // offer with light retry until accepted or writer is closed
            while (running.get() && !(queue!!.offer(op))) {
                // optional short timeout-based offer; uncomment if preferred over parkNanos:
                // if (queue.offer(op, 200, TimeUnit.MICROSECONDS)) break
                LockSupport.parkNanos(backoffNanos)
            }
            if (!running.get()) {
                // writer closed while waiting; ensure buffer is released
                io.release(buf)
                throw IllegalStateException("WalWriter is closed")
            }
        } else {
            try {
                synchronized(writeLock) {
                    writeFully(buf)
                    force()
                }
            } finally {
                io.release(buf)
            }
        }

        lg { "append: lsn=$lsn len=$len" }
        return lsn
    }

    /** Write SEAL + (optional fsync) + rotate segment. */
    fun sealSegment(): Long {
        check(running.get()) { "WalWriter is closed" }
        val lsn = nextLsn.incrementAndGet()
        val buf = io.get(Hdr.TAG + Hdr.LSN)
        buf.put(Tag.SEAL).putLong(lsn).flip()

        val op = {
            synchronized(writeLock) {
                writeFully(buf)
                if (!closeOnlyFsync) force()
                rotate()
            }
            io.release(buf)
            lg { "control: Seal lsn=$lsn (rotated to ${segPath.name})" }
        }
        if (fastMode) enqueueOrBackpressure(op) else op()
        return lsn
    }

    /** Write CHECKPOINT (optional fsync) to current segment. */
    fun checkpoint(stripesWritten: Long, lastSeq: Long): Long {
        check(running.get()) { "WalWriter is closed" }
        val lsn = nextLsn.incrementAndGet()
        val buf = io.get(Hdr.TAG + Hdr.LSN + Hdr.CKPT_BODY)
        buf.put(Tag.CHECKPOINT)
        buf.putLong(lsn)
        buf.putLong(stripesWritten)
        buf.putLong(lastSeq)
        buf.flip()

        val op = {
            synchronized(writeLock) {
                writeFully(buf)
                if (!closeOnlyFsync) force()
                lastCheckpointSegIdx = currentSegIdx
            }
            io.release(buf)
            lg { "control: CheckPoint lsn=$lsn stripes=$stripesWritten lastSeq=$lastSeq seg=${segPath.name}" }
        }
        if (fastMode) enqueueOrBackpressure(op) else op()

        return lsn
    }

    /** Delete WAL segments older than last checkpoint. */
    fun pruneObsoleteSegments() {
        val keepFrom = lastCheckpointSegIdx ?: return
        synchronized(writeLock) {
            Files.list(dir).use { stream ->
                stream.filter { p ->
                    p.fileName.toString().startsWith(prefix()) && p != segPath
                }.forEach { p ->
                    parseSegmentIndex(p.fileName.toString())?.let { idx ->
                        if (idx < keepFrom) {
                            runCatching { Files.deleteIfExists(p) }
                                .onSuccess { lg { "prune: deleted ${p.name}" } }
                                .onFailure { e -> lg { "prune failed ${p.name}: ${e.message}" } }
                        }
                    }
                }
            }
        }
    }

    override fun close() {
        if (!running.getAndSet(false)) return
        if (fastMode) queue?.offer { } // non-blocking unblock (okay if it fails; poll has a timeout)
        flusherThread?.join()
        synchronized(writeLock) { runCatching { force() } }
        runCatching { ch.close() }
    }

    // ─────────── internals ───────────

    private fun enqueueOrBackpressure(op: () -> Unit) {
        // shared helper for fastMode ops (bounded queue with light backpressure)
        while (running.get() && !(queue!!.offer(op))) {
            LockSupport.parkNanos(backoffNanos)
        }
        if (!running.get()) throw IllegalStateException("WalWriter is closed")
    }

    private fun writeFully(buf: ByteBufferL) {
        val view = buf.asReadOnlyByteBuffer()
        while (view.hasRemaining()) {
            val n = ch.write(view)
            if (n == 0) Thread.yield()
        }
    }

    private fun force() = ch.force(false)

    private fun rotate() {
        if (!closeOnlyFsync) runCatching { ch.force(false) }
        runCatching { ch.close() }
        currentSegIdx += 1
        segPath = segmentPath(currentSegIdx)
        ch = openChannel(segPath)
    }

    private fun openChannel(path: Path): FileChannel {
        if (path.parent != null && path.parent.notExists()) Files.createDirectories(path.parent)
        return FileChannel.open(path, CREATE, WRITE, APPEND)
    }

    private fun segmentPath(idx: Int): Path =
        dir.resolve("${prefix()}_${idx.toString().padStart(6, '0')}.log")

    private fun prefix() = filePrefix

    private fun scanLatestSegmentIndex(dir: Path, prefix: String): Int {
        if (dir.notExists()) return 0
        var maxIdx = 0
        Files.list(dir).use { stream ->
            stream.filter { p ->
                val n = p.fileName.toString()
                n.startsWith(prefix) && n.endsWith(".log")
            }.forEach { p ->
                parseSegmentIndex(p.fileName.toString())?.let { idx ->
                    if (idx > maxIdx) maxIdx = idx
                }
            }
        }
        return maxIdx
    }

    private fun parseSegmentIndex(name: String): Int? =
        SEG_REGEX.matchEntire(name)?.groupValues?.get(1)?.toIntOrNull()

    private inline fun lg(crossinline msg: () -> String) {
        if (enableLog) println("[WAL] ${msg()}")
    }

    // fast mode flusher thread
    private fun flusherLoop() {
        val cap = (fsyncBatchN ?: 64).coerceAtLeast(1)
        val ops = ArrayList<() -> Unit>(cap)

        var lastFsync = System.nanoTime()
        while ((running.get() || !(queue?.isEmpty() ?: true)) && !Thread.interrupted()) {
            try {
                val timeoutUs = (fsyncIntervalMicros ?: 1000L).coerceAtLeast(1L)
                val op = queue!!.poll(timeoutUs, TimeUnit.MICROSECONDS)
                if (op != null) ops.add(op)

                if (closeOnlyFsync) {
                    if (ops.isNotEmpty()) {
                        ops.forEach { it() }
                        ops.clear()
                    }
                    continue
                }

                val hitBatch = fsyncBatchN != null && ops.size >= fsyncBatchN
                val hitInterval = fsyncIntervalMicros != null &&
                        (System.nanoTime() - lastFsync) >= fsyncIntervalMicros * 1000

                if (ops.isNotEmpty() && (hitBatch || hitInterval)) {
                    ops.forEach { it() }
                    synchronized(writeLock) { force() }
                    ops.clear()
                    lastFsync = System.nanoTime()
                }
            } catch (_: InterruptedException) {
                break
            }
        }

        // Drain everything left (including things queued during the last iteration)
        if (ops.isNotEmpty()) {
            ops.forEach { it() }
            ops.clear()
        }
        while (true) {
            val op = queue!!.poll() ?: break
            op()
        }

        if (!closeOnlyFsync) {
            synchronized(writeLock) { runCatching { force() } }
        }
    }
}
