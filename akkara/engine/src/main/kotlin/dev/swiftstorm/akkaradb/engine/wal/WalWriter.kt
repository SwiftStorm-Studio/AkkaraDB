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
 */
class WalWriter(
    private val dir: Path,
    private val filePrefix: String,
    private val enableLog: Boolean,
    private val fastMode: Boolean,
    private val fsyncBatchN: Int,          // fastMode: batch size
    private val fsyncIntervalMicros: Long // fastMode: flush interval
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

    // fast mode用
    private val queue = LinkedBlockingQueue<() -> Unit>()
    private val flusherThread: Thread?

    init {
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
            // enqueue without fsync
            queue.put {
                synchronized(writeLock) { writeFully(buf) }
                io.release(buf)
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

    /** Write SEAL + fsync + rotate segment. */
    fun sealSegment(): Long {
        check(running.get()) { "WalWriter is closed" }
        val lsn = nextLsn.incrementAndGet()
        val buf = io.get(Hdr.TAG + Hdr.LSN)
        buf.put(Tag.SEAL).putLong(lsn).flip()

        val op = {
            synchronized(writeLock) {
                writeFully(buf)
                force()
                rotate()
            }
            io.release(buf)
            lg { "control: Seal lsn=$lsn (rotated to ${segPath.name})" }
        }
        if (fastMode) queue.put(op) else op()

        return lsn
    }

    /** Write CHECKPOINT (fsync) to current segment. */
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
                force()
                lastCheckpointSegIdx = currentSegIdx
            }
            io.release(buf)
            lg { "control: CheckPoint lsn=$lsn stripes=$stripesWritten lastSeq=$lastSeq seg=${segPath.name}" }
        }
        if (fastMode) queue.put(op) else op()

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
        queue.put {} // ダミーで unblock
        flusherThread?.join()
        synchronized(writeLock) { force() }
        ch.close()
    }

    // ─────────── internals ───────────

    private fun writeFully(buf: ByteBufferL) {
        val view = buf.asReadOnlyByteBuffer()
        while (view.hasRemaining()) {
            val n = ch.write(view)
            if (n == 0) Thread.yield()
        }
    }

    private fun force() = ch.force(false)

    private fun rotate() {
        runCatching { ch.force(false) }
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
                p.fileName.toString().startsWith(prefix) && p.fileName.toString().endsWith(".log")
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
        val ops = ArrayList<() -> Unit>(fsyncBatchN)
        var lastFsync = System.nanoTime()
        while (running.get() && !Thread.interrupted()) {
            try {
                val op = queue.poll(fsyncIntervalMicros, TimeUnit.MICROSECONDS)
                if (op != null) ops.add(op)
                if (ops.size >= fsyncBatchN ||
                    (System.nanoTime() - lastFsync) >= fsyncIntervalMicros * 1000
                ) {
                    if (ops.isNotEmpty()) {
                        ops.forEach { it() }
                        synchronized(writeLock) { force() }
                        ops.clear()
                        lastFsync = System.nanoTime()
                    }
                }
            } catch (ie: InterruptedException) {
                break
            }
        }
        // drain残り
        ops.forEach { it() }
        synchronized(writeLock) { runCatching { force() } }
    }
}
