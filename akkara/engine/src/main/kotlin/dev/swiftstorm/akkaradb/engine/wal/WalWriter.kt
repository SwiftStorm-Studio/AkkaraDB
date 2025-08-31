package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.engine.wal.WalReplay.SEG_REGEX
import java.io.Closeable
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.name
import kotlin.io.path.notExists

/**
 * Segmented WAL writer with explicit Seal → rotate, CheckPoint, and prune.
 *
 * Public API:
 *  - append(payload)
 *  - sealSegment()
 *  - checkpoint(stripesWritten, lastSeq)
 *  - pruneObsoleteSegments()
 *  - close()
 */
class WalWriter(
    private val dir: Path,
    private val filePrefix: String,
    private val enableLog: Boolean,
) : Closeable {

    private object Tag {
        const val ADD: Byte = 0x01
        const val SEAL: Byte = 0x02
        const val CHECKPOINT: Byte = 0x03
    }

    private object Hdr {
        const val TAG: Int = 1
        const val LSN: Int = 8
        const val LEN: Int = 4           // for ADD payload length
        const val CKPT_BODY: Int = 16    // stripesWritten(8) + lastSeq(8)
    }

    private val running = AtomicBoolean(true)
    private val nextLsn = AtomicLong(0)

    private var currentSegIdx: Int
    private var lastCheckpointSegIdx: Int? = null

    private var ch: FileChannel
    private var segPath: Path

    private val writeLock = Any()
    private val io = Pools.io()

    init {
        if (dir.notExists()) Files.createDirectories(dir)
        currentSegIdx = scanLatestSegmentIndex(dir, filePrefix) + 1
        segPath = segmentPath(currentSegIdx)
        ch = openChannel(segPath)
        lg { "open: segment=${segPath.name}" }
    }

    /** Append a single ADD record. Copies payload into an owned buffer and writes it. */
    fun append(payload: ByteBufferL): Long {
        check(running.get()) { "WalWriter is closed" }

        lg { "append: lsn=${nextLsn.get() + 1} len=${payload.remaining}" }

        val lsn = nextLsn.incrementAndGet()
        val len = payload.remaining
        val total = Hdr.TAG + Hdr.LSN + Hdr.LEN + len

        val buf = io.get(total)
        try {
            buf.put(Tag.ADD)
            buf.putLong(lsn)
            buf.putInt(len)
            // copy payload contents into owned buffer
            buf.put(payload.asReadOnly())
            buf.flip()
            synchronized(writeLock) {
                writeFully(buf)
            }
        } finally {
            io.release(buf)
        }
        return lsn
    }

    /** Write SEAL (fsync) then rotate to next segment. */
    fun sealSegment(): Long {
        check(running.get()) { "WalWriter is closed" }

        val lsn = nextLsn.incrementAndGet()
        val buf = io.get(Hdr.TAG + Hdr.LSN)
        try {
            buf.put(Tag.SEAL)
            buf.putLong(lsn)
            buf.flip()
            synchronized(writeLock) {
                writeFully(buf)
                force()
                // rotate()
            }
        } finally {
            io.release(buf)
        }
        lg { "control: Seal lsn=$lsn (rotated to ${segPath.name})" }
        return lsn
    }

    /**
     * Write CHECKPOINT to the *current* segment and fsync.
     * After success, this segment becomes the pruning boundary.
     */
    fun checkpoint(stripesWritten: Long, lastSeq: Long): Long {
        check(running.get()) { "WalWriter is closed" }

        val lsn = nextLsn.incrementAndGet()
        val buf = io.get(Hdr.TAG + Hdr.LSN + Hdr.CKPT_BODY)
        try {
            buf.put(Tag.CHECKPOINT)
            buf.putLong(lsn)
            buf.putLong(stripesWritten)
            buf.putLong(lastSeq)
            buf.flip()
            synchronized(writeLock) {
                writeFully(buf)
                force()
                lastCheckpointSegIdx = currentSegIdx
            }
        } finally {
            io.release(buf)
        }
        lg { "control: CheckPoint lsn=$lsn stripes=$stripesWritten lastSeq=$lastSeq seg=${segPath.name}" }
        return lsn
    }

    /** Delete WAL segments older than the last checkpointed segment. */
    fun pruneObsoleteSegments() {
        val keepFrom = lastCheckpointSegIdx ?: return
        synchronized(writeLock) {
            Files.list(dir).use { stream ->
                stream.filter { p -> p.fileName.toString().startsWith(prefix()) && p != segPath }
                    .forEach { p ->
                        val idx = parseSegmentIndex(p.fileName.toString())
                        if (idx != null && idx < keepFrom) {
                            runCatching { Files.deleteIfExists(p) }
                                .onSuccess { lg { "prune: deleted ${p.name}" } }
                                .onFailure { e -> lg { "prune: failed ${p.name} : ${e.message}" } }
                        }
                    }
            }
        }
    }

    override fun close() {
        if (!running.getAndSet(false)) return
        synchronized(writeLock) {
            try {
                force()
            } catch (_: Throwable) {
                // ignore
            } finally {
                runCatching { ch.close() }
                lg { "close: segment=${segPath.name}" }
            }
        }
    }

    // ─────────────── internals ───────────────

    private fun writeFully(buf: ByteBufferL) {
        val view = buf.asReadOnlyByteBuffer()
        while (view.hasRemaining()) {
            val n = ch.write(view)
            if (n == 0) Thread.onSpinWait()
        }
    }

    private fun force() {
        try {
            ch.force(false)
        } catch (e: IOException) {
            throw e
        }
    }

    private fun rotate() {
        runCatching { ch.force(false) }
        runCatching { ch.close() }
        currentSegIdx += 1
        segPath = segmentPath(currentSegIdx)
        ch = openChannel(segPath)
    }

    private fun openChannel(path: Path): FileChannel {
        if (path.parent != null && path.parent.notExists()) Files.createDirectories(path.parent)
        return FileChannel.open(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND
        )
    }

    private fun segmentPath(idx: Int): Path =
        dir.resolve("${prefix()}_${idx.toString().padStart(6, '0')}.log")

    private fun prefix() = filePrefix

    private fun scanLatestSegmentIndex(dir: Path, prefix: String): Int {
        if (dir.notExists()) return 0
        var maxIdx = 0
        Files.list(dir).use { stream ->
            stream.filter { p -> p.fileName.toString().startsWith(prefix) && p.fileName.toString().endsWith(".log") }
                .forEach { p ->
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
        if (!enableLog) return
        println("[WAL] ${msg()}")
    }
}