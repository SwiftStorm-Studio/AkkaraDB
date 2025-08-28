package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.StripeWriter
import io.netty.channel.unix.FileDescriptor
import java.io.Closeable
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.file.Path

class AkkStripeWriter(
    val baseDir: Path,
    override val k: Int,
    val parityCoder: ParityCoder? = null,
    private val autoFlush: Boolean = true,
    private val pool: BufferPool = Pools.io(),
    // --- group commit controls ---
    private val fsyncBatchN: Int = 32,
    private val fsyncIntervalMicros: Long = 500,
    private val onCommit: ((Long) -> Unit)? = null
) : Closeable, StripeWriter {

    /* ───────── lane writers ───────── */
    private val dataCh = Array<FileWriter>(k) { idx -> makeWriter(baseDir.resolve("data_$idx.akd")) }
    private val parityCh = Array(parityCoder?.parityCount ?: 0) { idx -> makeWriter(baseDir.resolve("parity_$idx.akp")) }

    override val m: Int get() = parityCh.size

    /* ───────── state ───────── */
    private var laneIdx = 0
    private var stripesWritten_ = 0L
    override val stripesWritten: Long get() = stripesWritten_

    private var stripesSinceLastFlush = 0
    private var lastFlushAtNanos: Long = System.nanoTime()

    /* ───────── write-path (ByteBufferL) ───────── */
    override fun addBlock(block: ByteBufferL) {
        require(block.remaining == BLOCK_SIZE) { "block size must be $BLOCK_SIZE, got ${block.remaining}" }

        val writer = dataCh[laneIdx]
        writer.write(block)
        writer.lastWritten = block
        laneIdx++

        if (laneIdx == k) {
            // 2. parity
            parityCoder?.let { coder ->
                val parityBlocks: List<ByteBufferL> = coder.encode(dataCh.map { it.lastWritten!! })
                parityBlocks.forEachIndexed { i, buf ->
                    parityCh[i].write(buf)
                    pool.release(buf)
                }
            }

            // 3. release per-lane cached buffers
            dataCh.forEach { w ->
                w.lastWritten?.let(pool::release)
                w.lastWritten = null
            }
            laneIdx = 0
            stripesWritten_++

            // 4. group commit policy: N stripes or T µs
            stripesSinceLastFlush++
            val now = System.nanoTime()
            val dueByTime = (now - lastFlushAtNanos) >= (fsyncIntervalMicros * 1_000)
            val dueByCount = stripesSinceLastFlush >= fsyncBatchN
            if (autoFlush && (dueByCount || dueByTime)) {
                flush()
            }
        }
    }

    /** Force fsync on every lane (blocking). */
    override fun flush(): Long {
        dataCh.forEach(FileWriter::flush)
        parityCh.forEach(FileWriter::flush)
        stripesSinceLastFlush = 0
        lastFlushAtNanos = System.nanoTime()
        onCommit?.invoke(stripesWritten_)
        return stripesWritten_
    }

    /** Re-positions all writers to `stripes×BLOCK_SIZE`. */
    override fun seek(stripeIndex: Long) {
        val off = stripeIndex * BLOCK_SIZE.toLong()
        dataCh.forEach { it.seek(off) }
        parityCh.forEach { it.seek(off) }
        stripesWritten_ = stripeIndex
    }

    fun truncateTo(stripes: Long) {
        val off = stripes * BLOCK_SIZE.toLong()
        dataCh.forEach { (it as FileWriterEx).truncate(off) }
        parityCh.forEach { (it as FileWriterEx).truncate(off) }
        seek(stripes)
    }

    override fun close() {
        flush()
        dataCh.forEach(FileWriter::close)
        parityCh.forEach(FileWriter::close)
    }

    /* ───────── writer factory ───────── */

    private fun makeWriter(path: Path): FileWriter {
        val force = System.getProperty("akkaradb.writer")?.lowercase()
        if (isWindows()) {
            // Windows では Netty FD パスは使わない
            return JdkAggWriter(path, pool)
        }

        return when (force) {
            "fd" -> FdWriter(path)
            "jdk" -> JdkAggWriter(path, pool)
            else -> try {
                FdWriter(path)
            } catch (_: Throwable) {
                JdkAggWriter(path, pool)
            }
        }
    }

    private fun isWindows(): Boolean =
        System.getProperty("os.name")?.startsWith("Windows", ignoreCase = true) == true

    /* ───────── writer interfaces (ByteBufferL) ───────── */
    private interface FileWriter : Closeable {
        var lastWritten: ByteBufferL?
        fun write(buf: ByteBufferL)
        fun flush()
        fun seek(pos: Long)
    }

    private interface FileWriterEx : FileWriter {
        fun truncate(size: Long)
    }

    /**
     * Direct file writer backed by Netty-native `io.netty.channel.unix.FileDescriptor`
     * for data-path writes, plus a single long-lived `RandomAccessFile`
     * (and its `java.nio.channels.FileChannel`) to issue the fsync.
     *
     *  * write() – zero-copy into the kernel page-cache via `fd.write()`.
     *  * flush() – one `fd.sync()` (`fsync(2)`) on the companion JDK descriptor.
     */
    private class FdWriter(path: Path) : FileWriterEx {

        /** Native descriptor used for all pwrite(2) operations. */
        private val fd: FileDescriptor = FileDescriptor.from(path.toString())

        /** JDK-level handle kept solely for durable flushes. */
        private val raf = RandomAccessFile(path.toFile(), "rw")
        private val jdkFd: java.io.FileDescriptor = raf.fd
        private val ch: FileChannel = raf.channel

        /** Current write offset (pwrite position). */
        private var offset: Long = ch.size()

        override var lastWritten: ByteBufferL? = null

        override fun write(buf: ByteBufferL) {
            lastWritten = buf
            val len = buf.remaining
            var written = 0
            val bb = buf.toMutableByteBuffer() // view; positionは buf と同期しない
            val start = bb.position()
            while (written < len) {
                val n = fd.write(bb, start + written, start + len) // (buffer, pos, limit)
                if (n <= 0) throw IOException("short pwrite ($n) @offset=$offset")
                written += n
            }
            buf.advance(len) // ByteBufferL 側の position を同期
            offset += len
        }

        /** fsync via `FileDescriptor.sync()` – no extra FD churn. */
        override fun flush() {
            jdkFd.sync() // fsync(2)
        }

        /** Adjust both native and JDK descriptors to the new position. */
        override fun seek(pos: Long) {
            offset = pos
            ch.position(pos) // keep them in sync
        }

        override fun close() {
            flush()
            fd.close()
            ch.close() // closes `raf` implicitly
        }

        override fun truncate(size: Long) {
            ch.truncate(size)
            seek(size)
        }
    }

    /**
     * Portable fast path: JDK FileChannel with 1MiB lane aggregation and fdatasync batching.
     *
     * Strategy:
     * - append into a lane-local 1MiB buffer (32KiB×32)
     * - when full, do a single write() at the current lane position
     * - force(false) only on group-commit boundaries (outer flush())
     */
    private class JdkAggWriter(
        path: Path,
        private val pool: BufferPool,
        laneBufferSize: Int = BLOCK_SIZE * 32, // 1 MiB per lane
        private val preallocStep: Long = 64L * 1024 * 1024 // 64 MiB coarse prealloc (best-effort)
    ) : FileWriterEx {
        private val raf = RandomAccessFile(path.toFile(), "rw")
        private val ch: FileChannel = raf.channel
        private var pos: Long = ch.size()
        private var preallocLimit: Long = roundUp(pos, preallocStep)

        // lane-local aggregation buffer
        private val agg: ByteBufferL = pool.get(laneBufferSize)

        override var lastWritten: ByteBufferL? = null

        override fun write(buf: ByteBufferL) {
            lastWritten = buf
            val cap = agg.capacity
            val need = buf.remaining

            if (need >= cap) {
                flushLane()
                writeDirect(buf)
                return
            }
            if (agg.remaining < need) {
                flushLane()
            }
            agg.put(buf.asReadOnlyByteBuffer()) // copy into lane agg
        }

        override fun flush() {
            flushLane()
            ch.force(false)
        }

        override fun seek(pos: Long) {
            flushLane()
            this@JdkAggWriter.pos = pos
        }

        override fun truncate(size: Long) {
            flushLane()
            ch.truncate(size)
            pos = size
            preallocLimit = roundUp(size, preallocStep)
        }

        override fun close() {
            try {
                flush()
            } finally {
                ch.close(); raf.close()
                pool.release(agg)
            }
        }

        private fun writeDirect(src: ByteBufferL) {
            ensurePreallocated(pos + src.remaining)
            val bb = src.toMutableByteBuffer()
            var rem = bb.remaining()
            while (rem > 0) {
                val n = ch.write(bb, pos)
                if (n <= 0) throw IOException("short write ($n) @pos=$pos (direct)")
                pos += n
                rem -= n
            }
            src.advance(src.remaining) // src はすべて消費
        }

        private fun flushLane() {
            if (agg.position == 0) return
            ensurePreallocated(pos + agg.position)
            agg.flip()
            val bb = agg.toMutableByteBuffer()
            while (bb.hasRemaining()) {
                val n = ch.write(bb, pos)
                if (n <= 0) throw IOException("short write ($n) @pos=$pos")
                pos += n
            }
            agg.clear()
        }

        private fun ensurePreallocated(requiredEnd: Long) {
            if (requiredEnd <= preallocLimit) return
            val newLimit = roundUp(requiredEnd + preallocStep, preallocStep)
            raf.setLength(newLimit)
            preallocLimit = newLimit
        }

        private fun roundUp(v: Long, step: Long): Long = ((v + step - 1) / step) * step
    }
}
