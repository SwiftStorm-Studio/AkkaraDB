@file:Suppress("PrivatePropertyName")

package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import io.netty.channel.unix.FileDescriptor
import java.io.Closeable
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path

class AkkStripeWriter(
    val baseDir: Path,
    val k: Int,
    val parityCoder: ParityCoder? = null,
    private val autoFlush: Boolean = true,
    private val pool: BufferPool = Pools.io(),
    // --- group commit controls ---
    private val fsyncBatchN: Int = 32,
    private val fsyncIntervalMicros: Long = 500,
    private val onCommit: ((Long) -> Unit)? = null
) : Closeable {

    /* ───────── lane writers (FdWriter only) ───────── */
    private val dataCh = Array<FileWriter>(k) { idx -> FdWriter(baseDir.resolve("data_$idx.akd")) }
    private val parityCh = Array(parityCoder?.parityCount ?: 0) { idx -> FdWriter(baseDir.resolve("parity_$idx.akp")) }

    /* ───────── state ───────── */
    private var laneIdx = 0
    private var stripesWritten_ = 0L
    val stripesWritten: Long get() = stripesWritten_

    private var stripesSinceLastFlush = 0
    private var lastFlushAtNanos: Long = System.nanoTime()

    /* ───────── write‑path ───────── */
    fun addBlock(block: ByteBuffer) {
        require(block.remaining() == BLOCK_SIZE)

        try {
            /* 1. write data lane */
            dataCh[laneIdx].write(block.duplicate())
            laneIdx++

            if (laneIdx == k) {
                // 2. parity
                parityCoder?.let { coder ->
                    val parity = coder.encode(dataCh.map { it.lastWritten!! })
                    parity.forEachIndexed { i, buf ->
                        parityCh[i].write(buf.duplicate())
                        pool.release(buf)
                    }
                }
                // 3. release per‑lane cached buffers
                dataCh.forEach { writer ->
                    writer.lastWritten?.let(pool::release)
                    writer.lastWritten = null
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
        } finally {
            pool.release(block)
        }
    }

    /** Force fsync on every lane (blocking). */
    fun flush() {
        dataCh.forEach(FileWriter::flush)
        parityCh.forEach(FileWriter::flush)
        stripesSinceLastFlush = 0
        lastFlushAtNanos = System.nanoTime()
        onCommit?.invoke(stripesWritten_)
    }

    /** Re‑positions all writers to <code>stripe×BLOCK_SIZE</code>. */
    fun seek(stripes: Long) {
        val off = stripes * BLOCK_SIZE.toLong()
        dataCh.forEach { it.seek(off) }
        parityCh.forEach { it.seek(off) }
        stripesWritten_ = stripes
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

    /* ───────── writer interfaces ───────── */
    private interface FileWriter : Closeable {
        var lastWritten: ByteBuffer?
        fun write(buf: ByteBuffer)
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
     *  * **write()** – zero-copy into the kernel page-cache via `fd.write()`.
     *  * **flush()** – one `fd.sync()` (`fsync(2)`) on the companion JDK
     *    descriptor; no per-call open/close, so latency stays low.
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

        override var lastWritten: ByteBuffer? = null

        override fun write(buf: ByteBuffer) {
            lastWritten = buf
            val len = buf.remaining()
            var written = 0
            while (written < len) {
                val n = fd.write(buf, buf.position() + written, buf.limit())
                if (n <= 0) throw IOException("short pwrite ($n) @offset=$offset")
                written += n
            }
            offset += len
        }

        /** fsync via `FileDescriptor.sync()` – no extra FD churn. */
        override fun flush() {
            jdkFd.sync()                 // translates to fsync(2)
        }

        /** Adjust both native and JDK descriptors to the new position. */
        override fun seek(pos: Long) {
            offset = pos
            ch.position(pos)             // keep them in sync
        }

        override fun close() {
            flush()
            fd.close()
            ch.close()                   // closes `raf` implicitly
        }

        override fun truncate(size: Long) {
            ch.truncate(size)
            seek(size)
        }
    }
}
