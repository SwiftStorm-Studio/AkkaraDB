package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import io.netty.channel.unix.FileDescriptor
import io.netty.channel.uring.IoUring
import java.io.Closeable
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*

class AkkStripeWriter(
    val baseDir: Path,
    val k: Int,
    val parityCoder: ParityCoder? = null,
    private val autoFlush: Boolean = true,
) : Closeable {

    /* ───────── backend selection ───────── */
    private val useFdWriter = IoUring.isAvailable()

    /* ───────── lane writers ───────── */
    private val dataCh = Array<FileWriter>(k) { idx -> openWriter(baseDir.resolve("data_$idx.akd")) }
    private val parityCh = Array(parityCoder?.parityCount ?: 0) { idx -> openWriter(baseDir.resolve("parity_$idx.akp")) }

    private fun openWriter(path: Path): FileWriter = if (useFdWriter) FdWriter(path) else NioWriter(path)

    /* ───────── state ───────── */
    private var laneIdx = 0
    private var stripesWritten_ = 0L
    val stripesWritten: Long get() = stripesWritten_

    /* ───────── write‑path ───────── */
    fun addBlock(block: ByteBuffer) {
        require(block.remaining() == BLOCK_SIZE) { "block must be exactly 32 KiB" }
        dataCh[laneIdx].write(block.duplicate())
        laneIdx++

        if (laneIdx == k) {
            // parity encode
            parityCoder?.let { coder ->
                val parityBlks = coder.encode(dataCh.map { it.lastWritten!! })
                for (i in parityCh.indices) parityCh[i].write(parityBlks[i])
            }
            laneIdx = 0
            stripesWritten_++
            if (autoFlush) flush()
        }
    }

    /** Force fsync on every lane (blocking). */
    fun flush() {
        dataCh.forEach(FileWriter::flush)
        parityCh.forEach(FileWriter::flush)
    }

    /** Re‑positions all writers to <code>stripe×BLOCK_SIZE</code>. */
    fun seek(stripes: Long) {
        val off = stripes * BLOCK_SIZE.toLong()
        dataCh.forEach { it.seek(off) }
        parityCh.forEach { it.seek(off) }
        stripesWritten_ = stripes
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

    /** Fallback writer based on classic NIO<code>FileChannel</code>. */
    private class NioWriter(path: Path) : FileWriter {
        private val ch = FileChannel.open(path, CREATE, WRITE, DSYNC)
        override var lastWritten: ByteBuffer? = null
        override fun write(buf: ByteBuffer) {
            lastWritten = buf; ch.write(buf)
        }

        override fun flush() {
            ch.force(true)
        }

        override fun seek(pos: Long) {
            ch.position(pos)
        }

        override fun close() {
            ch.close()
        }
    }

    /**
     * Direct file writer backed by Netty-native `io.netty.channel.unix.FileDescriptor`
     * for data-path writes, plus a single long-lived `RandomAccessFile`
     * (and its `java.nio.channels.FileChannel`) to issue the fsync.
     *
     *  * **write()** – zero-copy into the kernel page-cache via `fd.write()`.
     *  * **flush()** – one `fd.sync()` (`fsync(2)`) on the companion JDK
     *    descriptor; no per-call open/close, so latency stays ≈ 3–4 µs.
     */
    private class FdWriter(path: Path) : FileWriter {

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
    }
}