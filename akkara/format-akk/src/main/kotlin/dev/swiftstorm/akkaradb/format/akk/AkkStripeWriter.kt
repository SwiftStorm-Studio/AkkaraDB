/*
 * AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

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

/**
 * Append-only stripe writer: writes fixed-size 32 KiB blocks into
 * [k] data lanes plus [m] parity lanes at the same logical offset.
 *
 * Thread-safety: not intended for concurrent writers.
 * One thread must own the writer instance at all times.
 */
class AkkStripeWriter(
    val baseDir: Path,
    override val k: Int,
    val parityCoder: ParityCoder? = null,
    private val autoFlush: Boolean,
    /** If true, `flush()` will NOT issue fsync/fdatasync (best-effort, faster, non-durable). */
    private val isFastMode: Boolean,
    private val pool: BufferPool = Pools.io(),
    // --- group commit controls ---
    private val fsyncBatchN: Int,
    private val fsyncIntervalMicros: Long,
    private val onCommit: ((Long) -> Unit)? = null
) : Closeable, StripeWriter {

    /* ───────── lane writers ───────── */
    val dataCh = Array<FileWriter>(k) { idx -> makeWriter(baseDir.resolve("data_$idx.akd")) }
    val parityCh = Array(parityCoder?.parityCount ?: 0) { idx -> makeWriter(baseDir.resolve("parity_$idx.akp")) }

    override val m: Int get() = parityCh.size

    /* ───────── state ───────── */
    private var laneIdx = 0
    private var stripesWritten_ = 0L
    override val stripesWritten: Long get() = stripesWritten_

    private var stripesSinceLastFlush = 0
    private var lastFlushAtNanos: Long = System.nanoTime()

    @Volatile
    private var closed = false

    /* ───────── write-path (ByteBufferL) ───────── */
    override fun addBlock(block: ByteBufferL) {
        check(!closed) { "StripeWriter is closed" }
        require(block.remaining == BLOCK_SIZE) {
            "block size must be $BLOCK_SIZE, got ${block.remaining}"
        }

        val writer = dataCh[laneIdx]
        writer.write(block)
        writer.lastWritten = block
        laneIdx++

        if (laneIdx == k) {
            // 1. generate parity if configured
            parityCoder?.let { coder ->
                val parityBlocks = coder.encode(dataCh.map { it.lastWritten!! })
                parityBlocks.forEachIndexed { i, buf ->
                    parityCh[i].write(buf)
                    pool.release(buf)
                }
            }

            // 2. release cached data buffers
            dataCh.forEach { w ->
                w.lastWritten?.let(pool::release)
                w.lastWritten = null
            }
            laneIdx = 0
            stripesWritten_++

            // 3. group commit trigger
            stripesSinceLastFlush++
            val now = System.nanoTime()
            val dueByTime = (now - lastFlushAtNanos) >= (fsyncIntervalMicros * 1_000)
            val dueByCount = stripesSinceLastFlush >= fsyncBatchN
            if (autoFlush && (dueByCount || dueByTime)) {
                flush()
            }
        }
    }

    /** Flush lane buffers. If [isFastMode] is true, fsync/fdatasync is **skipped**. */
    override fun flush(): Long {
        if (closed) return stripesWritten
        // In fast mode we still push buffered bytes to the file (write), but skip durability.
        dataCh.forEach { it.flush(force = !isFastMode).also { /* no-op */ } }
        parityCh.forEach { it.flush(force = !isFastMode).also { /* no-op */ } }
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

    fun fsyncAll(): Long {
        if (closed) return stripesWritten
        dataCh.forEach { it.sync() }
        parityCh.forEach { it.sync() }
        return stripesWritten
    }

    override fun close() {
        if (closed) return
        try {
            // Respect isFastMode on close as well. If you want "always durable on close",
            // change to flush(force = true) by adding a separate close path.
            flush()
        } catch (_: Throwable) {
            // best-effort
        }
        dataCh.forEach { it.close() }
        parityCh.forEach { it.close() }
        closed = true
    }

    /* ───────── writer factory ───────── */
    private fun makeWriter(path: Path): FileWriter {
        val force = System.getProperty("akkaradb.writer")?.lowercase()
        if (isWindows()) return JdkAggWriter(path, pool)

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
    interface FileWriter : Closeable {
        var lastWritten: ByteBufferL?
        fun write(buf: ByteBufferL)

        /**
         * Flush buffered bytes to the file. If [force] is true, also issue a durability barrier
         * (e.g., fsync/fdatasync). If false, only pushes page-cache writes (non-durable).
         */
        fun flush(force: Boolean = true)
        fun seek(pos: Long)
        fun sync()
    }

    private interface FileWriterEx : FileWriter {
        fun truncate(size: Long)
    }

    /**
     * Direct file writer backed by Netty-native FileDescriptor (Linux only).
     * - write(): zero-copy pwrite() into page cache.
     * - flush(): fsync(2) via JDK descriptor when force=true.
     */
    private class FdWriter(path: Path) : FileWriterEx {
        private val fd: FileDescriptor = FileDescriptor.from(path.toString())
        private val raf = RandomAccessFile(path.toFile(), "rw")
        private val jdkFd: java.io.FileDescriptor = raf.fd
        private val ch: FileChannel = raf.channel
        private var offset: Long = ch.size()

        override var lastWritten: ByteBufferL? = null

        override fun write(buf: ByteBufferL) {
            lastWritten = buf
            val len = buf.remaining
            var written = 0
            val bb = buf.toMutableByteBuffer()
            val start = bb.position()
            while (written < len) {
                val n = fd.write(bb, start + written, start + len)
                if (n <= 0) throw IOException("short pwrite ($n) @offset=$offset")
                written += n
            }
            buf.advance(len)
            offset += len
        }

        override fun flush(force: Boolean) {
            if (force) jdkFd.sync()
        }

        override fun seek(pos: Long) {
            offset = pos; ch.position(pos)
        }

        override fun sync() = jdkFd.sync()

        override fun truncate(size: Long) {
            ch.truncate(size); seek(size)
        }

        override fun close() {
            try {
                flush(force = true)
            } finally {
                fd.close(); ch.close()
            }
        }
    }

    /**
     * Portable JDK writer: aggregates into 1MiB lane buffer and optionally calls fdatasync
     * at group-commit boundaries depending on [force].
     */
    private class JdkAggWriter(
        path: Path,
        private val pool: BufferPool,
        laneBufferSize: Int = BLOCK_SIZE * 32,
        private val preallocStep: Long = 64L * 1024 * 1024
    ) : FileWriterEx {
        private val raf = RandomAccessFile(path.toFile(), "rw")
        private val ch: FileChannel = raf.channel
        private var pos: Long = ch.size()
        private var preallocLimit: Long = roundUp(pos, preallocStep)
        private val agg: ByteBufferL = pool.get(laneBufferSize)

        override var lastWritten: ByteBufferL? = null

        override fun write(buf: ByteBufferL) {
            lastWritten = buf
            val need = buf.remaining
            if (need >= agg.capacity) {
                flushLane()
                writeDirect(buf)
                return
            }
            if (agg.remaining < need) flushLane()
            agg.put(buf.asReadOnlyByteBuffer())
        }

        override fun flush(force: Boolean) {
            flushLane()
            if (force) ch.force(false)
        }

        override fun seek(pos: Long) {
            flushLane(); this@JdkAggWriter.pos = pos
        }

        override fun sync() = ch.force(false)

        override fun truncate(size: Long) {
            flushLane(); ch.truncate(size); pos = size; preallocLimit = roundUp(size, preallocStep)
        }

        override fun close() {
            try {
                flush(force = true)
            } finally {
                ch.close(); raf.close(); pool.release(agg)
            }
        }

        private fun writeDirect(src: ByteBufferL) {
            ensurePreallocated(pos + src.remaining)
            val bb = src.toMutableByteBuffer()
            while (bb.hasRemaining()) {
                val n = ch.write(bb, pos)
                if (n <= 0) throw IOException("short write ($n) @pos=$pos (direct)")
                pos += n
            }
            src.advance(src.remaining)
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
