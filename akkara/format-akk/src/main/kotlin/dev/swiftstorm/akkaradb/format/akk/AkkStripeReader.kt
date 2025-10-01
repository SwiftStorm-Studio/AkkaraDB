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

@file:Suppress("NOTHING_TO_INLINE", "unused")

package dev.swiftstorm.akkaradb.format.impl

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.vh.LE
import dev.swiftstorm.akkaradb.format.api.*
import java.io.Closeable
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min

/**
 * High-performance StripeWriter implementation targeting NVMe + Java 17.
 *
 * Design goals:
 * - Append-only, lane-synchronized offsets (k data + m parity).
 * - Minimal syscalls: contiguous writes per-lane; fsync grouped by (N blocks or T µs).
 * - Two durability modes: SYNC (safety-first), ASYNC (FastMode: background fsync).
 * - Zero-copy hot path: blocks are direct buffers from BufferPool; no heap copies.
 * - Parity pluggable (XOR/DualXor/RS). Default is XOR for m==1, DualXor for m==2.
 */
class AkkStripeWriter(
    override val k: Int,
    override val m: Int,
    laneDir: Path,
    private val pool: BufferPool,
    private val coder: ParityCoder = when (m) {
        0 -> NoParity
        1 -> XorParity
        2 -> DualXorParity
        else -> RsPlaceholder(m) // TODO: replace with real RS coder
    },
    override var flushPolicy: FlushPolicy = FlushPolicy(),
    private val fastMode: Boolean = false,
    private val fallocateHintBytes: Long = 0L,
    private val laneFilePrefixData: String = "data_",
    private val laneFilePrefixParity: String = "parity_",
    private val laneFileExtData: String = ".akd",
    private val laneFileExtParity: String = ".akp"
) : StripeWriter {

    override val blockSize: Int = BLOCK_SIZE
    override val isFastMode: Boolean get() = fastMode

    // ---- Lane files ----
    private val dataCh: Array<FileChannel>
    private val parityCh: Array<FileChannel> = if (m > 0) Array(m) { open(laneDir.resolve("$laneFilePrefixParity${it}$laneFileExtParity")) } else emptyArray()

    @Volatile
    override var lastSealedStripe: Long = -1; private set

    @Volatile
    override var lastDurableStripe: Long = -1; private set

    @Volatile
    override var pendingInStripe: Int = 0; private set

    private val closed = AtomicBoolean(false)

    // Staging for current stripe (data only). Index = laneId [0..k-1]
    private val stage: Array<ByteBuffer?> = arrayOfNulls(k)

    // Commit queue (sealed-but-not-durable stripes)
    private val commitQ = ConcurrentLinkedQueue<Long>()

    // Metrics (monotonic)
    private val sealedStripes = AtomicLong(0)
    private val durableStripes = AtomicLong(0)
    private val bytesWrittenData = AtomicLong(0)
    private val bytesWrittenParity = AtomicLong(0)
    private val parityMicros = AtomicLong(0)
    private val laneWriteMicros = AtomicLong(0)
    private val fsyncMicros = AtomicLong(0)
    private val backpressureMaxMicros = AtomicLong(0)

    private val fsyncExec: Executor by lazy {
        flushPolicy.executor ?: Executors.newSingleThreadExecutor { r ->
            val t = Thread(r, "akkara-stripe-fsync"); t.isDaemon = true; t
        }
    }

    init {
        require(k >= 1) { "k must be >=1" }
        require(m >= 0) { "m must be >=0" }
        Files.createDirectories(laneDir)
        dataCh = Array(k) { open(laneDir.resolve("$laneFilePrefixData${it}$laneFileExtData")) }
        if (fallocateHintBytes > 0) {
            // Portable preallocation: bump size; OS may sparsely allocate.
            val hint = alignUp(fallocateHintBytes, blockSize.toLong())
            (dataCh + parityCh).forEach { ch ->
                val cur = ch.size()
                if (cur < hint) ch.truncate(hint)
            }
        }
    }

    private fun open(p: Path): FileChannel = FileChannel.open(
        p,
        CREATE, WRITE, READ, DSYNC // DSYNC gives stronger ordering for safety mode
    )

    // ------------------------------------------------------------
    // Write path
    // ------------------------------------------------------------
    override fun addBlock(block: ByteBufferL) {
        ensureOpen()
        val bb = block.unwrap()
        require(bb.isDirect) { "Block must be direct" }
        require(bb.remaining() == blockSize) { "Block must be exactly $blockSize bytes" }

        val idx = pendingInStripe
        require(idx in 0 until k) { "too many blocks for current stripe: $idx >= $k" }

        // Capture a positioned duplicate to avoid caller position interference
        val dup = bb.duplicate()
        stage[idx] = dup
        pendingInStripe = idx + 1

        if (pendingInStripe == k) {
            sealStripe()
        }
    }

    /** Seals current stripe: writes data+parity to lanes, enqueues for commit. */
    private fun sealStripe() {
        val stripeIndex = lastSealedStripe + 1
        val off = stripeIndex * blockSize.toLong()

        // --- Parity ---
        val parityBufs: Array<ByteBuffer> = if (m > 0) Array(m) { pool.get(blockSize).unwrap().apply { clear(); limit(blockSize) } } else emptyArray()
        val parityStart = System.nanoTime()
        coder.encode(stage.requireFull(k), parityBufs)
        val parityEnd = System.nanoTime()
        parityMicros.addAndGet((parityEnd - parityStart) / 1000)

        // --- Lane writes (contiguous per-lane) ---
        val writeStart = System.nanoTime()
        // data lanes
        for (lane in 0 until k) {
            val ch = dataCh[lane]
            val buf = stage[lane]!!
            buf.position(0).limit(blockSize)
            ch.position(off)
            writeFully(ch, buf)
            bytesWrittenData.addAndGet(blockSize.toLong())
        }
        // parity lanes (if any)
        if (m > 0) {
            for (pm in 0 until m) {
                val ch = parityCh[pm]
                val buf = parityBufs[pm]
                buf.position(0).limit(blockSize)
                ch.position(off)
                writeFully(ch, buf)
                bytesWrittenParity.addAndGet(blockSize.toLong())
            }
            // recycle parity buffers back to pool
            for (b in parityBufs) pool.release(ByteBufferL.wrap(b))
        }
        val writeEnd = System.nanoTime()
        laneWriteMicros.addAndGet((writeEnd - writeStart) / 1000)

        // Reset staging
        for (i in 0 until k) stage[i] = null
        pendingInStripe = 0

        // Mark sealed & enqueue for commit
        lastSealedStripe = stripeIndex
        sealedStripes.incrementAndGet()
        commitQ.add(stripeIndex)

        // Group-commit policy check
        maybeCommit()
    }

    private fun maybeCommit() {
        val n = commitQ.size
        if (n == 0) return
        val policyBlocks = flushPolicy.maxBlocks
        val policyMicros = flushPolicy.maxMicros
        var doCommit = false

        if (policyBlocks > 0 && n >= policyBlocks) doCommit = true
        // Time-based trigger: simple heuristic — if queue non-empty and enough time elapsed since last fsync.
        // We don't track the exact timestamp of last fsync group here for simplicity; engines can call flush() proactively.
        if (!doCommit && policyMicros <= 0L) return
        if (doCommit) performCommit(FlushMode.ASYNC.takeIf { fastMode } ?: FlushMode.SYNC)
    }

    override fun flush(mode: FlushMode): CommitTicket {
        ensureOpen()
        // If there is a complete stripe staged, it is already sealed in addBlock(); otherwise nothing to seal here.
        if (commitQ.isEmpty()) {
            return CommitTicket(lastSealedStripe, CompletableFuture.completedFuture(lastDurableStripe))
        }
        return performCommit(mode)
    }

    private fun performCommit(mode: FlushMode): CommitTicket {
        val upto = drainCommitUpto()
        if (upto < 0) return CommitTicket(lastSealedStripe, CompletableFuture.completedFuture(lastDurableStripe))

        return when (mode) {
            FlushMode.SYNC -> {
                val t0 = System.nanoTime()
                // Order: force all data lanes, then parity lanes
                forceAll()
                val t1 = System.nanoTime()
                fsyncMicros.addAndGet((t1 - t0) / 1000)
                lastDurableStripe = upto
                durableStripes.set(upto + 1)
                CommitTicket(upto, CompletableFuture.completedFuture(upto))
            }

            FlushMode.ASYNC -> {
                val fut = CompletableFuture<Long>()
                fsyncExec.execute {
                    try {
                        val t0 = System.nanoTime()
                        forceAll()
                        val t1 = System.nanoTime()
                        fsyncMicros.addAndGet((t1 - t0) / 1000)
                        lastDurableStripe = upto
                        durableStripes.set(upto + 1)
                        fut.complete(upto)
                    } catch (e: Throwable) {
                        fut.completeExceptionally(e)
                    }
                }
                CommitTicket(upto, fut)
            }
        }
    }

    private fun drainCommitUpto(): Long {
        var last = -1L
        var x = commitQ.poll()
        while (x != null) {
            last = x
            x = commitQ.poll()
        }
        return last
    }

    private fun forceAll() {
        // Both arrays may be empty depending on m
        for (ch in dataCh) ch.force(false)
        for (ch in parityCh) ch.force(false)
    }

    private fun writeFully(ch: FileChannel, buf: ByteBuffer) {
        var left = buf.remaining()
        while (left > 0) {
            val n = ch.write(buf)
            if (n < 0) throw IOException("channel closed")
            left -= n
        }
    }

    override fun sealIfComplete(): Boolean {
        if (pendingInStripe == k) {
            sealStripe(); return true
        }
        return false
    }

    override fun seek(stripeIndex: Long) {
        // Simple seek: advance logical counters without scanning. Caller must ensure correctness.
        require(stripeIndex >= -1) { "invalid stripeIndex" }
        lastSealedStripe = stripeIndex
        lastDurableStripe = min(lastDurableStripe, stripeIndex)
        pendingInStripe = 0
        commitQ.clear()
    }

    override fun recover(): RecoveryResult {
        // Lightweight placeholder: position after the lastDurableStripe we tracked.
        // A production impl would scan lane tails or consult Manifest.
        return RecoveryResult(lastSealedStripe, lastDurableStripe, false)
    }

    override fun metrics(): StripeMetricsSnapshot = StripeMetricsSnapshot(
        sealedStripes = sealedStripes.get(),
        durableStripes = durableStripes.get(),
        bytesWrittenData = bytesWrittenData.get(),
        bytesWrittenParity = bytesWrittenParity.get(),
        parityMicros = parityMicros.get(),
        laneWriteMicros = laneWriteMicros.get(),
        fsyncMicros = fsyncMicros.get(),
        maxBackpressureMicros = backpressureMaxMicros.get()
    )

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            try {
                // Best-effort: seal partial if complete; then fsync remaining
                if (pendingInStripe == k) sealStripe()
                if (commitQ.isNotEmpty()) performCommit(if (fastMode) FlushMode.ASYNC else FlushMode.SYNC).future.join()
            } finally {
                var first: Throwable? = null
                fun swallow(t: Throwable) {
                    if (first == null) first = t
                }
                (dataCh + parityCh).forEach { ch ->
                    try {
                        ch.close()
                    } catch (t: Throwable) {
                        swallow(t)
                    }
                }
                if (first != null) throw first
            }
        }
    }

    private fun ensureOpen() {
        check(!closed.get()) { "writer closed" }
    }

    private fun Array<ByteBuffer?>.requireFull(n: Int): Array<ByteBuffer> {
        check(size >= n) { "staging size < k" }
        @Suppress("UNCHECKED_CAST")
        val out = Array(n) { i ->
            val b = this[i] ?: error("missing block for lane=$i")
            b
        }
        return out
    }

    private fun alignUp(x: Long, pow2: Long): Long {
        require(pow2 > 0 && (pow2 and (pow2 - 1)) == 0L)
        return (x + (pow2 - 1)) and (pow2 - 1).inv()
    }
}

// =====================================================================================
// Parity encoders
// =====================================================================================

/** Strategy interface for parity generation. */
interface ParityCoder : Closeable {
    /**
     * Encode [data] blocks into [parity] blocks.
     * Each buffer position/limit define the write window; implementations write exactly BLOCK_SIZE bytes.
     */
    fun encode(data: Array<ByteBuffer>, parity: Array<ByteBuffer>)
    override fun close() {}
}

/** No parity (m==0). */
object NoParity : ParityCoder {
    override fun encode(data: Array<ByteBuffer>, parity: Array<ByteBuffer>) {
        require(parity.isEmpty())
    }
}

/** XOR parity (m==1): p0 = data0 ^ data1 ^ ... ^ data{k-1}. */
object XorParity : ParityCoder {
    override fun encode(data: Array<ByteBuffer>, parity: Array<ByteBuffer>) {
        require(parity.size == 1) { "XOR requires exactly 1 parity buffer" }
        val p = parity[0]
        val sz = data[0].remaining()
        p.clear(); p.limit(sz)

        // Initialize parity with first data block
        val d0 = data[0].duplicate().apply { position(0).limit(sz) }
        p.put(d0)
        p.flip()

        val tmp = ByteBuffer.allocateDirect(sz)
        var i = 1
        while (i < data.size) {
            tmp.clear(); tmp.limit(sz)
            val di = data[i].duplicate().apply { position(0).limit(sz) }
            tmp.put(di); tmp.flip()
            xorInto(p, tmp)
            i++
        }
        p.position(sz).limit(sz)
    }

    private fun xorInto(dst: ByteBuffer, src: ByteBuffer) {
        // XOR in 8-byte chunks for throughput
        val n = dst.limit()
        var off = 0
        while (off + 8 <= n) {
            val a = LE.getLong(dst, off)
            val b = LE.getLong(src, off)
            LE.putLong(dst, off, a xor b)
            off += 8
        }
        while (off < n) {
            val a = dst.get(off)
            val b = src.get(off)
            dst.put(off, (a.toInt() xor b.toInt()).toByte())
            off++
        }
    }
}

/** Dual-XOR (m==2) example: p0 = XOR(all even lanes), p1 = XOR(all odd lanes). */
object DualXorParity : ParityCoder {
    override fun encode(data: Array<ByteBuffer>, parity: Array<ByteBuffer>) {
        require(parity.size == 2) { "DualXor requires exactly 2 parity buffers" }
        val sz = data[0].remaining()
        for (p in parity) {
            p.clear(); p.limit(sz)
        }

        // Initialize
        var evenInit = false
        var oddInit = false
        for (i in data.indices) {
            val di = data[i].duplicate().apply { position(0).limit(sz) }
            val dst = if ((i and 1) == 0) parity[0] else parity[1]
            if ((i and 1) == 0 && !evenInit || (i and 1) == 1 && !oddInit) {
                dst.put(di); dst.flip()
                if ((i and 1) == 0) evenInit = true else oddInit = true
            } else {
                xorInto(dst, di)
            }
        }
        parity[0].position(sz).limit(sz)
        parity[1].position(sz).limit(sz)
    }

    private fun xorInto(dst: ByteBuffer, src: ByteBuffer) {
        val n = dst.limit()
        var off = 0
        while (off + 8 <= n) {
            val a = LE.getLong(dst, off)
            val b = LE.getLong(src, off)
            LE.putLong(dst, off, a xor b)
            off += 8
        }
        while (off < n) {
            val a = dst.get(off)
            val b = src.get(off)
            dst.put(off, (a.toInt() xor b.toInt()).toByte())
            off++
        }
    }
}

/** Placeholder for Reed-Solomon parity (m>=3). Replace with a proper GF(256) coder. */
class RsPlaceholder(private val m: Int) : ParityCoder {
    override fun encode(data: Array<ByteBuffer>, parity: Array<ByteBuffer>) {
        // Simple fallback: first parity = XOR(all), others = zero. Not fault-tolerant beyond 1 loss.
        require(parity.size == m)
        if (m == 0) return
        if (parity.isEmpty()) return
        XorParity.encode(data, arrayOf(parity[0]))
        for (i in 1 until parity.size) {
            val p = parity[i]
            p.clear(); LE.fillZero(p, p.capacity()); p.limit(p.capacity())
        }
    }
}
