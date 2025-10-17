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

package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.format.akk.parity.DualXorParityCoder
import dev.swiftstorm.akkaradb.format.akk.parity.NoParityCoder
import dev.swiftstorm.akkaradb.format.akk.parity.RSParityCoder
import dev.swiftstorm.akkaradb.format.akk.parity.XorParityCoder
import dev.swiftstorm.akkaradb.format.api.*
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min

/**
 * High-performance StripeWriter (ByteBufferL-only).
 */
class AkkStripeWriter(
    override val k: Int,
    override val m: Int,
    laneDir: Path,
    private val pool: BufferPool,
    private val coder: ParityCoder = when (m) {
        0 -> NoParityCoder()
        1 -> XorParityCoder()
        2 -> DualXorParityCoder()
        else -> RSParityCoder(m)
    },
    override var flushPolicy: FlushPolicy = FlushPolicy(),
    private val fastMode: Boolean = false,
    fallocateHintBytes: Long = 0L,
    private val laneFilePrefixData: String = "data_",
    private val laneFilePrefixParity: String = "parity_",
    private val laneFileExtData: String = ".akd",
    private val laneFileExtParity: String = ".akp"
) : StripeWriter {

    override val blockSize: Int = BLOCK_SIZE
    override val isFastMode: Boolean get() = fastMode

    // ---- Lane files ----
    private val dataCh: Array<FileChannel>
    private val parityCh: Array<FileChannel> =
        if (m > 0) Array(m) { open(laneDir.resolve("$laneFilePrefixParity${it}$laneFileExtParity")) } else emptyArray()

    @Volatile
    override var lastSealedStripe: Long = -1; private set

    @Volatile
    override var lastDurableStripe: Long = -1; private set

    @Volatile
    override var pendingInStripe: Int = 0; private set

    private val closed = AtomicBoolean(false)

    // Staging for current stripe (data only). Index = laneId [0..k-1]
    private val stage: Array<ByteBufferL?> = arrayOfNulls(k)

    // Commit queue (sealed-but-not-durable stripes)
    private val commitQ = ConcurrentLinkedQueue<Long>()
    private val pendingCommits = AtomicInteger(0)

    // Metrics (monotonic)
    private val sealedStripes = AtomicLong(0)
    private val durableStripes = AtomicLong(0)
    private val bytesWrittenData = AtomicLong(0)
    private val bytesWrittenParity = AtomicLong(0)
    private val parityMicros = AtomicLong(0)
    private val laneWriteMicros = AtomicLong(0)
    private val fsyncMicros = AtomicLong(0)
    private val backpressureMaxMicros = AtomicLong(0)

    @Volatile
    private var lastCommitStartNanos: Long = System.nanoTime()

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
            val hint = alignUp(fallocateHintBytes, blockSize.toLong())
            (dataCh + parityCh).forEach { ch ->
                val cur = ch.size()
                if (cur < hint) ch.truncate(hint)
            }
        }
    }

    private fun open(p: Path): FileChannel =
        if (fastMode) FileChannel.open(p, CREATE, READ, WRITE)
        else FileChannel.open(p, CREATE, READ, WRITE, DSYNC)

    // ------------------------------------------------------------
    // Write path
    // ------------------------------------------------------------
    override fun addBlock(block: ByteBufferL) {
        ensureOpen()
        val idx = pendingInStripe
        require(idx in 0 until k) { "too many blocks for current stripe: $idx >= $k" }
        require(block.isDirect) { "Block must be direct" }
        require(block.remaining == blockSize) { "Block must be exactly $blockSize bytes" }

        stage[idx] = block
        pendingInStripe = idx + 1

        if (pendingInStripe == k) {
            sealStripe()
        }
    }

    /** Seals current stripe: tail-CRC stamp, parity encode, positional lane writes, enqueue commit. */
    private fun sealStripe() {
        val stripeIndex = lastSealedStripe + 1
        val off = stripeIndex * blockSize.toLong()

        val dataL: Array<ByteBufferL> = stage.requireFull(k)

        // --- CRC32C stamp (ByteBufferL only) ---
        run {
            for (i in 0 until k) {
                val buf = dataL[i]
                val crc = buf.crc32cRange(0, blockSize - 4)
                buf.at(blockSize - 4).i32 = crc
            }
        }

        // --- Parity encode ---
        val parityBufs: Array<ByteBufferL> =
            if (m > 0) Array(m) { pool.get(blockSize).clear() } else emptyArray()
        try {
            val parityStart = System.nanoTime()
            coder.encodeInto(dataL, parityBufs)
            val parityEnd = System.nanoTime()
            parityMicros.addAndGet((parityEnd - parityStart) / 1000)

            // --- Lane writes (positional; contiguous per lane) ---
            val writeStart = System.nanoTime()
            // data lanes
            for (lane in 0 until k) {
                val ch = dataCh[lane]
                positionalWriteFully(ch, dataL[lane], off)
                bytesWrittenData.addAndGet(blockSize.toLong())
            }
            // parity lanes (if any)
            if (m > 0) {
                for (pm in 0 until m) {
                    val ch = parityCh[pm]
                    positionalWriteFully(ch, parityBufs[pm], off)
                    bytesWrittenParity.addAndGet(blockSize.toLong())
                }
            }
            val writeEnd = System.nanoTime()
            laneWriteMicros.addAndGet((writeEnd - writeStart) / 1000)
        } finally {
            // recycle parity buffers back to pool even if encode/write throws
            if (m > 0) for (b in parityBufs) pool.release(b)
        }

        // Reset staging
        for (i in 0 until k) stage[i] = null
        pendingInStripe = 0

        // Mark sealed & enqueue for commit
        lastSealedStripe = stripeIndex
        sealedStripes.incrementAndGet()
        commitQ.add(stripeIndex)
        pendingCommits.incrementAndGet()

        // Group-commit policy check
        maybeCommit()
    }

    /** FileChannel.position(off) + ByteBufferL.writeFully(...). */
    private fun positionalWriteFully(ch: FileChannel, block: ByteBufferL, off: Long) {
        ch.position(off)
        val dup = block.duplicate().position(0)
        val wrote = dup.writeFully(ch, blockSize)
        if (wrote != blockSize) throw IOException("short write: $wrote/$blockSize")
    }

    private fun maybeCommit() {
        val n = pendingCommits.get()
        if (n == 0) return
        val policyBlocks = flushPolicy.maxBlocks
        val policyMicros = flushPolicy.maxMicros

        val doByBlocks = (policyBlocks > 0 && n >= policyBlocks)
        val doByTime = (policyMicros > 0L &&
                (System.nanoTime() - lastCommitStartNanos) >= policyMicros * 1_000)

        if (doByBlocks || doByTime) {
            lastCommitStartNanos = System.nanoTime()
            performCommit(FlushMode.ASYNC.takeIf { fastMode } ?: FlushMode.SYNC)
        }
    }

    override fun flush(mode: FlushMode): CommitTicket {
        ensureOpen()
        if (pendingCommits.get() == 0) {
            return CommitTicket(lastSealedStripe, CompletableFuture.completedFuture(lastDurableStripe))
        }
        lastCommitStartNanos = System.nanoTime()
        return performCommit(mode)
    }

    private fun performCommit(mode: FlushMode): CommitTicket {
        val upto = drainCommitUpto()
        if (upto < 0) return CommitTicket(lastSealedStripe, CompletableFuture.completedFuture(lastDurableStripe))

        return when (mode) {
            FlushMode.SYNC -> {
                val t0 = System.nanoTime()
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
        var drained = 0
        var x = commitQ.poll()
        while (x != null) {
            last = x
            drained++
            x = commitQ.poll()
        }
        if (drained > 0) pendingCommits.addAndGet(-drained)
        return last
    }

    private fun forceAll() {
        for (ch in dataCh) ch.force(false)
        for (ch in parityCh) ch.force(false)
    }

    override fun sealIfComplete(): Boolean {
        if (pendingInStripe == k) {
            sealStripe(); return true
        }
        return false
    }

    override fun seek(stripeIndex: Long) {
        require(stripeIndex >= -1) { "invalid stripeIndex" }
        lastSealedStripe = stripeIndex
        lastDurableStripe = min(lastDurableStripe, stripeIndex)
        pendingInStripe = 0
        commitQ.clear()
        pendingCommits.set(0)
    }

    override fun recover(): RecoveryResult {
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
                if (pendingInStripe == k) sealStripe()

                val barrier = CompletableFuture<Void>()
                (flushPolicy.executor ?: fsyncExec).execute {
                    barrier.complete(null)
                }
                barrier.join()

                if (pendingCommits.get() > 0) {
                    performCommit(FlushMode.SYNC)
                }

                if (lastSealedStripe >= 0) {
                    val exactSize = (lastSealedStripe + 1) * blockSize.toLong()
                    for (ch in dataCh) if (ch.size() != exactSize) ch.truncate(exactSize)
                    for (ch in parityCh) if (ch.size() != exactSize) ch.truncate(exactSize)
                    dataCh.firstOrNull()?.force(true)
                } else {
                    dataCh.firstOrNull()?.force(true)
                }

                for (ch in dataCh) ch.force(false)
                for (ch in parityCh) ch.force(false)

            } finally {
                var first: Throwable? = null
                fun swallow(t: Throwable) {
                    if (first == null) first = t
                }
                (dataCh + parityCh).forEach {
                    try {
                        it.close()
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

    private fun Array<ByteBufferL?>.requireFull(n: Int): Array<ByteBufferL> {
        check(size >= n) { "staging size < k" }
        @Suppress("UNCHECKED_CAST")
        val out = Array(n) { i ->
            this[i] ?: error("missing block for lane=$i")
        }
        return out
    }

    private fun alignUp(x: Long, pow2: Long): Long {
        require(pow2 > 0 && (pow2 and (pow2 - 1)) == 0L)
        return (x + (pow2 - 1)) and (pow2 - 1).inv()
    }
}
