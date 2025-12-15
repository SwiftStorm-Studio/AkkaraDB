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
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.ReaderMetricsSnapshot
import dev.swiftstorm.akkaradb.format.api.ReaderRecoveryResult
import dev.swiftstorm.akkaradb.format.api.StripeReader
import java.io.EOFException
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min

class AkkStripeReader(
    override val k: Int,
    override val m: Int,
    laneDir: Path,
    private val pool: BufferPool,
    private val coder: ParityCoder,
    private val laneFilePrefixData: String = "data_",
    private val laneFilePrefixParity: String = "parity_",
    private val laneFileExtData: String = ".akd",
    private val laneFileExtParity: String = ".akp"
) : StripeReader {

    init {
        require(k >= 1) { "k must be >=1" }
        require(m >= 0) { "m must be >=0" }
        require(coder.parityCount == m) { "coder.parityCount(${coder.parityCount}) != m($m)" }
        Files.createDirectories(laneDir)
    }

    override val blockSize: Int = BLOCK_SIZE
    override var nextStripeIndex: Long = 0; private set

    private val dataCh: Array<FileChannel> =
        Array(k) { open(laneDir.resolve("$laneFilePrefixData${it}$laneFileExtData")) }
    private val parityCh: Array<FileChannel> =
        if (m > 0) Array(m) { open(laneDir.resolve("$laneFilePrefixParity${it}$laneFileExtParity")) } else emptyArray()

    private val closed = AtomicBoolean(false)

    // ---- metrics ----
    private val stripesReturned = AtomicLong(0)
    private val bytesReadData = AtomicLong(0)
    private val bytesReadParity = AtomicLong(0)
    private val readMicros = AtomicLong(0)
    private val verifyMicros = AtomicLong(0)
    private val reconstructMicros = AtomicLong(0)

    private fun open(p: Path): FileChannel = FileChannel.open(p, READ)

    override fun readStripe(): StripeReader.Stripe? {
        ensureOpen()
        val stripe = nextStripeIndex
        val off = stripe * blockSize.toLong()

        val dataBufs = arrayOfNulls<ByteBufferL>(k)
        val parityBufs = arrayOfNulls<ByteBufferL>(m)

        fun releaseAll() {
            for (i in 0 until k) dataBufs[i]?.let(pool::release)
            for (j in 0 until m) parityBufs[j]?.let(pool::release)
        }

        try {
            val tRead0 = System.nanoTime()

            // ---- read data lanes (positional) ----
            var anyEofAtHead = false
            val missingData = BooleanArray(k)

            var lane = 0
            while (lane < k) {
                val buf = pool.get(blockSize).clear()
                val n = positionalReadFully(dataCh[lane], buf, off)
                if (n < 0) { // EOF at head of lane
                    anyEofAtHead = true
                    pool.release(buf)
                    break
                }

                var corrupt = false
                if (n == blockSize) {
                    val stored = buf.at(blockSize - 4).i32
                    val calc = buf.crc32cRange(0, blockSize - 4)
                    corrupt = (calc != stored)
                }

                if (n != blockSize || corrupt) missingData[lane] = true
                dataBufs[lane] = buf
                bytesReadData.addAndGet(maxOf(0, n).toLong())
                lane++
            }

            if (anyEofAtHead) {
                releaseAll()
                return null
            }

            // ---- read parity lanes (positional) ----
            // 将来: 「データが全てOKならパリティを読まない」最適化を入れる余地あり（API互換のため今回は従来通り読む）
            val missingParity = BooleanArray(m)
            var pm = 0
            while (pm < m) {
                val buf = pool.get(blockSize).clear()
                val n = positionalReadFully(parityCh[pm], buf, off)
                if (n != blockSize) missingParity[pm] = true
                parityBufs[pm] = buf
                bytesReadParity.addAndGet(maxOf(0, n).toLong())
                pm++
            }

            val tRead1 = System.nanoTime()
            readMicros.addAndGet((tRead1 - tRead0) / 1_000)

            // ---- fast path: nothing missing ----
            var anyMissing = false
            run {
                var i = 0
                while (i < k) {
                    if (missingData[i]) {
                        anyMissing = true; break
                    }; i++
                }
                if (!anyMissing) {
                    var j = 0
                    while (j < m) {
                        if (missingParity[j]) {
                            anyMissing = true; break
                        }; j++
                    }
                }
            }
            if (!anyMissing) {
                stripesReturned.incrementAndGet()
                nextStripeIndex = stripe + 1
                // toList() 相当（N固定の軽い変換）
                val payloads = listOf(*dataBufs.requireNoNulls())
                val lanes = ArrayList<ByteBufferL>(k + m).apply {
                    addAll(payloads)
                    if (m > 0) addAll(listOf(*parityBufs.requireNoNulls()))
                }
                return StripeReader.Stripe(payloads, lanes, pool)
            }

            // ---- collect erasures ----
            var lostDataCount = 0
            var lostParityCount = 0
            var i = 0
            while (i < k) {
                if (missingData[i]) lostDataCount++; i++
            }
            var j = 0
            while (j < m) {
                if (missingParity[j]) lostParityCount++; j++
            }

            if (lostDataCount + lostParityCount > m) {
                releaseAll()
                throw IOException("unrecoverable stripe=$stripe (erasures=${lostDataCount + lostParityCount}, m=$m)")
            }

            val lostDataIdx = IntArray(lostDataCount)
            val lostParityIdx = IntArray(lostParityCount)
            var di = 0; i = 0
            while (i < k) {
                if (missingData[i]) lostDataIdx[di++] = i; i++
            }
            var pj = 0; j = 0
            while (j < m) {
                if (missingParity[j]) lostParityIdx[pj++] = j; j++
            }

            // ---- reconstruct & verify ----
            if (lostDataCount > 0 || lostParityCount > 0) {
                val tR0 = System.nanoTime()

                val presentData = arrayOfNulls<ByteBufferL>(k)
                val presentParity = arrayOfNulls<ByteBufferL>(m)
                i = 0; while (i < k) {
                    presentData[i] = if (missingData[i]) null else dataBufs[i]; i++
                }
                j = 0; while (j < m) {
                    presentParity[j] = if (missingParity[j]) null else parityBufs[j]; j++
                }

                val outData = Array(lostDataCount) { pool.get(blockSize) }
                val outParity = Array(lostParityCount) { pool.get(blockSize) }

                val repaired = coder.reconstruct(
                    lostDataIdx, lostParityIdx,
                    presentData,
                    presentParity,
                    outData, outParity
                )

                // swap-in
                i = 0
                while (i < outData.size) {
                    val idx = lostDataIdx[i]
                    pool.release(dataBufs[idx]!!)
                    dataBufs[idx] = outData[i]
                    i++
                }
                j = 0
                while (j < outParity.size) {
                    val idx = lostParityIdx[j]
                    pool.release(parityBufs[idx]!!)
                    parityBufs[idx] = outParity[j]
                    j++
                }

                val tR1 = System.nanoTime()
                reconstructMicros.addAndGet((tR1 - tR0) / 1_000)

                if (m > 0) {
                    val tV0 = System.nanoTime()
                    val ok = coder.verify(
                        dataBufs.requireNoNulls(),
                        parityBufs.requireNoNulls()
                    )
                    val tV1 = System.nanoTime()
                    verifyMicros.addAndGet((tV1 - tV0) / 1_000)
                    if (!ok) {
                        releaseAll()
                        throw IOException("parity verify failed after reconstruction (stripe=$stripe, repaired=$repaired)")
                    }
                }
            }

            stripesReturned.incrementAndGet()
            nextStripeIndex = stripe + 1
            val payloads = listOf(*dataBufs.requireNoNulls())
            val lanes = ArrayList<ByteBufferL>(k + m).apply {
                addAll(payloads)
                if (m > 0) addAll(listOf(*parityBufs.requireNoNulls()))
            }
            return StripeReader.Stripe(payloads, lanes, pool)

        } catch (eof: EOFException) {
            releaseAll(); return null
        } catch (t: Throwable) {
            releaseAll(); throw t
        }
    }

    override fun seek(stripeIndex: Long) {
        require(stripeIndex >= 0) { "invalid stripeIndex" }
        nextStripeIndex = stripeIndex
    }

    override fun recover(): ReaderRecoveryResult {
        val stripesData = dataCh.minOfOrNull { (it.size() / blockSize) } ?: 0L
        val stripesParity = if (m > 0) parityCh.minOfOrNull { (it.size() / blockSize) } ?: 0L else stripesData
        val minStripes = min(stripesData, stripesParity)
        val lastSealed = if (minStripes == 0L) -1L else (minStripes - 1)
        val lastDurable = lastSealed
        val truncatedTail = dataCh.any { (it.size() % blockSize) != 0L } ||
                parityCh.any { (it.size() % blockSize) != 0L }
        nextStripeIndex = 0
        return ReaderRecoveryResult(lastSealed, lastDurable, truncatedTail)
    }

    override fun metrics(): ReaderMetricsSnapshot = ReaderMetricsSnapshot(
        stripesReturned.get(),
        bytesReadData.get(),
        bytesReadParity.get(),
        readMicros.get(),
        verifyMicros.get(),
        reconstructMicros.get()
    )

    override fun close() {
        if (closed.compareAndSet(false, true)) {
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

    private fun ensureOpen() {
        check(!closed.get()) { "reader closed" }
    }

    /**
     * Positional read that does not mutate channel position.
     * Returns bytes read; -1 means EOF at head.
     */
    @Suppress("DEPRECATION")
    private fun positionalReadFully(ch: FileChannel, block: ByteBufferL, off: Long): Int {
        val dst = block.duplicate().position(0)
        dst.limit(blockSize)
        var read = 0
        while (read < blockSize) {
            val n = ch.read(dst.rawDuplicate(), off + read)
            if (n < 0) return if (read == 0) -1 else read
            if (n == 0) continue
            read += n
        }
        return read
    }
}