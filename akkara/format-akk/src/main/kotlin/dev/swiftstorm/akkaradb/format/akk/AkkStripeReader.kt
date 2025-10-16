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
    }

    override val blockSize: Int = BLOCK_SIZE
    override var nextStripeIndex: Long = 0; private set

    private val dataCh: Array<FileChannel>
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

    init {
        Files.createDirectories(laneDir)
        dataCh = Array(k) { open(laneDir.resolve("$laneFilePrefixData${it}$laneFileExtData")) }
    }

    private fun open(p: Path): FileChannel = FileChannel.open(p, READ)

    override fun readStripe(): StripeReader.Stripe? {
        ensureOpen()
        val stripe = nextStripeIndex
        val off = stripe * blockSize.toLong()

        val dataBufs = ArrayList<ByteBufferL>(k)
        val parityBufs = ArrayList<ByteBufferL>(m)

        fun releaseAll() {
            dataBufs.forEach(pool::release)
            parityBufs.forEach(pool::release)
        }

        try {
            val tRead0 = System.nanoTime()
            var anyEofAtHead = false
            val missingData = BooleanArray(k)
            val missingParity = BooleanArray(m)

            // ---- read data lanes (positional), then CRC verify using ByteBufferL only ----
            run {
                for (lane in 0 until k) {
                    val buf = pool.get(blockSize).clear()
                    dataCh[lane].position(off)
                    var n = 0
                    try {
                        n = buf.readFully(dataCh[lane], blockSize)
                    } catch (eof: EOFException) {
                        // EOF at head of stripe
                        anyEofAtHead = true
                        pool.release(buf)
                        break
                    }

                    val full = (n == blockSize)
                    var corrupt = false
                    if (full) {
                        val stored = buf.at(blockSize - 4).i32
                        val calc = buf.crc32cRange(0, blockSize - 4)
                        corrupt = (calc != stored)
                    }

                    if (!full || corrupt) missingData[lane] = true
                    dataBufs += buf
                    bytesReadData.addAndGet(maxOf(0, n).toLong())
                }
            }
            if (anyEofAtHead) {
                releaseAll()
                return null
            }

            // ---- read parity lanes (positional) ----
            for (pm in 0 until m) {
                val buf = pool.get(blockSize).clear()
                parityCh[pm].position(off)
                var n = 0
                try {
                    n = buf.readFully(parityCh[pm], blockSize)
                } catch (eof: EOFException) {
                    n = -1
                }
                if (n != blockSize) missingParity[pm] = true
                parityBufs += buf
                bytesReadParity.addAndGet(maxOf(0, n).toLong())
            }

            val tRead1 = System.nanoTime()
            readMicros.addAndGet((tRead1 - tRead0) / 1000)

            // ---- lost index lists ----
            val lostDataIdx = IntArray(missingData.count { it })
            val lostParityIdx = IntArray(missingParity.count { it })
            run {
                var p = 0
                for (i in 0 until k) if (missingData[i]) lostDataIdx[p++] = i
                p = 0
                for (j in 0 until m) if (missingParity[j]) lostParityIdx[p++] = j
            }

            // too many erasures
            if (lostDataIdx.size + lostParityIdx.size > m) {
                releaseAll()
                throw IOException("unrecoverable stripe=$stripe (erasures=${lostDataIdx.size + lostParityIdx.size}, m=$m)")
            }

            // ---- reconstruct & verify (if needed) ----
            if (lostDataIdx.isNotEmpty() || lostParityIdx.isNotEmpty()) {
                val tR0 = System.nanoTime()

                val presentData: Array<ByteBufferL?> = Array(k) { i -> if (missingData[i]) null else dataBufs[i] }
                val presentParity: Array<ByteBufferL?> = Array(m) { j -> if (missingParity[j]) null else parityBufs[j] }

                val outData: Array<ByteBufferL> = Array(lostDataIdx.size) { pool.get(blockSize) }
                val outParity: Array<ByteBufferL> = Array(lostParityIdx.size) { pool.get(blockSize) }

                val repaired = coder.reconstruct(
                    lostDataIdx, lostParityIdx,
                    presentData, presentParity,
                    outData, outParity
                )

                for (t in outData.indices) {
                    val di = lostDataIdx[t]
                    dataBufs[di] = outData[t]
                }
                for (t in outParity.indices) {
                    val pj = lostParityIdx[t]
                    parityBufs[pj] = outParity[t]
                }

                val tR1 = System.nanoTime()
                reconstructMicros.addAndGet((tR1 - tR0) / 1000)

                if (m > 0) {
                    val tV0 = System.nanoTime()
                    val ok = coder.verify(dataBufs.toTypedArray(), parityBufs.toTypedArray())
                    val tV1 = System.nanoTime()
                    verifyMicros.addAndGet((tV1 - tV0) / 1000)
                    if (!ok) {
                        releaseAll()
                        throw IOException("parity verify failed after reconstruction (stripe=$stripe, repaired=$repaired)")
                    }
                }
            }

            stripesReturned.incrementAndGet()
            nextStripeIndex = stripe + 1
            val payloads = dataBufs.toList()
            val laneBlocks = ArrayList<ByteBufferL>(k + m).apply { addAll(dataBufs); addAll(parityBufs) }
            return StripeReader.Stripe(payloads, laneBlocks, pool)

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
            if (first != null) throw first!!
        }
    }

    private fun ensureOpen() {
        check(!closed.get()) { "reader closed" }
    }
}
