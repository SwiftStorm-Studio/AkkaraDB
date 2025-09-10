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

import dev.swiftstorm.akkaradb.common.*
import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.format.akk.parity.RSErrorCorrectingParityCoder
import dev.swiftstorm.akkaradb.format.akk.parity.RSParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.StripeReader
import dev.swiftstorm.akkaradb.format.api.StripeReader.Stripe
import dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException
import java.nio.channels.ReadableByteChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import java.util.zip.CRC32C

/**
 * Reader for a *stripe* in the append-only segment.
 *
 * Ownership & Lifetime:
 *  * The returned [Stripe] holds both payload slices and the backing lane blocks.
 *  * Caller **must** call [Stripe.close] after use to release buffers back to the pool.
 */
class AkkStripeReader(
    baseDir: Path,
    private val k: Int,
    private val parityCoder: ParityCoder? = null,
) : StripeReader, AutoCloseable {

    /* ───────── file handles ───────── */
    private val dataCh = (0 until k).map { Files.newByteChannel(baseDir.resolve("data_$it.akd"), READ) }
    private val parityCh = (0 until (parityCoder?.parityCount ?: 0))
        .map { Files.newByteChannel(baseDir.resolve("parity_$it.akp"), READ) }

    /* ───────── helpers ───────── */
    private val unpacker = AkkBlockUnpacker()
    private val pool = Pools.io()
    private val crc32c = CRC32C()

    /**
     * Reads one stripe and returns a [Stripe] with payload slices and lane blocks.
     * Returns `null` on EOF.
     */
    override fun readStripe(): Stripe? {
        val laneBlocks = MutableList<ByteBufferL?>(k) { null }
        val parityBufs = ArrayList<ByteBufferL?>(parityCoder?.parityCount ?: 0)

        try {
            // 1) data lanes
            for ((idx, ch) in dataCh.withIndex()) {
                val blk = pool.get(BLOCK_SIZE)
                if (readFullBlock(ch, blk)) laneBlocks[idx] = blk
                else pool.release(blk) // EOF/partial
            }
            if (laneBlocks.all { it == null }) return null // true EOF

            val missingIdx = laneBlocks.withIndex().filter { it.value == null }.map { it.index }
            val missingCnt = missingIdx.size
            val pCount = parityCoder?.parityCount ?: 0
            if (missingCnt > pCount) {
                laneBlocks.forEach { it?.let(pool::release) }
                throw CorruptedBlockException("Unrecoverable stripe – missing=$missingCnt, parity=$pCount")
            }

            // 2) recover if needed
            if (missingCnt > 0 && parityCoder != null) {
                for (pch in parityCh) {
                    val buf = pool.get(BLOCK_SIZE)
                    if (readFullBlock(pch, buf)) parityBufs += buf
                    else {
                        pool.release(buf); parityBufs += null
                    }
                }

                val erasureOnly = laneBlocks.asSequence()
                    .filterNotNull()
                    .all { verifyDataBlockCrc(it) }

                when (val coder = parityCoder) {
                    is RSParityCoder -> {
                        if (!erasureOnly) {
                            laneBlocks.forEach { it?.let(pool::release) }
                            parityBufs.forEach { it?.let(pool::release) }
                            throw CorruptedBlockException(
                                "Non-erasure corruption detected. RSParityCoder cannot correct unknown-position errors."
                            )
                        }
                        val recovered = coder.decodeAllErasures(laneBlocks, parityBufs)
                        for ((idx, buf) in recovered) laneBlocks[idx] = buf
                    }

                    is RSErrorCorrectingParityCoder -> {
                        val recovered = coder.decodeWithErrors(
                            presentData = laneBlocks,
                            presentParity = parityBufs,
                            knownErasures = missingIdx.toIntArray()
                        )
                        for ((idx, buf) in recovered) laneBlocks[idx] = buf
                    }

                    else -> {
                        if (!erasureOnly) {
                            laneBlocks.forEach { it?.let(pool::release) }
                            parityBufs.forEach { it?.let(pool::release) }
                            throw CorruptedBlockException("Non-erasure corruption detected; unknown coder cannot correct errors.")
                        }
                        for (i in laneBlocks.indices) {
                            if (laneBlocks[i] == null) {
                                laneBlocks[i] = parityCoder.decode(i, laneBlocks, parityBufs)
                            }
                        }
                    }
                }
            }

            // 3) cleanup parity buffers
            parityBufs.forEach { it?.let(pool::release) }

            // 4) unpack payloads
            val nonNullBlocks = laneBlocks.map { it ?: throw CorruptedBlockException("Corrupted stripe: unrecoverable lane") }
            val payloads = ArrayList<ByteBufferL>(64)
            for (blk in nonNullBlocks) {
                unpacker.unpackInto(blk, payloads)
            }
            return Stripe(payloads, nonNullBlocks, pool)

        } catch (e: Exception) {
            laneBlocks.forEach { it?.let(pool::release) }
            parityBufs.forEach { it?.let(pool::release) }
            throw e
        }
    }

    /** Read exactly BLOCK_SIZE bytes into [blk]. Returns true on success (and flips), false otherwise. */
    private fun readFullBlock(ch: ReadableByteChannel, blk: ByteBufferL): Boolean {
        val bb = blk.getByteBuffer()
        bb.clear()
        var read = 0
        var zeroStreak = 0
        while (read < BLOCK_SIZE) {
            val n = ch.read(bb)
            if (n < 0) break
            if (n == 0) {
                if (++zeroStreak >= 3) break
                Thread.onSpinWait() // JDK9+
                continue
            }
            zeroStreak = 0
            read += n
        }
        return if (read == BLOCK_SIZE) {
            blk.flip(); true
        } else false
    }

    private fun verifyDataBlockCrc(block: ByteBufferL): Boolean {
        return try {
            if (block.capacity != BLOCK_SIZE) return false
            val base = block.asReadOnlyByteBuffer()
            val payloadLen = base.getInt(0) // LE
            if (payloadLen < 0 || payloadLen > PAYLOAD_LIMIT) return false

            crc32c.reset()
            val region = base.duplicate().apply { position(0); limit(4 + payloadLen) }
            crc32c.update(region)
            val calc = crc32c.value.toInt()
            val stored = base.getInt(PAYLOAD_LIMIT + 4) // end-4
            calc == stored
        } catch (_: Throwable) {
            false
        }
    }

    override fun close() {
        (dataCh + parityCh).forEach { it.close() }
    }

    data class StripeHit(
        val record: Record,
        val stripeId: Long,
        val records: List<Record>,
        val stripe: Stripe
    ) : AutoCloseable {
        override fun close() = stripe.close()
    }

    fun searchLatestStripe(key: ByteBufferL, untilStripe: Long): StripeHit? {
        var idx = 0L
        val rr = AkkRecordReader
        while (true) {
            val stripe = readStripe() ?: break
            var hit: Record? = null
            val recs = ArrayList<Record>()
            for (p in stripe.payloads) {
                val r = rr.read(p.duplicate())
                recs += r
                if (hit == null && r.key.compareTo(key) == 0) hit = r
            }
            if (hit != null) {
                val stripeId = untilStripe - idx
                return StripeHit(hit, stripeId, recs, stripe)
            } else {
                // Caller will automatically release the stripe buffers
            }
            idx++
        }
        return null
    }
}
