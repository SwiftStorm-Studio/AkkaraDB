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

package dev.swiftstorm.akkaradb.engine.sstable.bs

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.bytesEqual
import dev.swiftstorm.akkaradb.common.lexCompare
import dev.swiftstorm.akkaradb.engine.sstable.BlockSearcher

/* =======================================================================
 * StandardBlockSearcher
 * -----------------------------------------------------------------------
 * No dependency on AkkBlockUnpacker. Parses the block payload bounds and
 * builds an offset list in one pass, then performs binary search via
 * ByteBufferL.readRecord32(). Safe and self-contained.
 * ======================================================================= */
object StandardBlockSearcher : BlockSearcher {

    override fun find(blockBuf32k: ByteBufferL, key: ByteBufferL): ByteBufferL? {
        val (start, end) = payloadBounds(blockBuf32k)
        if (start >= end) return null

        // Build offsets (one pass, O(n))
        val offs = buildOffsets(blockBuf32k, start, end)
        if (offs.isEmpty()) return null

        // Binary search by key (O(log n))
        var lo = 0
        var hi = offs.size
        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            val (_, pair) = blockBuf32k.readRecord32(offs[mid])
            val (k, _) = pair
            val cmp = lexCompare(k, key)
            if (cmp < 0) lo = mid + 1 else hi = mid
        }
        if (lo >= offs.size) return null

        val (_, pair) = blockBuf32k.readRecord32(offs[lo])
        val (k, v) = pair
        return if (bytesEqual(k, key)) v else null
    }

    override fun iter(blockBuf32k: ByteBufferL, startKey: ByteBufferL): Sequence<Pair<ByteBufferL, ByteBufferL>> = sequence {
        val (start, end) = payloadBounds(blockBuf32k)
        if (start >= end) return@sequence

        val offs = buildOffsets(blockBuf32k, start, end)
        if (offs.isEmpty()) return@sequence

        // lower_bound
        var lo = 0
        var hi = offs.size
        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            val (_, pair) = blockBuf32k.readRecord32(offs[mid])
            val (k, _) = pair
            val cmp = lexCompare(k, startKey)
            if (cmp < 0) lo = mid + 1 else hi = mid
        }

        var i = lo
        while (i < offs.size) {
            val (_, pair) = blockBuf32k.readRecord32(offs[i])
            val (k, v) = pair
            yield(k to v)
            i++
        }
    }

    /** Return (payloadStart, payloadEndExclusive). Validates bounds. */
    private fun payloadBounds(block: ByteBufferL): Pair<Int, Int> {
        val payloadLen = block.at(0).i32
        require(payloadLen in 0..PAYLOAD_LIMIT) { "invalid payloadLen=$payloadLen" }
        val start = 4
        val end = start + payloadLen
        require(end <= BLOCK_SIZE - 4) { "payload overruns block: end=$end > ${BLOCK_SIZE - 4}" }
        return start to end
    }

    /** Scan once and collect record start offsets. */
    private fun buildOffsets(block: ByteBufferL, start: Int, end: Int): IntArray {
        var p = start
        var n = 0
        // First pass: count
        while (p + 32 <= end) {
            val h = block.readHeader32(p)
            val total = 32 + h.kLen + h.vLen.toIntExact()
            val next = p + total
            if (next > end) break
            n++
            p = next
        }
        if (n == 0) return IntArray(0)

        val offs = IntArray(n)
        p = start
        var i = 0
        while (i < n) {
            offs[i] = p
            val h = block.readHeader32(p)
            p += 32 + h.kLen + h.vLen.toIntExact()
            i++
        }
        return offs
    }
}