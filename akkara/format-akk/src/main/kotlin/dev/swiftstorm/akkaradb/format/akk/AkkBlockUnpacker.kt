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

import dev.swiftstorm.akkaradb.common.AKHdr32
import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.format.api.BlockUnpacker
import dev.swiftstorm.akkaradb.format.api.RecordCursor
import dev.swiftstorm.akkaradb.format.api.RecordView

/**
 * High-throughput unpacker for blocks with AKHdr32-based records.
 * Validates frame bounds, iterates record-by-record, and returns zero-copy slices.
 */
class AkkBlockUnpacker : BlockUnpacker {
    private fun verifyCrc32c(block: ByteBufferL) {
        val stored = block.at(BLOCK_SIZE - 4).i32
        val calc = block.crc32cRange(0, BLOCK_SIZE - 4)
        require(calc == stored) { "crc32c mismatch: stored=$stored calc=$calc" }
    }

    override fun cursor(block: ByteBufferL): RecordCursor {
        verifyCrc32c(block)
        val payloadLen = block.at(0).i32
        require(payloadLen in 0..PAYLOAD_LIMIT) { "invalid payloadLen=$payloadLen" }
        val start = 4
        val end = start + payloadLen
        require(end <= BLOCK_SIZE - 4) { "payload overruns block: end=$end > ${BLOCK_SIZE - 4}" }

        return object : RecordCursor {
            var p = start
            override fun hasNext(): Boolean = p < end
            override fun tryNext(): RecordView? {
                if (!hasNext()) return null
                if (p + AKHdr32.SIZE > end) return null
                val h = block.readHeader32(p)
                val kLen = h.kLen
                val vLenInt = h.vLen.toIntExact()
                val total = AKHdr32.SIZE + kLen + vLenInt
                val next = p + total
                if (next > end) return null

                val keyOff = p + AKHdr32.SIZE
                val valOff = keyOff + kLen
                val keySlice = block.sliceAt(keyOff, kLen)
                val valSlice = block.sliceAt(valOff, vLenInt)

                val view = RecordView(
                    key = keySlice,
                    value = valSlice,
                    seq = h.seq,
                    flags = h.flags,
                    kLen = kLen,
                    vLen = h.vLen,
                    totalLen = total
                )
                p = next
                return view
            }

            override fun next(): RecordView = tryNext() ?: error("no more records or malformed tail at offset=$p")
        }
    }

    override fun unpackInto(block: ByteBufferL, out: MutableList<RecordView>) {
        out.clear()
        val cur = cursor(block)
        while (true) {
            val v = cur.tryNext() ?: break
            out += v
        }
    }
}
