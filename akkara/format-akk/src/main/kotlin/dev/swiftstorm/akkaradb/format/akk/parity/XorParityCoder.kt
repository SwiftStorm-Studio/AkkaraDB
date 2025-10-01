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

@file:Suppress("unused", "DuplicatedCode")

package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityKind

/**
 * XOR parity coder (m = 1).
 *
 * - **No allocations on the hot path**
 * - **Absolute-index I/O** with [ByteBufferL.i64] to avoid cursor mutations
 * - Operates in 64-bit chunks, then a small byte tail
 * - Requires each buffer to have at least [blockSize] bytes remaining
 */
class XorParityCoder(
    override val blockSize: Int = BLOCK_SIZE
) : ParityCoder {

    override val parityCount: Int = 1
    override val kind: ParityKind = ParityKind.XOR
    override val supportsErrorCorrection: Boolean = true // up to 1 erasure (XOR)

    override fun encodeInto(
        data: Array<ByteBufferL>,
        parityOut: Array<ByteBufferL>
    ) {
        require(parityOut.size == parityCount) { "parityOut.size != 1" }
        require(data.isNotEmpty()) { "data is empty" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parityOut)

        val p = parityOut[0]
        val pPos = p.position
        val longs = (blockSize ushr 3)
        val tail = blockSize - (longs shl 3)

        // 64-bit wide XOR
        var off = 0
        while (off < (longs shl 3)) {
            var acc = 0L
            // XOR across all data lanes at this offset
            for (d in data) {
                val dPos = d.position
                acc = acc xor d.at(dPos + off).i64
            }
            p.at(pPos + off).i64 = acc
            off += 8
        }

        // tail bytes
        var t = 0
        while (t < tail) {
            var b = 0
            val idx = off + t
            for (d in data) {
                val dPos = d.position
                b = b xor d.at(dPos + idx).i8
            }
            p.at(pPos + idx).i8 = b
            t++
        }
    }

    override fun verify(
        data: Array<ByteBufferL>,
        parity: Array<ByteBufferL>
    ): Boolean {
        require(parity.size == parityCount) { "parity.size != 1" }
        require(data.isNotEmpty()) { "data is empty" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parity)

        val p = parity[0]
        val pPos = p.position
        val longs = (blockSize ushr 3)
        val tail = blockSize - (longs shl 3)

        var off = 0
        while (off < (longs shl 3)) {
            var acc = 0L
            for (d in data) {
                val dPos = d.position
                acc = acc xor d.at(dPos + off).i64
            }
            if (acc != p.at(pPos + off).i64) return false
            off += 8
        }

        var t = 0
        while (t < tail) {
            var b = 0
            for (d in data) {
                val base = d.position + off
                b = b xor d.at(base + t).i8
            }
            if (b != p.at(pPos + off + t).i8) return false
            t++
        }
        return true
    }

    override fun reconstruct(
        lostDataIdx: IntArray,
        lostParityIdx: IntArray,
        data: Array<ByteBufferL?>,
        parity: Array<ByteBufferL?>,
        outData: Array<ByteBufferL>,
        outParity: Array<ByteBufferL>
    ): Int {
        val losses = lostDataIdx.size + lostParityIdx.size
        require(losses <= 1) { "XOR can correct at most 1 erasure, got $losses" }
        require(parity.size == 1) { "parity array must have size 1" }
        requireAllHave(blockSize, outData)
        requireAllHave(blockSize, outParity)

        // lost parity -> recompute parity from present data
        if (lostParityIdx.isNotEmpty()) {
            require(lostParityIdx.size == 1 && lostParityIdx[0] == 0) { "invalid lostParityIdx for XOR" }
            val presentData = ArrayList<ByteBufferL>(data.size)
            for (d in data) presentData += requireNotNull(d) { "reconstruct parity requires all data present" }
            encodeInto(presentData.toTypedArray(), arrayOf(outParity[0]))
            return 1
        }

        // lost data -> XOR of all present data plus existing parity
        if (lostDataIdx.isNotEmpty()) {
            require(lostDataIdx.size == 1) { "only one lost data block can be reconstructed" }
            val li = lostDataIdx[0]
            require(li in data.indices) { "lost data index out of range: $li" }
            val p = requireNotNull(parity[0]) { "parity block required to reconstruct data" }
            val out = outData[0]

            requireAllHave(blockSize, arrayOf(out))
            val longs = (blockSize ushr 3)
            val tail = blockSize - (longs shl 3)

            val outPos = out.position
            val pPos = p.position

            var off = 0
            while (off < (longs shl 3)) {
                var acc = p.at(pPos + off).i64
                // XOR all present data except the lost index
                for ((i, d) in data.withIndex()) {
                    if (i == li) continue
                    val buf = requireNotNull(d) { "only one data loss supported for XOR" }
                    val dPos = buf.position
                    acc = acc xor buf.at(dPos + off).i64
                }
                out.at(outPos + off).i64 = acc
                off += 8
            }

            var t = 0
            while (t < tail) {
                var b = p.at(pPos + off + t).i8
                for ((i, d) in data.withIndex()) {
                    if (i == li) continue
                    val buf = requireNotNull(d)
                    b = b xor buf.at(buf.position + off + t).i8
                }
                out.at(outPos + off + t).i8 = b
                t++
            }
            return 1
        }

        // nothing to do
        return 0
    }

    override fun attachScratch(buf: ByteBufferL?) {}

    override fun scratchBytesHint(k: Int, m: Int): Int = 0

    private fun requireAllHave(size: Int, arr: Array<ByteBufferL>) {
        for (b in arr) {
            require(b.remaining >= size) {
                "buffer remaining < blockSize: remaining=${b.remaining}, required=$size"
            }
        }
    }
}
