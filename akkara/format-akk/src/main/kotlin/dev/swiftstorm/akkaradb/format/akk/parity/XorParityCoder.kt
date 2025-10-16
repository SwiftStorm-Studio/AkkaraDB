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
@file:Suppress("NOTHING_TO_INLINE", "DuplicatedCode", "KotlinConstantConditions")

package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityKind

/**
 * XOR parity coder (m = 1).
 *
 * - No allocations on the hot path
 * - Absolute-index I/O via LE.* on raw ByteBuffer
 * - 64-bit wide XOR + tail-bytes
 */
class XorParityCoder(
    override val blockSize: Int = BLOCK_SIZE
) : ParityCoder {

    override val parityCount: Int = 1
    override val kind: ParityKind = ParityKind.XOR
    override val supportsErrorCorrection: Boolean = true // up to 1 erasure

    // ---------------- encode ----------------
    override fun encodeInto(
        data: Array<ByteBufferL>,
        parityOut: Array<ByteBufferL>
    ) {
        require(parityOut.size == parityCount) { "parityOut.size != 1" }
        require(data.isNotEmpty()) { "data is empty" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parityOut)

        val p = parityOut[0]
        val pBase = p.position

        val dBase = IntArray(data.size) { i -> data[i].position }

        val longBytes = (blockSize ushr 3) shl 3
        val tail = blockSize - longBytes

        var off = 0
        while (off < longBytes) {
            var acc = 0L
            var i = 0
            while (i < data.size) {
                acc = acc xor data[i].getI64At(dBase[i] + off)
                i++
            }
            p.putI64At(pBase + off, acc)
            off += 8
        }

        var t = 0
        while (t < tail) {
            val idx = off + t
            var b = 0
            var i = 0
            while (i < data.size) {
                b = b xor data[i].getU8At(dBase[i] + idx)
                i++
            }
            p.putU8At(pBase + idx, b)
            t++
        }
    }

    // ---------------- verify ----------------
    override fun verify(
        data: Array<ByteBufferL>,
        parity: Array<ByteBufferL>
    ): Boolean {
        require(parity.size == parityCount) { "parity.size != 1" }
        require(data.isNotEmpty()) { "data is empty" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parity)

        val p = parity[0]
        val pBase = p.position

        val dBase = IntArray(data.size) { i -> data[i].position }

        val longBytes = (blockSize ushr 3) shl 3
        val tail = blockSize - longBytes

        var off = 0
        while (off < longBytes) {
            var acc = 0L
            var i = 0
            while (i < data.size) {
                acc = acc xor data[i].getI64At(dBase[i] + off)
                i++
            }
            if (acc != p.getI64At(pBase + off)) return false
            off += 8
        }

        var t = 0
        while (t < tail) {
            val idx = off + t
            var b = 0
            var i = 0
            while (i < data.size) {
                b = b xor data[i].getU8At(dBase[i] + idx)
                i++
            }
            if (b != p.getU8At(pBase + idx)) return false
            t++
        }
        return true
    }

    // ---------------- reconstruct (up to 1 erasure) ----------------
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

        // lost parity -> recompute from present data
        if (lostParityIdx.isNotEmpty()) {
            require(lostParityIdx.size == 1 && lostParityIdx[0] == 0) { "invalid lostParityIdx for XOR" }
            val present = ArrayList<ByteBufferL>(data.size)
            for (d in data) present += requireNotNull(d) { "reconstruct parity requires all data present" }
            encodeInto(present.toTypedArray(), arrayOf(outParity[0]))
            return 1
        }

        // lost data -> parity ^ xor(all other data)
        if (lostDataIdx.isNotEmpty()) {
            val li = lostDataIdx[0]

            val p = requireNotNull(parity[0]) { "parity block required" }
            val pBase = p.position

            val out = outData[0]
            val outBase = out.position

            val dBase = IntArray(data.size) { i -> data[i]?.position ?: 0 }

            val longBytes = (blockSize ushr 3) shl 3
            val tail = blockSize - longBytes

            var off = 0
            while (off < longBytes) {
                var acc = p.getI64At(pBase + off)
                var i = 0
                while (i < data.size) {
                    if (i != li) {
                        val db = data[i]
                        if (db != null) acc = acc xor db.getI64At(dBase[i] + off)
                    }
                    i++
                }
                out.putI64At(outBase + off, acc)
                off += 8
            }

            var t = 0
            while (t < tail) {
                val idx = off + t
                var b = p.getU8At(pBase + idx)
                var i = 0
                while (i < data.size) {
                    if (i != li) {
                        val db = data[i]
                        if (db != null) b = b xor db.getU8At(dBase[i] + idx)
                    }
                    i++
                }
                out.putU8At(outBase + idx, b)
                t++
            }
            return 1
        }
        return 0
    }

    override fun attachScratch(buf: ByteBufferL?) {}
    override fun scratchBytesHint(k: Int, m: Int): Int = 0

    // ------------- helpers -------------
    private fun requireAllHave(size: Int, arr: Array<ByteBufferL>) {
        for (b in arr) {
            require(b.remaining >= size) {
                "buffer remaining < blockSize: remaining=${b.remaining}, required=$size"
            }
        }
    }
}