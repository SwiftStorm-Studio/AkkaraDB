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
import dev.swiftstorm.akkaradb.common.vh.LE
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityKind
import java.nio.ByteBuffer

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

        // Parity buffer + absolute base
        val pBBL = parityOut[0]
        val pBuf: ByteBuffer = pBBL.duplicate()
        val pBase = pBBL.position

        // Data buffers + absolute bases
        val dBufs = Array(data.size) { i -> data[i].duplicate() }
        val dBase = IntArray(data.size) { i -> data[i].position }

        val longBytes = (blockSize ushr 3) shl 3
        val tail = blockSize - longBytes

        // 64-bit XOR
        var off = 0
        while (off < longBytes) {
            var acc = 0L
            // 軽アンローリング（k==4が多い前提ならここを増やす）
            when (dBufs.size) {
                4 -> {
                    acc = acc xor LE.getLong(dBufs[0], dBase[0] + off)
                    acc = acc xor LE.getLong(dBufs[1], dBase[1] + off)
                    acc = acc xor LE.getLong(dBufs[2], dBase[2] + off)
                    acc = acc xor LE.getLong(dBufs[3], dBase[3] + off)
                }

                else -> {
                    var i = 0
                    while (i < dBufs.size) {
                        acc = acc xor LE.getLong(dBufs[i], dBase[i] + off)
                        i++
                    }
                }
            }
            LE.putLong(pBuf, pBase + off, acc)
            off += 8
        }

        // tail bytes
        var t = 0
        while (t < tail) {
            val idx = off + t
            var b = 0
            when (dBufs.size) {
                4 -> {
                    b = b xor LE.getU8(dBufs[0], dBase[0] + idx)
                    b = b xor LE.getU8(dBufs[1], dBase[1] + idx)
                    b = b xor LE.getU8(dBufs[2], dBase[2] + idx)
                    b = b xor LE.getU8(dBufs[3], dBase[3] + idx)
                }

                else -> {
                    var i = 0
                    while (i < dBufs.size) {
                        b = b xor LE.getU8(dBufs[i], dBase[i] + idx)
                        i++
                    }
                }
            }
            LE.putU8(pBuf, pBase + idx, b)
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

        val pBBL = parity[0]
        val pBuf = pBBL.duplicate()
        val pBase = pBBL.position

        val dBufs = Array(data.size) { i -> data[i].duplicate() }
        val dBase = IntArray(data.size) { i -> data[i].position }

        val longBytes = (blockSize ushr 3) shl 3
        val tail = blockSize - longBytes

        var off = 0
        while (off < longBytes) {
            var acc = 0L
            when (dBufs.size) {
                4 -> {
                    acc = acc xor LE.getLong(dBufs[0], dBase[0] + off)
                    acc = acc xor LE.getLong(dBufs[1], dBase[1] + off)
                    acc = acc xor LE.getLong(dBufs[2], dBase[2] + off)
                    acc = acc xor LE.getLong(dBufs[3], dBase[3] + off)
                }

                else -> {
                    var i = 0
                    while (i < dBufs.size) {
                        acc = acc xor LE.getLong(dBufs[i], dBase[i] + off)
                        i++
                    }
                }
            }
            if (acc != LE.getLong(pBuf, pBase + off)) return false
            off += 8
        }

        var t = 0
        while (t < tail) {
            val idx = off + t
            var b = 0
            when (dBufs.size) {
                4 -> {
                    b = b xor LE.getU8(dBufs[0], dBase[0] + idx)
                    b = b xor LE.getU8(dBufs[1], dBase[1] + idx)
                    b = b xor LE.getU8(dBufs[2], dBase[2] + idx)
                    b = b xor LE.getU8(dBufs[3], dBase[3] + idx)
                }

                else -> {
                    var i = 0
                    while (i < dBufs.size) {
                        b = b xor LE.getU8(dBufs[i], dBase[i] + idx)
                        i++
                    }
                }
            }
            if (b != LE.getU8(pBuf, pBase + idx)) return false
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
            val pBBL = requireNotNull(parity[0]) { "parity block required" }
            val pBuf = pBBL.duplicate()
            val pBase = pBBL.position

            val out = outData[0]
            val outBuf = out.duplicate()
            val outBase = out.position

            val dBufs = Array(data.size) { i -> data[i]?.duplicate() }
            val dBase = IntArray(data.size) { i -> data[i]?.position ?: 0 }

            val longBytes = (blockSize ushr 3) shl 3
            val tail = blockSize - longBytes

            var off = 0
            while (off < longBytes) {
                var acc = LE.getLong(pBuf, pBase + off)
                var i = 0
                while (i < dBufs.size) {
                    if (i != li) {
                        val db = dBufs[i]
                        if (db != null) acc = acc xor LE.getLong(db, dBase[i] + off)
                    }
                    i++
                }
                LE.putLong(outBuf, outBase + off, acc)
                off += 8
            }

            var t = 0
            while (t < tail) {
                val idx = off + t
                var b = LE.getU8(pBuf, pBase + idx)
                var i = 0
                while (i < dBufs.size) {
                    if (i != li) {
                        val db = dBufs[i]
                        if (db != null) b = b xor LE.getU8(db, dBase[i] + idx)
                    }
                    i++
                }
                LE.putU8(outBuf, outBase + idx, b)
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
