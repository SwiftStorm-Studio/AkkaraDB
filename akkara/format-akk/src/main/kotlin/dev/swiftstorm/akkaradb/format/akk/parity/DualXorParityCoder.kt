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
@file:Suppress("unused", "DuplicatedCode", "NOTHING_TO_INLINE")

package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.vh.LE
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityKind
import java.lang.Long.rotateLeft
import java.lang.Long.rotateRight
import java.nio.ByteBuffer

/**
 * Dual-XOR (P+Q) parity coder (m = 2).
 *
 * - No allocations on the hot path
 * - Absolute-index I/O via LE.* on raw ByteBuffer
 * - 64-bit wide XOR + tail-bytes
 * - Q uses bit-rotated values for better protection
 */
class DualXorParityCoder(
    override val blockSize: Int = BLOCK_SIZE
) : ParityCoder {

    override val parityCount: Int = 2
    override val kind: ParityKind = ParityKind.DUAL_XOR
    override val supportsErrorCorrection: Boolean = true // up to 1 data erasure

    private inline fun rotBits64ForLane(lane: Int): Int = (lane * 13 + 5) and 63
    private inline fun rotBits8ForLane(lane: Int): Int = (lane * 3 + 1) and 7

    override fun encodeInto(data: Array<ByteBufferL>, parityOut: Array<ByteBufferL>) {
        require(parityOut.size == 2) { "parityOut.size != 2" }
        val k = data.size
        require(k > 0) { "data is empty" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parityOut)

        val pBBL = parityOut[0];
        val qBBL = parityOut[1]
        val pBuf: ByteBuffer = pBBL.duplicate()
        val qBuf: ByteBuffer = qBBL.duplicate()
        val pBase = pBBL.position;
        val qBase = qBBL.position

        val dBufs = Array(k) { i -> data[i].duplicate() }
        val dBase = IntArray(k) { i -> data[i].position }

        val longBytes = (blockSize ushr 3) shl 3
        val tail = blockSize - longBytes

        // 64-bit wide
        var off = 0
        while (off < longBytes) {
            var accP = 0L
            var accQ = 0L
            when (k) {
                4 -> {
                    val v0 = LE.getLong(dBufs[0], dBase[0] + off)
                    val v1 = LE.getLong(dBufs[1], dBase[1] + off)
                    val v2 = LE.getLong(dBufs[2], dBase[2] + off)
                    val v3 = LE.getLong(dBufs[3], dBase[3] + off)
                    accP = v0 xor v1 xor v2 xor v3
                    accQ = (if (rotBits64ForLane(0) == 0) v0 else rotateLeft(v0, rotBits64ForLane(0))) xor
                            (if (rotBits64ForLane(1) == 0) v1 else rotateLeft(v1, rotBits64ForLane(1))) xor
                            (if (rotBits64ForLane(2) == 0) v2 else rotateLeft(v2, rotBits64ForLane(2))) xor
                            (if (rotBits64ForLane(3) == 0) v3 else rotateLeft(v3, rotBits64ForLane(3)))
                }

                else -> {
                    var i = 0
                    while (i < k) {
                        val v = LE.getLong(dBufs[i], dBase[i] + off)
                        accP = accP xor v
                        val r = rotBits64ForLane(i)
                        accQ = accQ xor if (r == 0) v else rotateLeft(v, r)
                        i++
                    }
                }
            }
            LE.putLong(pBuf, pBase + off, accP)
            LE.putLong(qBuf, qBase + off, accQ)
            off += 8
        }

        // tail bytes
        var t = 0
        while (t < tail) {
            val idx = off + t
            var bp = 0
            var bq = 0
            var i = 0
            while (i < k) {
                val v = LE.getU8(dBufs[i], dBase[i] + idx)
                bp = bp xor v
                val r8 = rotBits8ForLane(i)
                bq = bq xor rotl8(v, r8)
                i++
            }
            LE.putU8(pBuf, pBase + idx, bp)
            LE.putU8(qBuf, qBase + idx, bq)
            t++
        }
    }

    override fun verify(data: Array<ByteBufferL>, parity: Array<ByteBufferL>): Boolean {
        require(parity.size == 2) { "parity.size != 2" }
        val k = data.size
        require(k > 0) { "data is empty" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parity)

        val pBBL = parity[0];
        val qBBL = parity[1]
        val pBuf = pBBL.duplicate();
        val qBuf = qBBL.duplicate()
        val pBase = pBBL.position;
        val qBase = qBBL.position

        val dBufs = Array(k) { i -> data[i].duplicate() }
        val dBase = IntArray(k) { i -> data[i].position }

        val longBytes = (blockSize ushr 3) shl 3
        val tail = blockSize - longBytes

        var off = 0
        while (off < longBytes) {
            var accP = 0L
            var accQ = 0L
            var i = 0
            while (i < k) {
                val v = LE.getLong(dBufs[i], dBase[i] + off)
                accP = accP xor v
                val r = rotBits64ForLane(i)
                accQ = accQ xor if (r == 0) v else rotateLeft(v, r)
                i++
            }
            if (accP != LE.getLong(pBuf, pBase + off)) return false
            if (accQ != LE.getLong(qBuf, qBase + off)) return false
            off += 8
        }

        var t = 0
        while (t < tail) {
            val idx = off + t
            var bp = 0
            var bq = 0
            var i = 0
            while (i < k) {
                val v = LE.getU8(dBufs[i], dBase[i] + idx)
                bp = bp xor v
                bq = bq xor rotl8(v, rotBits8ForLane(i))
                i++
            }
            if (bp != LE.getU8(pBuf, pBase + idx)) return false
            if (bq != LE.getU8(qBuf, qBase + idx)) return false
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
        require(losses <= 2) { "too many losses" }
        require(parity.size == 2) { "parity.size != 2" }
        requireAllHave(blockSize, outData)
        requireAllHave(blockSize, outParity)
        data.forEach { if (it != null) require(it.remaining >= blockSize) }
        parity.forEach { if (it != null) require(it.remaining >= blockSize) }

        // lost parity only -> recompute
        if (lostDataIdx.isEmpty() && lostParityIdx.isNotEmpty()) {
            val k = data.size
            for (i in 0 until k) requireNotNull(data[i]) { "reconstruct parity requires all data present" }
            val needP = lostParityIdx.any { it == 0 }
            val needQ = lostParityIdx.any { it == 1 }
            val outP = if (needP) outParity[lostParityIdx.indexOf(0)] else null
            val outQ = if (needQ) outParity[lostParityIdx.indexOf(1)] else null
            val arr = Array(k) { requireNotNull(data[it]) }
            if (outP != null && outQ != null) encodeInto(arr, arrayOf(outP, outQ))
            else if (outP != null) encodeInto(arr, arrayOf(outP, dummyNull()))
            else if (outQ != null) encodeInto(arr, arrayOf(dummyNull(), outQ))
            return lostParityIdx.size
        }

        // one data loss -> use P or Q
        if (lostDataIdx.size == 1) {
            val li = lostDataIdx[0]
            val haveP = parity[0] != null
            val haveQ = parity[1] != null
            require(haveP || haveQ) { "need at least one parity to reconstruct" }

            val out = outData[0]
            val outBuf = out.duplicate();
            val outBase = out.position

            val k = data.size
            val dBufs = Array(k) { i -> data[i]?.duplicate() }
            val dBase = IntArray(k) { i -> data[i]?.position ?: 0 }

            val longBytes = (blockSize ushr 3) shl 3
            val tail = blockSize - longBytes

            if (haveP) {
                val pBBL = parity[0]!!
                val pBuf = pBBL.duplicate();
                val pBase = pBBL.position

                var off = 0
                while (off < longBytes) {
                    var acc = LE.getLong(pBuf, pBase + off)
                    var i = 0
                    while (i < k) {
                        if (i != li) {
                            val db = dBufs[i]!!
                            acc = acc xor LE.getLong(db, dBase[i] + off)
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
                    while (i < k) {
                        if (i != li) {
                            val db = dBufs[i]!!
                            b = b xor LE.getU8(db, dBase[i] + idx)
                        }
                        i++
                    }
                    LE.putU8(outBuf, outBase + idx, b)
                    t++
                }
            } else {
                val qBBL = parity[1]!!
                val qBuf = qBBL.duplicate();
                val qBase = qBBL.position
                val r64 = rotBits64ForLane(li);
                val r8 = rotBits8ForLane(li)

                var off = 0
                while (off < longBytes) {
                    var acc = LE.getLong(qBuf, qBase + off)
                    var i = 0
                    while (i < k) {
                        if (i != li) {
                            val db = dBufs[i]!!
                            val v = LE.getLong(db, dBase[i] + off)
                            val ri = rotBits64ForLane(i)
                            acc = acc xor if (ri == 0) v else rotateLeft(v, ri)
                        }
                        i++
                    }
                    val restored = if (r64 == 0) acc else rotateRight(acc, r64)
                    LE.putLong(outBuf, outBase + off, restored)
                    off += 8
                }
                var t = 0
                while (t < tail) {
                    val idx = off + t
                    var b = LE.getU8(qBuf, qBase + idx)
                    var i = 0
                    while (i < k) {
                        if (i != li) {
                            val v = LE.getU8(dBufs[i]!!, dBase[i] + idx)
                            b = b xor rotl8(v, rotBits8ForLane(i))
                        }
                        i++
                    }
                    LE.putU8(outBuf, outBase + idx, rotr8(b, r8))
                    t++
                }
            }

            // if parity also lost -> recompute from full data (using recovered out)
            if (lostParityIdx.isNotEmpty()) {
                val full = Array(k) { idx -> if (idx == li) out else requireNotNull(data[idx]) }
                if (lostParityIdx.size == 2) {
                    encodeInto(full, outParity)
                } else {
                    val which = lostParityIdx[0]
                    if (which == 0) encodeInto(full, arrayOf(outParity[0], dummyNull()))
                    else encodeInto(full, arrayOf(dummyNull(), outParity[0]))
                }
            }
            return 1 + lostParityIdx.size
        }

        return 0
    }

    override fun attachScratch(buf: ByteBufferL?) {}
    override fun scratchBytesHint(k: Int, m: Int): Int = 0

    // ---- helpers ----
    private fun requireAllHave(size: Int, arr: Array<ByteBufferL>) {
        for (b in arr) require(b.remaining >= size) {
            "buffer remaining < blockSize: remaining=${b.remaining}, required=$size"
        }
    }
    private fun rotl8(x: Int, r: Int): Int {
        val v = x and 0xFF;
        val n = r and 7
        return ((v shl n) or (v ushr (8 - n))) and 0xFF
    }
    private fun rotr8(x: Int, r: Int): Int {
        val v = x and 0xFF;
        val n = r and 7
        return ((v ushr n) or (v shl (8 - n))) and 0xFF
    }

    private fun dummyNull(): ByteBufferL = throw UnsupportedOperationException("not used")
}
