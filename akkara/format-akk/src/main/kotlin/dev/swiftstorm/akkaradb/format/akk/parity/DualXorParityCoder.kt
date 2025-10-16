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
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityKind
import java.lang.Long.rotateLeft
import java.lang.Long.rotateRight

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
    override val supportsErrorCorrection: Boolean = true

    private inline fun rotBits64ForLane(lane: Int): Int = (lane * 13 + 5) and 63
    private inline fun rotBits8ForLane(lane: Int): Int = (lane * 3 + 1) and 7

    // ---------------- encode ----------------
    override fun encodeInto(data: Array<ByteBufferL>, parityOut: Array<ByteBufferL>) {
        require(parityOut.size == 2) { "parityOut.size != 2" }
        val k = data.size
        require(k > 0) { "data is empty" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parityOut)

        val p = parityOut[0]
        val q = parityOut[1]
        val pBase = p.position
        val qBase = q.position
        val dBase = IntArray(k) { i -> data[i].position }

        val longBytes = (blockSize ushr 3) shl 3
        val tail = blockSize - longBytes

        var off = 0
        while (off < longBytes) {
            var accP = 0L
            var accQ = 0L
            var i = 0
            while (i < k) {
                val v = data[i].getI64At(dBase[i] + off)
                accP = accP xor v
                val r = rotBits64ForLane(i)
                accQ = accQ xor if (r == 0) v else rotateLeft(v, r)
                i++
            }
            p.putI64At(pBase + off, accP)
            q.putI64At(qBase + off, accQ)
            off += 8
        }

        var t = 0
        while (t < tail) {
            val idx = off + t
            var bp = 0
            var bq = 0
            var i = 0
            while (i < k) {
                val v = data[i].getU8At(dBase[i] + idx)
                bp = bp xor v
                bq = bq xor rotl8(v, rotBits8ForLane(i))
                i++
            }
            p.putU8At(pBase + idx, bp)
            q.putU8At(qBase + idx, bq)
            t++
        }
    }

    // ---------------- verify ----------------
    override fun verify(data: Array<ByteBufferL>, parity: Array<ByteBufferL>): Boolean {
        require(parity.size == 2) { "parity.size != 2" }
        val k = data.size
        require(k > 0) { "data is empty" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parity)

        val p = parity[0]
        val q = parity[1]
        val pBase = p.position
        val qBase = q.position
        val dBase = IntArray(k) { i -> data[i].position }

        val longBytes = (blockSize ushr 3) shl 3
        val tail = blockSize - longBytes

        var off = 0
        while (off < longBytes) {
            var accP = 0L
            var accQ = 0L
            var i = 0
            while (i < k) {
                val v = data[i].getI64At(dBase[i] + off)
                accP = accP xor v
                val r = rotBits64ForLane(i)
                accQ = accQ xor if (r == 0) v else rotateLeft(v, r)
                i++
            }
            if (accP != p.getI64At(pBase + off)) return false
            if (accQ != q.getI64At(qBase + off)) return false
            off += 8
        }

        var t = 0
        while (t < tail) {
            val idx = off + t
            var bp = 0
            var bq = 0
            var i = 0
            while (i < k) {
                val v = data[i].getU8At(dBase[i] + idx)
                bp = bp xor v
                bq = bq xor rotl8(v, rotBits8ForLane(i))
                i++
            }
            if (bp != p.getU8At(pBase + idx)) return false
            if (bq != q.getU8At(qBase + idx)) return false
            t++
        }
        return true
    }

    // ---------------- reconstruct ----------------
    override fun reconstruct(
        lostDataIdx: IntArray,
        lostParityIdx: IntArray,
        data: Array<ByteBufferL?>,
        parity: Array<ByteBufferL?>,
        outData: Array<ByteBufferL>,
        outParity: Array<ByteBufferL>
    ): Int {
        val losses = lostDataIdx.size + lostParityIdx.size
        require(losses <= 2) { "too many losses for DualXor (max 2 including parity)" }
        require(parity.size == 2) { "parity.size != 2" }
        requireAllHave(blockSize, outData)
        requireAllHave(blockSize, outParity)
        data.forEach { if (it != null) require(it.remaining >= blockSize) }
        parity.forEach { if (it != null) require(it.remaining >= blockSize) }

        val k = data.size

        if (lostDataIdx.isEmpty() && lostParityIdx.isNotEmpty()) {
            val present = Array(k) { idx -> requireNotNull(data[idx]) { "reconstruct parity requires all data present" } }
            val needP = lostParityIdx.any { it == 0 }
            val needQ = lostParityIdx.any { it == 1 }
            if (needP && needQ) {
                encodeInto(present, outParity) // 両方一気に
            } else if (needP) {
                computePInto(present, outParity[lostParityIdx.indexOf(0)])
            } else if (needQ) {
                computeQInto(present, outParity[lostParityIdx.indexOf(1)])
            }
            return lostParityIdx.size
        }

        if (lostDataIdx.size == 1) {
            val li = lostDataIdx[0]
            val haveP = parity[0] != null
            val haveQ = parity[1] != null
            require(haveP || haveQ) { "need at least one parity to reconstruct lost data" }

            val out = outData[0]
            val outBase = out.position
            val dBase = IntArray(k) { i -> data[i]?.position ?: 0 }

            val longBytes = (blockSize ushr 3) shl 3
            val tail = blockSize - longBytes

            if (haveP) {
                val p = parity[0]!!
                val pBase = p.position
                var off = 0
                while (off < longBytes) {
                    var acc = p.getI64At(pBase + off)
                    var i = 0
                    while (i < k) {
                        if (i != li) {
                            val db = data[i]!!
                            acc = acc xor db.getI64At(dBase[i] + off)
                        }
                        i++
                    }
                    out.putI64At(outBase + off, acc); off += 8
                }
                var t = 0
                while (t < tail) {
                    val idx = off + t
                    var b = p.getU8At(pBase + idx)
                    var i = 0
                    while (i < k) {
                        if (i != li) {
                            val db = data[i]!!
                            b = b xor db.getU8At(dBase[i] + idx)
                        }
                        i++
                    }
                    out.putU8At(outBase + idx, b); t++
                }
            } else {
                val q = parity[1]!!
                val qBase = q.position
                val rLi64 = rotBits64ForLane(li)
                val rLi8 = rotBits8ForLane(li)

                var off = 0
                while (off < longBytes) {
                    var acc = q.getI64At(qBase + off)
                    var i = 0
                    while (i < k) {
                        if (i != li) {
                            val db = data[i]!!
                            val v = db.getI64At(dBase[i] + off)
                            val ri = rotBits64ForLane(i)
                            acc = acc xor if (ri == 0) v else rotateLeft(v, ri)
                        }
                        i++
                    }
                    val restored = if (rLi64 == 0) acc else rotateRight(acc, rLi64)
                    out.putI64At(outBase + off, restored); off += 8
                }
                var t = 0
                while (t < tail) {
                    val idx = off + t
                    var b = q.getU8At(qBase + idx)
                    var i = 0
                    while (i < k) {
                        if (i != li) {
                            val v = data[i]!!.getU8At(dBase[i] + idx)
                            b = b xor rotl8(v, rotBits8ForLane(i))
                        }
                        i++
                    }
                    out.putU8At(outBase + idx, rotr8(b, rLi8)); t++
                }
            }

            if (lostParityIdx.isNotEmpty()) {
                val full = Array(k) { idx -> if (idx == li) out else requireNotNull(data[idx]) }
                if (lostParityIdx.size == 2) {
                    encodeInto(full, outParity)
                } else {
                    val which = lostParityIdx[0]
                    if (which == 0) computePInto(full, outParity[0]) else computeQInto(full, outParity[0])
                }
            }
            return 1 + lostParityIdx.size
        }

        return 0
    }

    override fun attachScratch(buf: ByteBufferL?) {}
    override fun scratchBytesHint(k: Int, m: Int): Int = 0

    // ---------------- helpers ----------------
    private fun requireAllHave(size: Int, arr: Array<ByteBufferL>) {
        for (b in arr) require(b.remaining >= size) {
            "buffer remaining < blockSize: remaining=${b.remaining}, required=$size"
        }
    }

    private fun rotl8(x: Int, r: Int): Int {
        val v = x and 0xFF
        val n = r and 7
        return ((v shl n) or (v ushr (8 - n))) and 0xFF
    }
    private fun rotr8(x: Int, r: Int): Int {
        val v = x and 0xFF
        val n = r and 7
        return ((v ushr n) or (v shl (8 - n))) and 0xFF
    }

    private fun computePInto(data: Array<ByteBufferL>, outP: ByteBufferL) {
        val k = data.size
        val pBase = outP.position
        val dBase = IntArray(k) { i -> data[i].position }
        val longBytes = (blockSize ushr 3) shl 3
        val tail = blockSize - longBytes

        var off = 0
        while (off < longBytes) {
            var acc = 0L
            var i = 0
            while (i < k) {
                acc = acc xor data[i].getI64At(dBase[i] + off); i++
            }
            outP.putI64At(pBase + off, acc); off += 8
        }
        var t = 0
        while (t < tail) {
            val idx = off + t
            var b = 0
            var i = 0
            while (i < k) {
                b = b xor data[i].getU8At(dBase[i] + idx); i++
            }
            outP.putU8At(pBase + idx, b); t++
        }
    }

    private fun computeQInto(data: Array<ByteBufferL>, outQ: ByteBufferL) {
        val k = data.size
        val qBase = outQ.position
        val dBase = IntArray(k) { i -> data[i].position }
        val longBytes = (blockSize ushr 3) shl 3
        val tail = blockSize - longBytes

        var off = 0
        while (off < longBytes) {
            var acc = 0L
            var i = 0
            while (i < k) {
                val v = data[i].getI64At(dBase[i] + off)
                val r = rotBits64ForLane(i)
                acc = acc xor if (r == 0) v else rotateLeft(v, r)
                i++
            }
            outQ.putI64At(qBase + off, acc); off += 8
        }
        var t = 0
        while (t < tail) {
            val idx = off + t
            var b = 0
            var i = 0
            while (i < k) {
                val v = data[i].getU8At(dBase[i] + idx)
                b = b xor rotl8(v, rotBits8ForLane(i))
                i++
            }
            outQ.putU8At(qBase + idx, b); t++
        }
    }
}
