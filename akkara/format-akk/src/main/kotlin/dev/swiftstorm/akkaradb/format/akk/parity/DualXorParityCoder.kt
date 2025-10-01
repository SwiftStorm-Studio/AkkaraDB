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
import java.lang.Long.rotateLeft
import java.lang.Long.rotateRight

/**
 * Dual-XOR parity coder (m = 2).
 *
 * ## Overview
 * - Produces two independent parity blocks:
 *   - **P** = XOR of all data blocks
 *   - **Q** = XOR of bit-rotated data blocks (rotation amount depends on lane index)
 *
 * ## Properties
 * - Supports up to **one data loss** (can be reconstructed using either P or Q).
 * - Any lost parity block(s) can be recomputed from all data blocks.
 * - Losing both parity blocks simultaneously is still safe as long as all data blocks remain.
 * - **Two data losses cannot be reconstructed** — Reed–Solomon should be used instead.
 *
 * ## Implementation Notes
 * - Operates in 64-bit chunks for throughput; handles remaining tail bytes separately.
 * - All access is **absolute-index** based; buffer positions/limits remain unchanged.
 * - No heap allocations on the hot path.
 * - Bit rotations use lane-dependent constants to avoid collisions between P and Q.
 */
class DualXorParityCoder(
    override val blockSize: Int = BLOCK_SIZE
) : ParityCoder {

    override val parityCount: Int = 2
    override val kind: ParityKind = ParityKind.DUAL_XOR
    override val supportsErrorCorrection: Boolean = true // up to 1 data erasure

    private fun rotBits64ForLane(lane: Int): Int = (lane * 13 + 5) and 63
    private fun rotBits8ForLane(lane: Int): Int = (lane * 3 + 1) and 7

    override fun encodeInto(
        data: Array<ByteBufferL>,
        parityOut: Array<ByteBufferL>
    ) {
        require(parityOut.size == parityCount) { "parityOut.size != 2" }
        val k = data.size
        require(k > 0) { "data is empty" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parityOut)

        val p = parityOut[0]
        val q = parityOut[1]
        val pPos = p.position
        val qPos = q.position

        val longs = (blockSize ushr 3)
        val tail = blockSize - (longs shl 3)

        // 64-bit wide
        var off = 0
        while (off < (longs shl 3)) {
            var accP = 0L
            var accQ = 0L
            for (i in 0 until k) {
                val d = data[i]
                val dPos = d.position
                val v = d.at(dPos + off).i64
                accP = accP xor v
                val r = rotBits64ForLane(i)
                accQ = accQ xor if (r == 0) v else rotateLeft(v, r)
            }
            p.at(pPos + off).i64 = accP
            q.at(qPos + off).i64 = accQ
            off += 8
        }

        // tail bytes
        var t = 0
        while (t < tail) {
            val idx = off + t
            var bp = 0
            var bq = 0
            for (i in 0 until k) {
                val d = data[i]
                val v = d.at(d.position + idx).i8 // 0..255
                bp = bp xor v
                val r8 = rotBits8ForLane(i)
                bq = bq xor rotl8(v, r8)
            }
            p.at(pPos + idx).i8 = bp
            q.at(qPos + idx).i8 = bq
            t++
        }
    }

    override fun verify(
        data: Array<ByteBufferL>,
        parity: Array<ByteBufferL>
    ): Boolean {
        require(parity.size == parityCount) { "parity.size != 2" }
        val k = data.size
        require(k > 0) { "data is empty" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parity)

        val p = parity[0]
        val q = parity[1]
        val pPos = p.position
        val qPos = q.position

        val longs = (blockSize ushr 3)
        val tail = blockSize - (longs shl 3)

        var off = 0
        while (off < (longs shl 3)) {
            var accP = 0L
            var accQ = 0L
            for (i in 0 until k) {
                val d = data[i]
                val v = d.at(d.position + off).i64
                accP = accP xor v
                val r = rotBits64ForLane(i)
                accQ = accQ xor if (r == 0) v else rotateLeft(v, r)
            }
            if (accP != p.at(pPos + off).i64) return false
            if (accQ != q.at(qPos + off).i64) return false
            off += 8
        }

        var t = 0
        while (t < tail) {
            val idx = off + t
            var bp = 0
            var bq = 0
            for (i in 0 until k) {
                val v = data[i].at(data[i].position + idx).i8
                bp = bp xor v
                bq = bq xor rotl8(v, rotBits8ForLane(i))
            }
            if (bp != p.at(pPos + idx).i8) return false
            if (bq != q.at(qPos + idx).i8) return false
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
        val lossData = lostDataIdx.size
        val lossPar = lostParityIdx.size
        require(lossPar <= 2) { "at most 2 parity losses supported" }
        require(lossData <= 2) { "at most 2 data losses declared; DualXor can only fix up to 1" }
        if (lossData >= 2) {
            throw IllegalArgumentException("DualXor cannot reconstruct two data losses; use RS coder")
        }

        if (lossData == 0 && lossPar > 0) {
            val k = data.size
            for (i in 0 until k) requireNotNull(data[i]) { "parity reconstruct requires all data present" }

            val needP = lostParityIdx.any { it == 0 }
            val needQ = lostParityIdx.any { it == 1 }
            val tmpP = if (needP) outParity[lostParityIdx.indexOf(0)] else null
            val tmpQ = if (needQ) outParity[lostParityIdx.indexOf(1)] else null
            val arrData = Array(k) { requireNotNull(data[it]) }
            encodeInto(arrData, arrayOfNotNull(tmpP, tmpQ))
            return lossPar
        }

        if (lossData == 1) {
            val li = lostDataIdx[0]
            require(li in data.indices) { "lost data index out of range: $li" }

            val haveP = parity.getOrNull(0) != null
            val haveQ = parity.getOrNull(1) != null

            require(haveP || haveQ) { "need at least one parity (P or Q) to reconstruct one data block" }

            val out = outData[0]
            require(out.remaining >= blockSize) { "out buffer too small" }

            if (haveP) {
                val p = parity[0]!!
                val pPos = p.position

                val longs = (blockSize ushr 3)
                val tail = blockSize - (longs shl 3)
                val outPos = out.position

                var off = 0
                while (off < (longs shl 3)) {
                    var acc = p.at(pPos + off).i64
                    for (i in data.indices) {
                        if (i == li) continue
                        val buf = requireNotNull(data[i]) { "only one data loss supported" }
                        acc = acc xor buf.at(buf.position + off).i64
                    }
                    out.at(outPos + off).i64 = acc
                    off += 8
                }
                var t = 0
                while (t < tail) {
                    val idx = off + t
                    var b = parity[0]!!.at(pPos + idx).i8
                    for (i in data.indices) {
                        if (i == li) continue
                        val buf = requireNotNull(data[i])
                        b = b xor buf.at(buf.position + idx).i8
                    }
                    out.at(outPos + idx).i8 = b
                    t++
                }
            } else {
                val q = parity[1]!!
                val qPos = q.position

                val r64 = rotBits64ForLane(li)
                val r8 = rotBits8ForLane(li)

                val longs = (blockSize ushr 3)
                val tail = blockSize - (longs shl 3)
                val outPos = out.position

                var off = 0
                while (off < (longs shl 3)) {
                    var acc = q.at(qPos + off).i64
                    for (i in data.indices) {
                        if (i == li) continue
                        val buf = requireNotNull(data[i]) { "only one data loss supported" }
                        val v = buf.at(buf.position + off).i64
                        val ri = rotBits64ForLane(i)
                        acc = acc xor if (ri == 0) v else rotateLeft(v, ri)
                    }
                    // acc = ROTL_li(D_li) → D_li = ROR_li(acc)
                    val restored = if (r64 == 0) acc else rotateRight(acc, r64)
                    out.at(outPos + off).i64 = restored
                    off += 8
                }
                var t = 0
                while (t < tail) {
                    val idx = off + t
                    var b = parity[1]!!.at(qPos + idx).i8
                    for (i in data.indices) {
                        if (i == li) continue
                        val v = requireNotNull(data[i]).at(requireNotNull(data[i]).position + idx).i8
                        b = b xor rotl8(v, rotBits8ForLane(i))
                    }
                    // b = rotl8(D_li, r8) → D_li = rotr8(b, r8)
                    out.at(outPos + idx).i8 = rotr8(b, r8)
                    t++
                }
            }

            if (lossPar > 0) {
                val k = data.size
                val fullData = Array(k) { idx -> if (idx == li) out else requireNotNull(data[idx]) }
                if (lossPar == 2) {
                    encodeInto(fullData, outParity)
                } else {
                    val which = lostParityIdx[0]
                    if (which == 0) {
                        encodeInto(fullData, arrayOf(outParity[0], dummyNullParity()))
                    } else {
                        encodeInto(fullData, arrayOf(dummyNullParity(), outParity[0]))
                    }
                }
            }
            return 1 + lossPar
        }

        return 0
    }

    override fun attachScratch(buf: ByteBufferL?) {}

    override fun scratchBytesHint(k: Int, m: Int): Int = 0

    /* -------------------- helpers -------------------- */

    private fun requireAllHave(size: Int, arr: Array<ByteBufferL>) {
        for (b in arr) {
            require(b.remaining >= size) {
                "buffer remaining < blockSize: remaining=${b.remaining}, required=$size"
            }
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

    private fun arrayOfNotNull(vararg items: ByteBufferL?): Array<ByteBufferL> {
        return Array(items.count { it != null }) { idx ->
            items.filterNotNull()[idx]
        }
    }

    private fun dummyNullParity(): ByteBufferL {
        throw UnsupportedOperationException("dummyNullParity should not be called")
    }
}
