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
@file:Suppress("unused", "NOTHING_TO_INLINE", "DuplicatedCode", "LocalVariableName")

package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.vh.LE
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityKind
import java.nio.ByteBuffer

/**
 * Reed-Solomon parity coder over GF(256) (n ≤ 255).
 *
 * - No allocations on the hot path (except first-time coeff table build)
 * - Absolute-index I/O via LE.* on raw ByteBuffer
 * - Uses lookup table for multiplication
 * - Can reconstruct up to m erasures (data+parity combined)
 */
class RSParityCoder(
    override val parityCount: Int,
    override val blockSize: Int = BLOCK_SIZE
) : ParityCoder {

    init {
        require(parityCount >= 1) { "parityCount must be ≥ 1" }
    }

    override val kind: ParityKind = ParityKind.RS
    override val supportsErrorCorrection: Boolean get() = true

    /* ---------------- GF(256) tables (poly = 0x11D) ---------------- */
    private val exp = IntArray(512)
    private val log = IntArray(256)
    private val mulLUT: ByteArray = ByteArray(256 * 256) // [a<<8 | b] => a*b

    init {
        var x = 1
        for (i in 0 until 255) {
            exp[i] = x
            log[x] = i
            x = x shl 1
            if ((x and 0x100) != 0) x = x xor 0x11D
        }
        for (i in 255 until 512) exp[i] = exp[i - 255]
        log[0] = 0 // guard

        var idx = 0
        for (a in 0..255) for (b in 0..255)
            mulLUT[idx++] = gfMulExpLog(a, b).toByte()
    }

    private inline fun gfAdd(a: Int, b: Int): Int = a xor b
    private inline fun gfMulExpLog(a: Int, b: Int): Int {
        if (a == 0 || b == 0) return 0
        return exp[log[a] + log[b]]
    }

    /* ---------------- Coefficient cache: a(j,i) = α^{(j+1)·i} ---------------- */
    @Volatile
    private var cachedK: Int = -1
    /** coeff[j][i] */
    @Volatile
    private var coeff: Array<IntArray>? = null

    private fun ensureCoeff(k: Int) {
        val c = coeff
        if (c != null && cachedK == k) return
        val newCoeff = Array(parityCount) { j -> IntArray(k) { i -> coeffA(j, i) } }
        coeff = newCoeff
        cachedK = k
    }
    private inline fun coeffA(row: Int, i: Int): Int {
        return if (i == 0) 1 else exp[((row + 1) * i) % 255]
    }

    /* ---------------- ParityCoder API ---------------- */
    override fun encodeInto(data: Array<ByteBufferL>, parityOut: Array<ByteBufferL>) {
        val k = data.size;
        val m = parityOut.size
        require(k > 0) { "data is empty" }
        require(m == parityCount) { "parityOut.size=$m but parityCount=$parityCount" }
        require(k + m <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$m" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parityOut)

        ensureCoeff(k)
        val c = coeff!!

        // Zero parity blocks
        zeroBlocks(parityOut)

        // dst_j ^= a(j,i) * src_i
        for (i in 0 until k) {
            val src = data[i]
            val sBuf = src.duplicate()
            val sBase = src.position
            val aCol = IntArray(m) { j -> c[j][i] }
            for (j in 0 until m) if (aCol[j] != 0) {
                saxpy(parityOut[j], sBuf, sBase, aCol[j])
            }
        }
    }

    override fun verify(data: Array<ByteBufferL>, parity: Array<ByteBufferL>): Boolean {
        val k = data.size;
        val m = parity.size
        require(k > 0) { "data is empty" }
        require(m == parityCount) { "parity.size=$m but parityCount=$parityCount" }
        require(k + m <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$m" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parity)

        ensureCoeff(k)
        val c = coeff!!

        val N = blockSize
        val dBufs = Array(k) { i -> data[i].duplicate() }
        val dBase = IntArray(k) { i -> data[i].position }
        val pBufs = Array(m) { j -> parity[j].duplicate() }
        val pBase = IntArray(m) { j -> parity[j].position }

        var pos = 0
        while (pos < N) {
            for (j in 0 until m) {
                var acc = 0
                val row = c[j]
                var i = 0
                while (i < k) {
                    val s = LE.getU8(dBufs[i], dBase[i] + pos)
                    if (s != 0) acc = acc xor (mulLUT[(row[i] shl 8) or s].toInt() and 0xFF)
                    i++
                }
                if (acc != LE.getU8(pBufs[j], pBase[j] + pos)) return false
            }
            pos++
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
        val k = data.size;
        val m = parityCount
        require(parity.size == m) { "parity.size=${parity.size} but parityCount=$m" }
        require(k + m <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$m" }

        for (d in data) if (d != null) require(d.remaining >= blockSize)
        for (p in parity) if (p != null) require(p.remaining >= blockSize)
        for (od in outData) require(od.remaining >= blockSize)
        for (op in outParity) require(op.remaining >= blockSize)

        ensureCoeff(k)
        val c = coeff!!

        val eData = lostDataIdx.size
        val eParity = lostParityIdx.size
        var repaired = 0

        // only parity lost → recompute requested rows
        if (eData == 0 && eParity > 0) {
            val arrData = Array(k) { requireNotNull(data[it]) { "need all data to recompute parity" } }
            for (idx in lostParityIdx.indices) {
                val j = lostParityIdx[idx]
                encodeRowInto(outParity[idx], arrData, c, j)
                repaired++
            }
            return repaired
        }

        // data lost (<= m)
        if (eData > 0) {
            require(eData <= m) { "too many data erasures: $eData > m=$m" }

            val availRows = ArrayList<Int>(m)
            for (j in 0 until m) if (parity[j] != null) availRows += j
            require(availRows.size >= eData) { "need at least $eData parity rows; available=${availRows.size}" }
            val J = IntArray(eData) { availRows[it] }

            // M[r][t] = a(J[r], L[t])
            val M = Array(eData) { IntArray(eData) }
            for (r in 0 until eData) {
                val row = c[J[r]]
                for (t in 0 until eData) {
                    M[r][t] = row[lostDataIdx[t]]
                }
            }
            val Minv = invertMatrix(M)

            val N = blockSize
            val pBufs = Array(m) { j -> parity[j]?.duplicate() }
            val pBase = IntArray(m) { j -> parity[j]?.position ?: 0 }
            val outMap = HashMap<Int, ByteBufferL>(eData)
            for (t in 0 until eData) outMap[lostDataIdx[t]] = outData[t]
            val outBase = IntArray(eData) { outData[it].position }

            val dBufs = Array(k) { i -> data[i]?.duplicate() }
            val dBase = IntArray(k) { i -> data[i]?.position ?: 0 }

            var pos = 0
            while (pos < N) {
                // S_r = P_{J[r]} ⊕ Σ_{i∉L} a(J[r],i)·D_i
                val S = IntArray(eData)
                for (r in 0 until eData) {
                    val j = J[r]
                    var accumKnown = 0
                    val crow = c[j]
                    // known data
                    var i = 0
                    while (i < k) {
                        if (outMap.containsKey(i)) {
                            i++; continue
                        } // i in L
                        val db = dBufs[i]
                        if (db != null) {
                            val s = LE.getU8(db, dBase[i] + pos)
                            if (s != 0) accumKnown = accumKnown xor (mulLUT[(crow[i] shl 8) or s].toInt() and 0xFF)
                        }
                        i++
                    }
                    val pj = LE.getU8(pBufs[j]!!, pBase[j] + pos)
                    S[r] = pj xor accumKnown
                }

                // D_L = Minv · S
                for (t in 0 until eData) {
                    var v = 0
                    val row = Minv[t]
                    var cIdx = 0
                    while (cIdx < eData) {
                        val a = row[cIdx]
                        if (a != 0) v = v xor (mulLUT[(a shl 8) or S[cIdx]].toInt() and 0xFF)
                        cIdx++
                    }
                    val dst = outMap[lostDataIdx[t]]!!
                    val ob = outBase[t] + pos
                    LE.putU8(dst.duplicate(), ob, v)
                }
                pos++
            }
            repaired += eData

            // parity も失われていれば復元後に再計算
            if (eParity > 0) {
                val full = Array(k) { i -> outMap[i] ?: requireNotNull(data[i]) }
                for (idx in lostParityIdx.indices) {
                    val j = lostParityIdx[idx]
                    encodeRowInto(outParity[idx], full, c, j)
                    repaired++
                }
            }
            return repaired
        }

        return 0
    }

    override fun attachScratch(buf: ByteBufferL?) {}
    override fun scratchBytesHint(k: Int, m: Int): Int = 0

    /* ---------------- Inner kernels ---------------- */

    /** dst ^= a * src （絶対オフセット、positions不変、LE直叩き） */
    private inline fun saxpy(dst: ByteBufferL, srcBuf: ByteBuffer, srcBase: Int, a: Int) {
        val N = blockSize
        val lutBase = a shl 8
        val dBuf = dst.duplicate()
        val dBase = dst.position
        var p = 0
        while (p < N) {
            val s = LE.getU8(srcBuf, srcBase + p)
            if (s != 0) {
                val m = mulLUT[lutBase or s].toInt() and 0xFF
                val d = LE.getU8(dBuf, dBase + p)
                LE.putU8(dBuf, dBase + p, d xor m)
            }
            p++
        }
    }

    private fun encodeRowInto(dst: ByteBufferL, data: Array<ByteBufferL>, c: Array<IntArray>, j: Int) {
        require(j in c.indices) { "row j=$j out of range" }
        val N = blockSize
        val base = dst.position
        val dBuf = dst.duplicate()
        // zero dst
        var p = 0
        while (p < N) {
            LE.putU8(dBuf, base + p, 0)
            p++
        }
        // accumulate
        for (i in data.indices) {
            val di = data[i]
            val a = c[j][i]
            if (a == 0) continue
            saxpy(dst, di.duplicate(), di.position, a)
        }
    }

    // Gauss–Jordan (小さい正方行列, GF(256))
    private fun invertMatrix(a0: Array<IntArray>): Array<IntArray> {
        val n = a0.size
        val a = Array(n) { r -> a0[r].clone() }
        val inv = Array(n) { r -> IntArray(n).apply { this[r] = 1 } }

        var row = 0
        for (col in 0 until n) {
            var pivot = row
            while (pivot < n && a[pivot][col] == 0) pivot++
            if (pivot == n) continue
            if (pivot != row) {
                val tmp = a[pivot]; a[pivot] = a[row]; a[row] = tmp
                val tmp2 = inv[pivot]; inv[pivot] = inv[row]; inv[row] = tmp2
            }
            val invPivot = invElt(a[row][col])
            for (c in 0 until n) {
                a[row][c] = mul(a[row][c], invPivot)
                inv[row][c] = mul(inv[row][c], invPivot)
            }
            for (r in 0 until n) if (r != row) {
                val f = a[r][col]
                if (f != 0) {
                    for (c in 0 until n) {
                        a[r][c] = add(a[r][c], mul(f, a[row][c]))
                        inv[r][c] = add(inv[r][c], mul(f, inv[row][c]))
                    }
                }
            }
            row++
        }
        return inv
    }

    // zero out all bytes in parity blocks
    private fun zeroBlocks(parityOut: Array<ByteBufferL>) {
        for (b in parityOut) b.fillZero(blockSize)
    }

    // small inline wrappers to mulLUT/exp/log for matrix path
    private inline fun add(a: Int, b: Int) = a xor b
    private inline fun mul(a: Int, b: Int): Int = if (a == 0 || b == 0) 0 else exp[log[a] + log[b]]
    private inline fun invElt(a: Int): Int {
        require(a != 0); return exp[255 - log[a]]
    }

    /* ---------------- guards ---------------- */
    private fun requireAllHave(size: Int, arr: Array<ByteBufferL>) {
        for (b in arr) require(b.remaining >= size) {
            "buffer remaining < blockSize: remaining=${b.remaining}, required=$size"
        }
    }
}
