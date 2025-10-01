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

@file:Suppress("unused", "NOTHING_TO_INLINE", "DuplicatedCode", "LocalVariableName", "KDocUnresolvedReference")

package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityKind

/**
 * Reed–Solomon parity coder (GF(2^8), Vandermonde generator).
 *
 * ## Overview
 * - Supports any parity count `m ≥ 1` and arbitrary data lane count `k ≥ 1`, with `n = k + m ≤ 255`.
 * - In-place encoding: caller provides parity buffers; this class **never allocates** on the hot path.
 * - Works entirely over `ByteBufferL` with **absolute-index access** (via `.at(offset).i8`); buffer positions/limits remain unchanged.
 *
 * ## Math
 * Parity row `j` (0-based) uses coefficients `a(j,i) = α^{(j+1)·i}` where `α` is a primitive element of GF(256) (poly 0x11D).
 * Encoding for byte position `p`: `P_j[p] = ⊕_i ( a(j,i) · D_i[p] )`.
 *
 * ## Performance
 * - Precomputed GF tables (`exp/log`, 64K mul LUT).
 * - Per-`k` coefficient cache `coeff[j][i]`.
 * - Inner loop uses a SAXPY-like pattern (`dst ^= mul(coeff, srcByte)`), no branching, no heap writes except outputs.
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
        for (a in 0..255) {
            for (b in 0..255) {
                mulLUT[idx++] = gfMulExpLog(a, b).toByte()
            }
        }
    }

    private inline fun gfAdd(a: Int, b: Int): Int = a xor b

    private inline fun gfMul(a: Int, b: Int): Int {
        return mulLUT[(a shl 8) or b].toInt() and 0xFF
    }

    private inline fun gfMulExpLog(a: Int, b: Int): Int {
        if (a == 0 || b == 0) return 0
        return exp[log[a] + log[b]]
    }

    private inline fun gfInv(a: Int): Int {
        require(a != 0) { "gfInv(0)" }
        return exp[255 - log[a]]
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
        val newCoeff = Array(parityCount) { j ->
            IntArray(k) { i -> coeffA(j, i) }
        }
        coeff = newCoeff
        cachedK = k
    }

    private inline fun coeffA(row: Int, i: Int): Int {
        return if (i == 0) 1 else exp[((row + 1) * i) % 255]
    }

    /* ---------------- ParityCoder API ---------------- */

    override fun encodeInto(
        data: Array<ByteBufferL>,
        parityOut: Array<ByteBufferL>
    ) {
        val k = data.size
        val m = parityOut.size
        require(k > 0) { "data is empty" }
        require(m == parityCount) { "parityOut.size=$m but parityCount=$parityCount" }
        require(k + m <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$m" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parityOut)

        ensureCoeff(k)
        val c = coeff!!

        // zero parityOut
        zeroBlocks(parityOut)

        // For each data lane i: parity[j] ^= a(j,i) * data[i]
        for (i in 0 until k) {
            val src = data[i]
            val sBase = src.position
            for (j in 0 until m) {
                saxpy(parityOut[j], src, c[j][i], sBase)
            }
        }
    }

    override fun verify(
        data: Array<ByteBufferL>,
        parity: Array<ByteBufferL>
    ): Boolean {
        val k = data.size
        val m = parity.size
        require(k > 0) { "data is empty" }
        require(m == parityCount) { "parity.size=$m but parityCount=$parityCount" }
        require(k + m <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$m" }
        requireAllHave(blockSize, data)
        requireAllHave(blockSize, parity)

        ensureCoeff(k)
        val c = coeff!!

        val N = blockSize
        val pBase = IntArray(m) { parity[it].position }

        var pos = 0
        while (pos < N) {
            // compute expected parity byte for each row j
            for (j in 0 until m) {
                var acc = 0
                val row = c[j]
                for (i in 0 until k) {
                    val d = data[i]
                    val s = d.at(d.position + pos).i8 // 0..255
                    if (s != 0) {
                        val a = row[i]
                        acc = acc xor (mulLUT[(a shl 8) or s].toInt() and 0xFF)
                    }
                }
                val pj = parity[j].at(pBase[j] + pos).i8
                if (acc != pj) return false
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
        val k = data.size
        val m = parityCount
        require(parity.size == m) { "parity.size=${parity.size} but parityCount=$m" }
        require(k + m <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$m" }

        // Basic size checks
        for (d in data) if (d != null) require(d.remaining >= blockSize) { "data block too small" }
        for (p in parity) if (p != null) require(p.remaining >= blockSize) { "parity block too small" }
        for (od in outData) require(od.remaining >= blockSize) { "outData block too small" }
        for (op in outParity) require(op.remaining >= blockSize) { "outParity block too small" }

        ensureCoeff(k)
        val c = coeff!!

        val eData = lostDataIdx.size
        val eParity = lostParityIdx.size
        var repaired = 0

        // Case A: only parity lost -> recompute requested parity rows from all data (must have all data present)
        if (eData == 0 && eParity > 0) {
            for (idx in lostParityIdx.indices) {
                val j = lostParityIdx[idx]
                val dst = outParity[idx]
                encodeRowInto(dst, data, c, j)
                repaired++
            }
            return repaired
        }

        // Case B: one or more data lost (≤ m)
        if (eData > 0) {
            require(eData <= m) { "too many data erasures: $eData > m=$m" }

            // choose available parity rows
            val availRows = ArrayList<Int>(m)
            for (j in 0 until m) if (parity[j] != null) availRows += j
            require(availRows.size >= eData) {
                "need at least e=$eData parity rows; available=${availRows.size}"
            }
            val J = IntArray(eData) { availRows[it] }

            // Build M[r][c] = a(J[r], L[c]) where L = lostDataIdx
            val M = Array(eData) { IntArray(eData) }
            for (r in 0 until eData) {
                val row = J[r]
                val crow = c[row]
                for (t in 0 until eData) {
                    val i = lostDataIdx[t]
                    M[r][t] = crow[i]
                }
            }
            val Minv = invertMatrix(M)

            // Recover all lost data bytes: D_L = Minv · S
            val N = blockSize
            val pBase = IntArray(m) { parity[it]?.position ?: 0 }
            val outMap = HashMap<Int, ByteBufferL>(eData)
            for (t in 0 until eData) outMap[lostDataIdx[t]] = outData[t]
            val outBase = IntArray(eData) { outData[it].position }

            var pos = 0
            while (pos < N) {
                // S_r = P_{J[r]} ⊕ Σ_{i∉L} a(J[r], i)·D_i
                val S = IntArray(eData)
                for (r in 0 until eData) {
                    val j = J[r]
                    val pj = parity[j]!!.at(pBase[j] + pos).i8
                    var accumKnown = 0
                    val crow = c[j]
                    // known data (i not in L)
                    for (i in 0 until k) {
                        if (outMap.containsKey(i)) continue // i in L
                        val di = data[i]!!
                        val s = di.at(di.position + pos).i8
                        if (s != 0) {
                            val a = crow[i]
                            accumKnown = accumKnown xor (mulLUT[(a shl 8) or s].toInt() and 0xFF)
                        }
                    }
                    S[r] = pj xor accumKnown
                }

                // For each lost data t: v = Σ_c Minv[t][c] * S[c]
                for (t in 0 until eData) {
                    var v = 0
                    val row = Minv[t]
                    for (cIdx in 0 until eData) {
                        val a = row[cIdx]
                        if (a != 0) v = v xor (mulLUT[(a shl 8) or S[cIdx]].toInt() and 0xFF)
                    }
                    val dst = outMap[lostDataIdx[t]]!!
                    val dOff = outBase[t] + pos
                    dst.at(dOff).i8 = v
                }
                pos++
            }
            repaired += eData

            // If parity also lost -> recompute those now that all data is available
            if (eParity > 0) {
                // Assemble full data array (use recovered outputs for the lost indices)
                val full = arrayOfNulls<ByteBufferL>(k)
                for (i in 0 until k) {
                    full[i] = outMap[i] ?: data[i]!!   // ← nullable Array に埋める
                }
                for (idx in lostParityIdx.indices) {
                    val j = lostParityIdx[idx]
                    val dst = outParity[idx]
                    encodeRowInto(dst, full, c, j)      // OK: Array<ByteBufferL?>
                    repaired++
                }
            }

            return repaired
        }

        // Nothing to do
        return 0
    }

    override fun attachScratch(buf: ByteBufferL?) {
        // RS hot path does not require external scratch currently.
        // (Matrix ops allocate small IntArrays per reconstruct(); e ≤ m is typically tiny.)
    }

    override fun scratchBytesHint(k: Int, m: Int): Int = 0

    /* ---------------- Inner kernels ---------------- */

    /** dst ^= a * src (byte-wise), absolute offsets; dst/src positions are preserved. */
    private inline fun saxpy(dst: ByteBufferL, src: ByteBufferL, a: Int, srcBase: Int) {
        if (a == 0) return
        val N = blockSize
        val lutBase = a shl 8
        val dBase = dst.position
        var p = 0
        while (p < N) {
            val s = src.at(srcBase + p).i8
            if (s != 0) {
                val m = mulLUT[lutBase or s].toInt() and 0xFF
                val d = dst.at(dBase + p).i8
                dst.at(dBase + p).i8 = d xor m
            }
            p++
        }
    }

    /** zero-fill parity blocks; absolute offsets only (positions preserved). */
    private fun zeroBlocks(arr: Array<ByteBufferL>) {
        val N = blockSize
        for (bb in arr) {
            val base = bb.position
            var p = 0
            while (p < N) {
                bb.at(base + p).i8 = 0
                p++
            }
        }
    }

    /** Encode only row j into dst from data[], using coeff cache c. */
    private fun encodeRowInto(dst: ByteBufferL, data: Array<ByteBufferL?>, c: Array<IntArray>, j: Int) {
        require(j in c.indices) { "row j=$j out of range" }
        val N = blockSize
        val base = dst.position
        // zero dst
        var p = 0
        while (p < N) {
            dst.at(base + p).i8 = 0
            p++
        }
        // accumulate
        for (i in data.indices) {
            val di = data[i] ?: continue
            val a = c[j][i]
            if (a == 0) continue
            saxpy(dst, di, a, di.position)
        }
    }

    /* ---------------- Small GF helpers (matrix) ---------------- */

    // Gauss–Jordan elimination for small dense matrix over GF(256)
    private fun invertMatrix(a0: Array<IntArray>): Array<IntArray> {
        val n = a0.size
        val a = Array(n) { r -> a0[r].clone() }
        val inv = Array(n) { r -> IntArray(n).apply { this[r] = 1 } }

        var row = 0
        for (col in 0 until n) {
            // pivot
            var pivot = row
            while (pivot < n && a[pivot][col] == 0) pivot++
            if (pivot == n) continue // singular; should not happen for Vandermonde subsets

            if (pivot != row) {
                val tmp = a[pivot]; a[pivot] = a[row]; a[row] = tmp
                val tmp2 = inv[pivot]; inv[pivot] = inv[row]; inv[row] = tmp2
            }

            val invPivot = gfInv(a[row][col])
            for (c in 0 until n) {
                a[row][c] = gfMul(a[row][c], invPivot)
                inv[row][c] = gfMul(inv[row][c], invPivot)
            }

            for (r in 0 until n) if (r != row) {
                val factor = a[r][col]
                if (factor != 0) {
                    for (c in 0 until n) {
                        a[r][c] = gfAdd(a[r][c], gfMul(factor, a[row][c]))
                        inv[r][c] = gfAdd(inv[r][c], gfMul(factor, inv[row][c]))
                    }
                }
            }
            row++
        }
        return inv
    }

    /* ---------------- guards ---------------- */

    private fun requireAllHave(size: Int, arr: Array<ByteBufferL>) {
        for (b in arr) {
            require(b.remaining >= size) {
                "buffer remaining < blockSize: remaining=${b.remaining}, required=$size"
            }
        }
    }
}
