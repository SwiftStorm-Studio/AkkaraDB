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
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.ECParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityKind
import java.util.*

class RSErrorCorrectingParityCoder(
    override val parityCount: Int,
    override val blockSize: Int = BLOCK_SIZE,
    private val pool: BufferPool = Pools.io()
) : ECParityCoder {

    init {
        require(parityCount >= 1) { "parityCount must be ≥ 1" }
    }

    override val kind: ParityKind = ParityKind.RS_EC
    override val supportsErrorCorrection: Boolean get() = true

    /* ---------- GF(256) tables & mulLUT ---------- */
    private val exp = IntArray(512)
    private val log = IntArray(256)
    private val mulLUT: ByteArray = ByteArray(256 * 256)

    init {
        var x = 1
        for (i in 0 until 255) {
            exp[i] = x; log[x] = i
            x = x shl 1; if ((x and 0x100) != 0) x = x xor 0x11D
        }
        for (i in 255 until 512) exp[i] = exp[i - 255]
        log[0] = 0
        var idx = 0
        for (a in 0..255) for (b in 0..255) mulLUT[idx++] = gfMulExpLog(a, b).toByte()
    }

    private inline fun gfAdd(a: Int, b: Int) = a xor b
    private inline fun gfMul(a: Int, b: Int) = mulLUT[(a shl 8) or b].toInt() and 0xFF
    private inline fun gfMulExpLog(a: Int, b: Int): Int {
        if (a == 0 || b == 0) return 0; return exp[log[a] + log[b]]
    }
    private inline fun gfInv(a: Int): Int {
        require(a != 0) { "gfInv(0)" }; return exp[255 - log[a]]
    }
    private inline fun gfPowAlpha(ei: Int) = exp[ei % 255]

    /* ---------- Vandermonde coeff cache a(j,i)=α^{(j+1)·i} ---------- */
    @Volatile
    private var cachedK = -1
    @Volatile
    private var coeff: Array<IntArray>? = null
    private fun ensureCoeff(k: Int) {
        val c = coeff; if (c != null && cachedK == k) return
        val newC = Array(parityCount) { j -> IntArray(k) { i -> if (i == 0) 1 else gfPowAlpha((j + 1) * i) } }
        coeff = newC; cachedK = k
    }

    /* ================= ParityCoder ================= */

    override fun encodeInto(data: Array<ByteBufferL>, parityOut: Array<ByteBufferL>) {
        val k = data.size
        val m = parityOut.size
        require(k > 0) { "data is empty" }
        require(m == parityCount) { "parityOut.size=$m but parityCount=$parityCount" }
        require(k + m <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$m" }
        requireAllHave(blockSize, data); requireAllHave(blockSize, parityOut)
        ensureCoeff(k)
        val c = coeff!!

        zeroBlocks(parityOut)

        val sBase = IntArray(k) { i -> data[i].position }
        for (i in 0 until k) {
            val aCol = IntArray(m) { j -> c[j][i] }
            for (j in 0 until m) if (aCol[j] != 0) {
                saxpy(parityOut[j], parityOut[j].position, data[i], sBase[i], aCol[j])
            }
        }
    }

    override fun verify(data: Array<ByteBufferL>, parity: Array<ByteBufferL>): Boolean {
        val k = data.size
        val m = parity.size
        require(k > 0); require(m == parityCount); require(k + m <= 255)
        requireAllHave(blockSize, data); requireAllHave(blockSize, parity)
        ensureCoeff(k)
        val c = coeff!!

        val N = blockSize
        val dBase = IntArray(k) { i -> data[i].position }
        val pBase = IntArray(m) { j -> parity[j].position }

        var pos = 0
        while (pos < N) {
            for (j in 0 until m) {
                var acc = 0
                val row = c[j]
                var i = 0
                while (i < k) {
                    val s = data[i].getU8At(dBase[i] + pos)
                    if (s != 0) acc = acc xor (mulLUT[(row[i] shl 8) or s].toInt() and 0xFF)
                    i++
                }
                if (acc != parity[j].getU8At(pBase[j] + pos)) return false
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
        require(parity.size == m); require(k + m <= 255)
        for (d in data) if (d != null) require(d.remaining >= blockSize)
        for (p in parity) if (p != null) require(p.remaining >= blockSize)
        for (od in outData) require(od.remaining >= blockSize)
        for (op in outParity) require(op.remaining >= blockSize)

        ensureCoeff(k)
        val c = coeff!!

        val eData = lostDataIdx.size
        val eParity = lostParityIdx.size
        var repaired = 0

        // parity-only losses → recompute requested rows
        if (eData == 0 && eParity > 0) {
            val arrData = Array(k) { requireNotNull(data[it]) { "need all data to recompute parity" } }
            for (idx in lostParityIdx.indices) {
                val j = lostParityIdx[idx]
                encodeRowInto(outParity[idx], arrData, c, j)
                repaired++
            }
            return repaired
        }

        if (eData > 0) {
            require(eData <= m) { "too many data erasures: $eData > m=$m" }

            val availRows = ArrayList<Int>(m)
            for (j in 0 until m) if (parity[j] != null) availRows += j
            require(availRows.size >= eData) { "need at least $eData parity rows; available=${availRows.size}" }
            val J = IntArray(eData) { availRows[it] }

            // Build M (eData×eData) where M[r][t] = a(J[r], L[t])
            val M = Array(eData) { IntArray(eData) }
            for (r in 0 until eData) {
                val row = c[J[r]]
                for (t in 0 until eData) M[r][t] = row[lostDataIdx[t]]
            }
            val Minv = invertMatrix(M)

            val N = blockSize
            val pBase = IntArray(m) { j -> parity[j]?.position ?: 0 }
            val outMap = HashMap<Int, ByteBufferL>(eData)
            for (t in 0 until eData) outMap[lostDataIdx[t]] = outData[t]
            val outBase = IntArray(eData) { outData[it].position }
            val dBase = IntArray(k) { i -> data[i]?.position ?: 0 }

            var pos = 0
            while (pos < N) {
                // S_r = P_{J[r]} ⊕ Σ_{i∉L} a(J[r],i)·D_i
                val S = IntArray(eData)
                for (r in 0 until eData) {
                    val j = J[r]
                    var accumKnown = parity[j]?.getU8At(pBase[j] + pos) ?: 0
                    val crow = c[j]
                    var i = 0
                    while (i < k) {
                        if (outMap.containsKey(i)) {
                            i++; continue
                        } // i in L
                        val db = data[i]
                        if (db != null) {
                            val s = db.getU8At(dBase[i] + pos)
                            if (s != 0) accumKnown = accumKnown xor (mulLUT[(crow[i] shl 8) or s].toInt() and 0xFF)
                        }
                        i++
                    }
                    S[r] = accumKnown
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
                    dst.putU8At(ob, v)
                }
                pos++
            }
            repaired += eData

            // also parity losses → recompute after data recovery
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

    /* ================= ECParityCoder ================= */

    override fun decodeEC(
        presentData: Array<ByteBufferL?>,
        presentParity: Array<ByteBufferL?>,
        knownErasures: IntArray?
    ): Pair<IntArray, Array<ByteBufferL>> {
        val k = presentData.size
        var missCnt = 0
        val missing = IntArray(k) { -1 }
        for (i in 0 until k) if (presentData[i] == null) missing[missCnt++] = i
        require(missCnt > 0) { "nothing to decode" }

        val outIdx = IntArray(missCnt)
        val outBuf = Array(missCnt) { pool.get(blockSize) }

        val n = decodeECInto(presentData, presentParity, knownErasures, outIdx, outBuf)

        val outIdxTrim = outIdx.copyOf(n)
        val outBufTrim = Array(n) { i -> outBuf[i] }
        return outIdxTrim to outBufTrim
    }

    override fun decodeECInto(
        presentData: Array<ByteBufferL?>,
        presentParity: Array<ByteBufferL?>,
        knownErasures: IntArray?,
        outIndices: IntArray,
        outBuffers: Array<ByteBufferL>
    ): Int {
        val k = presentData.size
        val m = parityCount
        val n = k + m
        require(presentParity.size == m); require(k + m <= 255)
        for (p in presentParity) require(p == null || p.remaining >= blockSize)
        for (d in presentData) require(d == null || d.remaining >= blockSize)

        var missCnt = 0
        val missing = IntArray(k)
        for (i in 0 until k) if (presentData[i] == null) missing[missCnt++] = i
        require(missCnt > 0) { "nothing to decode" }
        require(outIndices.size >= missCnt && outBuffers.size >= missCnt) { "output arrays too small" }

        val erasuresSet = (knownErasures?.toMutableSet() ?: mutableSetOf())
        for (i in 0 until missCnt) erasuresSet.add(missing[i])

        // cache bases
        val dBase = IntArray(k) { i -> presentData[i]?.position ?: 0 }
        val pBase = IntArray(m) { j -> presentParity[j]?.position ?: 0 }

        // prepare outputs
        for (i in 0 until missCnt) outIndices[i] = missing[i]

        val S = IntArray(m)
        val tmp = IntArray(m + 1)

        for (pos in 0 until blockSize) {
            // syndromes S_r = c(α^{r+1})
            var r = 0
            while (r < m) {
                val a = r + 1
                var acc = 0
                // data part
                var i = 0
                var xPow = 1
                while (i < k) {
                    val ci = presentData[i]?.getU8At(dBase[i] + pos) ?: 0
                    if (ci != 0) acc = gfAdd(acc, gfMul(ci, xPow))
                    xPow = gfMul(xPow, gfPowAlpha(a))
                    i++
                }
                // parity part
                var j = 0
                xPow = 1
                while (j < m) {
                    val cj = presentParity[j]?.getU8At(pBase[j] + pos) ?: 0
                    if (cj != 0) acc = gfAdd(acc, gfMul(cj, xPow))
                    xPow = gfMul(xPow, gfPowAlpha(a))
                    j++
                }
                S[r] = acc; r++
            }

            // all-zero syndrome → skip
            var allZero = true
            r = 0
            while (r < m) {
                if (S[r] != 0) {
                    allZero = false; break
                }; r++
            }
            if (allZero) continue

            // erasure locator from hints
            val erasures = erasuresSet.toIntArray()
            val erasureLoc = makeErasureLocatorPoly(erasures)

            // Berlekamp–Massey
            var L = 0
            var B = intArrayOf(1)
            var b = 1
            var sigma = intArrayOf(1)
            sigma = polyMulInplace(sigma, erasureLoc, tmp)

            r = 0
            while (r < m) {
                val d = polyDiscrepancy(sigma, S, r)
                if (d == 0) {
                    B = polyShift(B); r++; continue
                }
                val T = sigma.copyOf()
                val scale = gfMul(d, gfInv(b))
                sigma = polyAdd(sigma, polyScale(B, scale, tmp))
                if (2 * L <= r) {
                    L = r + 1 - L; B = T; b = d
                }
                B = polyShift(B); r++
            }

            // Chien search
            val errorLocs = IntArray(n)
            var errN = 0
            var idx = 0
            while (idx < n) {
                val xInv = if (idx == 0) 1 else gfPowAlpha(255 - (idx % 255))
                if (polyEval(sigma, xInv) == 0) errorLocs[errN++] = idx
                idx++
            }
            require(errN <= m) { "too many errors: $errN > m=$m" }

            // Forney
            val omega = IntArray(m)
            var i = 0
            while (i < m) {
                var acc = 0
                var j = 0
                while (j <= i) {
                    val s = if (j < S.size) S[j] else 0
                    val sig = if (i - j < sigma.size) sigma[i - j] else 0
                    if (s != 0 && sig != 0) acc = gfAdd(acc, gfMul(s, sig))
                    j++
                }
                omega[i] = acc; i++
            }
            val sigmaPrime = IntArray(maxOf(0, sigma.size - 1))
            i = 1
            while (i < sigma.size) {
                if ((i and 1) == 1) sigmaPrime[i - 1] = sigma[i]
                i++
            }

            val errVals = IntArray(errN)
            i = 0
            while (i < errN) {
                val loc = errorLocs[i]
                val xInv = if (loc == 0) 1 else gfPowAlpha(255 - (loc % 255))
                val num = polyEval(omega, xInv)
                val den = polyEval(sigmaPrime, xInv)
                require(den != 0) { "sigma'(x)=0 at loc=$loc" }
                errVals[i] = gfMul(num, gfInv(den))
                i++
            }

            var t = 0
            while (t < missCnt) {
                val dataIdx = outIndices[t]
                var valAt = presentData[dataIdx]?.getU8At(dBase[dataIdx] + pos) ?: 0
                var e = 0
                while (e < errN) {
                    if (errorLocs[e] == dataIdx) {
                        valAt = gfAdd(valAt, errVals[e]); break
                    }
                    e++
                }
                val ob = outBuffers[t].position + pos
                outBuffers[t].putU8At(ob, valAt)
                t++
            }
        }
        return missCnt
    }

    /* ---------------- kernels & helpers ---------------- */

    /** dst ^= a * src */
    private inline fun saxpy(dst: ByteBufferL, dstBase: Int, src: ByteBufferL, srcBase: Int, a: Int) {
        val N = blockSize
        val lutBase = a shl 8
        var p = 0
        while (p < N) {
            val s = src.getU8At(srcBase + p)
            if (s != 0) {
                val m = mulLUT[lutBase or s].toInt() and 0xFF
                val d = dst.getU8At(dstBase + p)
                dst.putU8At(dstBase + p, d xor m)
            }
            p++
        }
    }

    /** Zero all parity buffers up to blockSize bytes. */
    private fun zeroBlocks(parityOut: Array<ByteBufferL>) {
        for (b in parityOut) b.fillZero(blockSize)
    }

    private fun encodeRowInto(dst: ByteBufferL, data: Array<ByteBufferL>, c: Array<IntArray>, j: Int) {
        val base = dst.position
        dst.fillZero(blockSize)
        val k = data.size
        val dBase = IntArray(k) { i -> data[i].position }
        for (i in 0 until k) {
            val a = c[j][i]; if (a == 0) continue
            saxpy(dst, base, data[i], dBase[i], a)
        }
    }

    // Gauss–Jordan
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
                val t = a[pivot]; a[pivot] = a[row]; a[row] = t
                val t2 = inv[pivot]; inv[pivot] = inv[row]; inv[row] = t2
            }
            val invPivot = gfInv(a[row][col])
            var c = 0
            while (c < n) {
                a[row][c] = gfMul(a[row][c], invPivot)
                inv[row][c] = gfMul(inv[row][c], invPivot)
                c++
            }
            var r = 0
            while (r < n) {
                if (r != row) {
                    val f = a[r][col]
                    if (f != 0) {
                        c = 0
                        while (c < n) {
                            a[r][c] = gfAdd(a[r][c], gfMul(f, a[row][c]))
                            inv[r][c] = gfAdd(inv[r][c], gfMul(f, inv[row][c]))
                            c++
                        }
                    }
                }
                r++
            }
            row++
        }
        return inv
    }

    private fun requireAllHave(size: Int, arr: Array<ByteBufferL>) {
        for (b in arr) require(b.remaining >= size) {
            "buffer remaining < blockSize: remaining=${b.remaining}, required=$size"
        }
    }

    /* ---------- BM/Chien helpers ---------- */
    private fun makeErasureLocatorPoly(erasures: IntArray?): IntArray {
        if (erasures == null || erasures.isEmpty()) return intArrayOf(1)
        var poly = intArrayOf(1)
        for (e in erasures) {
            val aPow = if (e == 0) 1 else gfPowAlpha(e % 255)
            val term = intArrayOf(1, aPow) // 1 + (α^e)x
            poly = polyMul(poly, term)
        }
        return poly
    }

    private fun polyEval(poly: IntArray, x: Int): Int {
        var acc = 0
        var i = poly.size - 1
        while (i >= 0) {
            acc = gfAdd(gfMul(acc, x), poly[i]); i--
        }
        return acc
    }

    private fun polyDiscrepancy(sigma: IntArray, S: IntArray, r: Int): Int {
        var acc = 0
        val up = minOf(r, sigma.lastIndex)
        var i = 0
        while (i <= up) {
            val s = S[r - i]
            if (s != 0) acc = gfAdd(acc, gfMul(sigma[i], s))
            i++
        }
        return acc
    }

    private fun polyShift(a: IntArray): IntArray {
        val out = IntArray(a.size + 1)
        System.arraycopy(a, 0, out, 1, a.size)
        return out
    }

    private fun polyAdd(a: IntArray, b: IntArray): IntArray {
        val n = maxOf(a.size, b.size)
        val out = IntArray(n)
        var i = 0
        while (i < n) {
            val ai = if (i < a.size) a[i] else 0
            val bi = if (i < b.size) b[i] else 0
            out[i] = gfAdd(ai, bi)
            i++
        }
        return out
    }

    private fun polyScale(a: IntArray, s: Int, tmp: IntArray): IntArray {
        val out = if (a.size <= tmp.size) tmp else IntArray(a.size)
        var i = 0
        while (i < a.size) {
            out[i] = gfMul(a[i], s); i++
        }
        return out.copyOf(a.size)
    }

    private fun polyMul(a: IntArray, b: IntArray): IntArray {
        val out = IntArray(a.size + b.size - 1)
        var i = 0
        while (i < a.size) {
            val ai = a[i]; if (ai != 0) {
                var j = 0
                while (j < b.size) {
                    val bj = b[j]; if (bj != 0) out[i + j] = gfAdd(out[i + j], gfMul(ai, bj))
                    j++
                }
            }
            i++
        }
        return out
    }

    private fun polyMulInplace(a: IntArray, b: IntArray, tmp: IntArray): IntArray {
        Arrays.fill(tmp, 0)
        val n = a.size + b.size - 1
        var i = 0
        while (i < a.size) {
            val ai = a[i]; if (ai != 0) {
                var j = 0
                while (j < b.size) {
                    val bj = b[j]; if (bj != 0) tmp[i + j] = gfAdd(tmp[i + j], gfMul(ai, bj))
                    j++
                }
            }
            i++
        }
        return tmp.copyOf(n)
    }
}