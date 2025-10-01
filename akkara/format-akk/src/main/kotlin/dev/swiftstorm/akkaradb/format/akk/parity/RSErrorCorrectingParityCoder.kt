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
        require(k > 0); require(m == parityCount); require(k + m <= 255)
        requireAllHave(blockSize, data); requireAllHave(blockSize, parityOut)
        ensureCoeff(k)
        val c = coeff!!

        zeroBlocks(parityOut)
        for (i in 0 until k) {
            val src = data[i]
            val sBase = src.position
            for (j in 0 until m) saxpy(parityOut[j], src, c[j][i], sBase)
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
        val pBase = IntArray(m) { parity[it].position }
        var pos = 0
        while (pos < N) {
            for (j in 0 until m) {
                var acc = 0
                val row = c[j]
                for (i in 0 until k) {
                    val s = data[i].at(data[i].position + pos).i8
                    if (s != 0) acc = acc xor (mulLUT[(row[i] shl 8) or s].toInt() and 0xFF)
                }
                if (acc != parity[j].at(pBase[j] + pos).i8) return false
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

        if (eData == 0 && eParity > 0) {
            for (idx in lostParityIdx.indices) encodeRowInto(outParity[idx], data, c, lostParityIdx[idx])
            return eParity
        }

        if (eData > 0) {
            require(eData <= m) { "too many data erasures: $eData > m=$m" }
            val availRows = ArrayList<Int>(m); for (j in 0 until m) if (parity[j] != null) availRows += j
            require(availRows.size >= eData) { "need ≥$eData parity rows; available=${availRows.size}" }
            val J = IntArray(eData) { availRows[it] }

            val M = Array(eData) { IntArray(eData) }
            ensureCoeff(k)
            val coeffs = coeff!!
            for (r in 0 until eData) for (t in 0 until eData) M[r][t] = coeffs[J[r]][lostDataIdx[t]]
            val Minv = invertMatrix(M)

            val N = blockSize
            val pBase = IntArray(m) { parity[it]?.position ?: 0 }
            val outMap = HashMap<Int, ByteBufferL>(eData)
            for (t in 0 until eData) outMap[lostDataIdx[t]] = outData[t]
            val outBase = IntArray(eData) { outData[it].position }

            var pos = 0
            while (pos < N) {
                val S = IntArray(eData)
                for (r in 0 until eData) {
                    val j = J[r]
                    var accum = parity[j]!!.at(pBase[j] + pos).i8
                    val crow = coeffs[j]
                    for (i in 0 until k) {
                        if (outMap.containsKey(i)) continue
                        val s = data[i]!!.at(data[i]!!.position + pos).i8
                        if (s != 0) accum = accum xor (mulLUT[(crow[i] shl 8) or s].toInt() and 0xFF)
                    }
                    S[r] = accum
                }
                for (t in 0 until eData) {
                    var v = 0
                    val row = Minv[t]
                    for (cIdx in 0 until eData) {
                        val a = row[cIdx]; if (a != 0) v = v xor (mulLUT[(a shl 8) or S[cIdx]].toInt() and 0xFF)
                    }
                    outMap[lostDataIdx[t]]!!.at(outBase[t] + pos).i8 = v
                }
                pos++
            }
            repaired += eData

            if (eParity > 0) {
                val full = Array<ByteBufferL?>(k) { idx -> outMap[idx] ?: data[idx]!! }
                for (idx in lostParityIdx.indices) encodeRowInto(outParity[idx], full, coeffs, lostParityIdx[idx])
                repaired += eParity
            }
        }
        return repaired
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
        for (i in 0 until k) if (presentData[i] == null) {
            missing[missCnt++] = i
        }
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

        val S = IntArray(m)
        var sigma: IntArray
        val tmp = IntArray(m + 1)

        for (i in 0 until missCnt) outIndices[i] = missing[i]

        for (pos in 0 until blockSize) {
            val cword = IntArray(n)
            for (i in 0 until k) cword[i] = (presentData[i]?.at(presentData[i]!!.position + pos)?.i8 ?: 0)
            for (j in 0 until m) cword[k + j] = (presentParity[j]?.at(presentParity[j]!!.position + pos)?.i8 ?: 0)

            // syndromes S_r = c(α^{r+1})
            for (r in 0 until m) {
                var acc = 0
                var xPow = 1
                val a = r + 1
                for (i in 0 until n) {
                    val ci = cword[i]
                    if (ci != 0) acc = gfAdd(acc, gfMul(ci, xPow))
                    xPow = gfMul(xPow, gfPowAlpha(a))
                }
                S[r] = acc
            }
            var allZero = true
            for (r in 0 until m) if (S[r] != 0) {
                allZero = false; break
            }
            if (allZero) continue

            val erasures = erasuresSet.toIntArray()
            val erasureLoc = makeErasureLocatorPoly(erasures)
            var L = 0
            var B = intArrayOf(1)
            var b = 1
            sigma = intArrayOf(1)
            sigma = polyMulInplace(sigma, erasureLoc, tmp)

            for (r in 0 until m) {
                val d = polyDiscrepancy(sigma, S, r)
                if (d == 0) {
                    B = polyShift(B); continue
                }
                val T = sigma.copyOf()
                val scale = gfMul(d, gfInv(b))
                sigma = polyAdd(sigma, polyScale(B, scale, tmp))
                if (2 * L <= r) {
                    L = r + 1 - L; B = T; b = d
                }
                B = polyShift(B)
            }

            // Chien search
            val errorLocs = IntArray(n)
            var errN = 0
            for (i in 0 until n) {
                val xInv = if (i == 0) 1 else gfPowAlpha(255 - (i % 255))
                if (polyEval(sigma, xInv) == 0) errorLocs[errN++] = i
            }
            require(errN <= m) { "too many errors: $errN > m=$m" }

            // Forney
            val omega = IntArray(m)
            for (i in 0 until m) {
                var acc = 0
                for (j in 0..i) {
                    val s = if (j < S.size) S[j] else 0
                    val sig = if (i - j < sigma.size) sigma[i - j] else 0
                    if (s != 0 && sig != 0) acc = gfAdd(acc, gfMul(s, sig))
                }
                omega[i] = acc
            }
            val sigmaPrime = IntArray(maxOf(0, sigma.size - 1))
            for (i in 1 until sigma.size) if ((i and 1) == 1) sigmaPrime[i - 1] = sigma[i]

            val errVals = IntArray(errN)
            for (e in 0 until errN) {
                val loc = errorLocs[e]
                val xInv = if (loc == 0) 1 else gfPowAlpha(255 - (loc % 255))
                val num = polyEval(omega, xInv)
                val den = polyEval(sigmaPrime, xInv)
                require(den != 0) { "sigma'(x)=0 at loc=$loc" }
                errVals[e] = gfMul(num, gfInv(den))
            }

            // apply & write recovered symbols for targets only（data領域の復元だけ outBuffers へ）
            for (e in 0 until errN) {
                val i = errorLocs[e]
                cword[i] = gfAdd(cword[i], errVals[e])
            }
            for (t in 0 until missCnt) {
                val dataIdx = outIndices[t]
                outBuffers[t].at(outBuffers[t].position + pos).i8 = cword[dataIdx]
            }
        }
        return missCnt
    }

    /* ---------------- kernels & helpers ---------------- */

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

    private fun zeroBlocks(arr: Array<ByteBufferL>) {
        val N = blockSize
        for (bb in arr) {
            val base = bb.position
            var p = 0
            while (p < N) {
                bb.at(base + p).i8 = 0; p++
            }
        }
    }

    private fun encodeRowInto(dst: ByteBufferL, data: Array<ByteBufferL?>, c: Array<IntArray>, j: Int) {
        val N = blockSize
        val base = dst.position
        var p = 0; while (p < N) {
            dst.at(base + p).i8 = 0; p++
        }
        for (i in data.indices) {
            val di = data[i] ?: continue
            val a = c[j][i]; if (a == 0) continue
            saxpy(dst, di, a, di.position)
        }
    }

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
            for (c in 0 until n) {
                a[row][c] = gfMul(a[row][c], invPivot); inv[row][c] = gfMul(inv[row][c], invPivot)
            }
            for (r in 0 until n) if (r != row) {
                val f = a[r][col]; if (f != 0) for (c in 0 until n) {
                    a[r][c] = gfAdd(a[r][c], gfMul(f, a[row][c]))
                    inv[r][c] = gfAdd(inv[r][c], gfMul(f, inv[row][c]))
                }
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
        for (i in poly.indices.reversed()) acc = gfAdd(gfMul(acc, x), poly[i])
        return acc
    }

    private fun polyDiscrepancy(sigma: IntArray, S: IntArray, r: Int): Int {
        var acc = 0
        val up = minOf(r, sigma.lastIndex)
        var i = 0; while (i <= up) {
            val s = S[r - i]; if (s != 0) acc = gfAdd(acc, gfMul(sigma[i], s)); i++
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
        for (i in 0 until n) {
            val ai = if (i < a.size) a[i] else 0
            val bi = if (i < b.size) b[i] else 0
            out[i] = gfAdd(ai, bi)
        }
        return out
    }

    private fun polyScale(a: IntArray, s: Int, tmp: IntArray): IntArray {
        val out = if (a.size <= tmp.size) tmp else IntArray(a.size)
        for (i in a.indices) out[i] = gfMul(a[i], s)
        return out.copyOf(a.size)
    }

    private fun polyMul(a: IntArray, b: IntArray): IntArray {
        val out = IntArray(a.size + b.size - 1)
        for (i in a.indices) {
            val ai = a[i]; if (ai == 0) continue
            for (j in b.indices) {
                val bj = b[j]; if (bj == 0) continue
                out[i + j] = gfAdd(out[i + j], gfMul(ai, bj))
            }
        }
        return out
    }

    private fun polyMulInplace(a: IntArray, b: IntArray, tmp: IntArray): IntArray {
        Arrays.fill(tmp, 0)
        val n = a.size + b.size - 1
        for (i in a.indices) {
            val ai = a[i]; if (ai == 0) continue
            for (j in b.indices) {
                val bj = b[j]; if (bj == 0) continue
                tmp[i + j] = gfAdd(tmp[i + j], gfMul(ai, bj))
            }
        }
        return tmp.copyOf(n)
    }
}
