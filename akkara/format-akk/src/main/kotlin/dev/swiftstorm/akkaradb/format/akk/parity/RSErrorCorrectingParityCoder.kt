@file:Suppress("NOTHING_TO_INLINE")

package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import kotlin.math.min

/**
 * Reed–Solomon coder over GF(2^8) that supports both errors (unknown locations) and erasures (known).
 *
 * - Field: GF(256) with primitive polynomial 0x11D
 * - Code: systematic Vandermonde evaluation at points {alpha^i | i = 0..n-1}
 * - Encode: P_j = Σ_i a(j,i) * D_i, with a(j,i) = alpha^{(j+1)*i}
 * - Decode: Berlekamp–Massey (+ erasure initialization) → Chien search → Forney
 *
 * Constraints:
 * - n = k + m ≤ 255
 * - Correctable if 2*t + s ≤ m  (t: unknown-position errors, s: known erasures)
 */
class RSErrorCorrectingParityCoder(
    override val parityCount: Int,
    private val pool: BufferPool = Pools.io()
) : ParityCoder {

    init {
        require(parityCount >= 1) { "parityCount must be ≥ 1" }
    }

    /* ---------------- GF(256) tables (poly = 0x11D) ---------------- */
    private val exp = IntArray(512)
    private val log = IntArray(256)

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
    }

    private val mulLUT: ByteArray = ByteArray(256 * 256).also { lut ->
        var idx = 0
        for (a in 0..255) {
            for (b in 0..255) {
                lut[idx++] = gfMulExpLog(a, b).toByte()
            }
        }
    }

    private inline fun gfAdd(a: Int, b: Int): Int = a xor b

    private inline fun gfMul(a: Int, b: Int): Int = mulLUT[(a shl 8) or b].toInt() and 0xFF

    private inline fun gfMulExpLog(a: Int, b: Int): Int {
        if (a == 0 || b == 0) return 0
        return exp[log[a] + log[b]]
    }

    private inline fun gfInv(a: Int): Int {
        require(a != 0) { "gfInv(0)" }
        return exp[255 - log[a]]
    }

    private inline fun gfPowAlpha(ei: Int): Int = exp[ei % 255] // α^ei

    @Volatile
    private var cachedK: Int = -1

    /** coeff[j][i] = α^{(j+1)·i} */
    @Volatile
    private var coeff: Array<IntArray>? = null

    private fun ensureCoeff(k: Int) {
        val curr = coeff
        if (curr != null && cachedK == k) return
        val newCoeff = Array(parityCount) { j -> IntArray(k) { i -> if (i == 0) 1 else gfPowAlpha((j + 1) * i) } }
        coeff = newCoeff
        cachedK = k
    }

    /* ---------------- Encode (SAXPY + LUT; 改善 #1) ---------------- */

    override fun encode(dataBlocks: List<ByteBufferL>): List<ByteBufferL> {
        require(dataBlocks.isNotEmpty())
        val k = dataBlocks.size
        val blks = dataBlocks.map { it.duplicate() }
        blks.forEach { require(it.remaining == BLOCK_SIZE) { "block must be 32 KiB" } }

        require(k + parityCount <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$parityCount" }

        val parity = ArrayList<ByteBufferL>(parityCount)
        repeat(parityCount) { parity += pool.get(BLOCK_SIZE) }
        parity.forEach { it.clear() }

        ensureCoeff(k)
        val c = coeff!!

        // SAXPY: for i in data lanes → for j in parity lanes
        for (i in 0 until k) {
            val src = blks[i].duplicate()
            for (j in 0 until parityCount) {
                saxpy(parity[j], src, c[j][i])
            }
        }

        parity.forEach { it.limit(BLOCK_SIZE).position(0) }
        return parity
    }

    /** P_j ← P_j ⊕ (a · D_i) */
    private inline fun saxpy(dst: ByteBufferL, src: ByteBufferL, a: Int) {
        val N = BLOCK_SIZE
        val lutBase = a shl 8
        var p = 0
        while (p < N) {
            val s = src.get(p).toInt() and 0xFF
            val m = mulLUT[lutBase or s].toInt() and 0xFF
            val d = dst.get(p).toInt() and 0xFF
            dst.put(p, (d xor m).toByte())
            p++
        }
    }

    /* ---------------- Decode with errors (+ optional erasures) ---------------- */

    override fun decode(lostIndex: Int, presentData: List<ByteBufferL?>, presentParity: List<ByteBufferL?>): ByteBufferL {
        throw NotImplementedError("use decodeWithErrors for error+erasure decoding")
    }

    /**
     * Recover all missing data blocks. Tolerates up to floor((m - s)/2) errors mixed in present blocks,
     * provided 2*t + s ≤ m. knownErasures are indices in 0..k-1 for missing data lanes (optional hint).
     *
     * @return list of recovered (index to buffer) pairs for each null entry in presentData, in the same order as indices
     */
    fun decodeWithErrors(
        presentData: List<ByteBufferL?>,
        presentParity: List<ByteBufferL?>,
        knownErasures: IntArray? = null
    ): List<Pair<Int, ByteBufferL>> {
        val k = presentData.size
        val m = parityCount
        require(presentParity.size == m) { "expected $m parity blocks, got ${presentParity.size}" }
        require(k + m <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$m" }

        val missingIdx = presentData.withIndex().filter { it.value == null }.map { it.index }
        require(missingIdx.isNotEmpty()) { "nothing to decode" }
        // sanity of buffers
        presentParity.forEach { p -> require(p == null || p.remaining == BLOCK_SIZE) { "bad parity size" } }
        presentData.forEach { d -> require(d == null || d.remaining == BLOCK_SIZE) { "bad data size" } }

        val out = missingIdx.map { it to pool.get(BLOCK_SIZE).apply { clear() } }.toMutableList()

        val n = k + m
        val erasuresSet = (knownErasures?.toMutableSet() ?: mutableSetOf())
        missingIdx.forEach { erasuresSet.add(it) }

        val parityAlive = presentParity.withIndex().filter { it.value != null }.map { it.index }
        require(parityAlive.size >= 1) { "no parity available" }

        val c = IntArray(n)       // codeword symbols at this position
        val isKnown = BooleanArray(n)
        val S = IntArray(m)       // syndromes
        var sigma = IntArray(m + 1)
        val tmp = IntArray(m + 1)

        for (pos in 0 until BLOCK_SIZE) {
            // c[0..k-1] = data, c[k..n-1] = parity
            for (i in 0 until k) {
                val b = if (presentData[i] != null) presentData[i]!!.get(pos).toInt() and 0xFF else 0
                c[i] = b
                isKnown[i] = presentData[i] != null
            }
            for (j in 0 until m) {
                val pbuf = presentParity[j]
                val b = if (pbuf != null) pbuf.get(pos).toInt() and 0xFF else 0
                c[k + j] = b
                isKnown[k + j] = pbuf != null
            }

            val erasures = erasuresSet.toIntArray()
            // syndromes S_r = c(alpha^{r+1})
            for (r in 0 until m) {
                var acc = 0
                var xPow = 1 // alpha^{(r+1)*0}
                val a = r + 1
                // data part
                for (i in 0 until k) {
                    val ci = c[i]
                    if (ci != 0) acc = gfAdd(acc, gfMul(ci, xPow))
                    xPow = gfMul(xPow, gfPowAlpha(a))
                }
                // parity part (evaluation points continue)
                for (j in 0 until m) {
                    val cj = c[k + j]
                    if (cj != 0) acc = gfAdd(acc, gfMul(cj, xPow))
                    xPow = gfMul(xPow, gfPowAlpha(a))
                }
                S[r] = acc
            }

            var allZero = true
            for (r in 0 until m) if (S[r] != 0) {
                allZero = false; break
            }
            if (allZero) continue

            // Berlekamp–Massey (with erasure init)
            val erasureLocPoly = makeErasureLocatorPoly(erasures)
            val erasureEval = polyEvalSyndromes(erasureLocPoly, S)
            var L = 0
            var B = IntArray(1) { 1 }
            var b = 1
            sigma.fill(0); sigma[0] = 1
            sigma = polyMulInplace(sigma, erasureLocPoly, tmp)

            for (r in 0 until m) {
                // discrepancy
                val d = polyDiscrepancy(sigma, S, r)
                if (d == 0) {
                    // shift B
                    B = polyShift(B)
                    continue
                }
                val T = sigma.copyOf()
                val scale = gfMul(d, gfInv(b))
                sigma = polyAdd(sigma, polyScale(B, scale, tmp))
                if (2 * L <= r) {
                    L = r + 1 - L
                    B = T
                    b = d
                }
                B = polyShift(B)
            }

            // Chien search for error locations among n symbols
            val errorLocs = mutableListOf<Int>()
            for (i in 0 until n) {
                val xInv = if (i == 0) 1 else gfPowAlpha(255 - (i % 255))
                if (polyEval(sigma, xInv) == 0) errorLocs.add(i)
            }
            // too many
            require(errorLocs.size <= m) { "too many errors: ${errorLocs.size} > m=$m" }

            // Forney error magnitudes
            val errVals = computeErrorMagnitudes(S, sigma, errorLocs.toIntArray())
            // Apply corrections into c[]
            for (idx in errorLocs.indices) {
                val i = errorLocs[idx]
                c[i] = gfAdd(c[i], errVals[idx])
            }

            for (t in out.indices) {
                val dataIndex = out[t].first
                val symbol = c[dataIndex]
                out[t].second.put(pos, symbol.toByte())
            }
        }

        // 完了
        out.forEach { it.second.limit(BLOCK_SIZE).position(0) }
        return out
    }

    /* ---------------- poly helpers (BM/Forney) ---------------- */

    private fun makeErasureLocatorPoly(erasures: IntArray?): IntArray {
        if (erasures == null || erasures.isEmpty()) return intArrayOf(1)
        // Γ(x) = Π (1 - x * alpha^{i})
        var poly = intArrayOf(1)
        for (e in erasures) {
            val aPow = if (e == 0) 1 else gfPowAlpha(e % 255)
            val term = intArrayOf(1, aPow) // 1 + (alpha^e) x
            poly = polyMul(poly, term)
        }
        return poly
    }

    private fun polyEvalSyndromes(poly: IntArray, S: IntArray): IntArray {
        val out = IntArray(S.size)
        for (r in S.indices) out[r] = polyEval(poly, gfPowAlpha(r + 1))
        return out
    }

    private fun polyEval(poly: IntArray, x: Int): Int {
        var acc = 0
        for (i in poly.indices.reversed()) acc = gfAdd(gfMul(acc, x), poly[i])
        return acc
    }

    private fun polyDiscrepancy(sigma: IntArray, S: IntArray, r: Int): Int {
        var acc = 0
        for (i in 0..min(r, sigma.lastIndex)) acc = gfAdd(acc, gfMul(sigma[i], S[r - i]))
        return acc
    }

    private fun polyShift(a: IntArray): IntArray {
        val out = IntArray(a.size + 1)
        for (i in a.indices) out[i + 1] = a[i]
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
            val ai = a[i]
            if (ai == 0) continue
            for (j in b.indices) {
                if (b[j] != 0) out[i + j] = gfAdd(out[i + j], gfMul(ai, b[j]))
            }
        }
        return out
    }

    private fun polyMulInplace(a: IntArray, b: IntArray, tmp: IntArray): IntArray {
        val out = tmp
        java.util.Arrays.fill(out, 0)
        val n = a.size + b.size - 1
        for (i in a.indices) {
            val ai = a[i]
            if (ai == 0) continue
            for (j in b.indices) {
                if (b[j] != 0) out[i + j] = gfAdd(out[i + j], gfMul(ai, b[j]))
            }
        }
        return out.copyOf(n)
    }

    private fun computeErrorMagnitudes(S: IntArray, sigma: IntArray, errorLocs: IntArray): IntArray {
        // ω(x) = [S(x) * σ(x)] mod x^m
        val omega = IntArray(parityCount)
        for (i in omega.indices) {
            var acc = 0
            for (j in 0..i) {
                val s = if (j < S.size) S[j] else 0
                val sig = if (i - j < sigma.size) sigma[i - j] else 0
                if (s != 0 && sig != 0) acc = gfAdd(acc, gfMul(s, sig))
            }
            omega[i] = acc
        }

        // σ'(x): σ(x) の formal derivative（GF(2^8) は特別扱い）
        val sigmaPrime = IntArray(sigma.size - 1)
        for (i in 1 until sigma.size) {
            if (i % 2 == 1) sigmaPrime[i - 1] = sigma[i]
        }
        val out = IntArray(errorLocs.size)
        for (idx in errorLocs.indices) {
            val i = errorLocs[idx]
            val xInv = if (i == 0) 1 else gfPowAlpha(255 - (i % 255))
            val num = polyEval(omega, xInv)
            val den = polyEval(sigmaPrime, xInv)
            require(den != 0) { "sigma'(x) = 0 at location i=$i" }
            out[idx] = gfMul(num, gfInv(den)) // 符号は GF(2^8) ではxorなのでこのままでOK
        }
        return out
    }
}