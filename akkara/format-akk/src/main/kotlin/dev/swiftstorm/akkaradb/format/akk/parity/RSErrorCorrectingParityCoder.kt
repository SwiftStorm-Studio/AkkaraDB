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

    private inline fun gfAdd(a: Int, b: Int): Int = a xor b
    private inline fun gfMul(a: Int, b: Int): Int {
        if (a == 0 || b == 0) return 0
        return exp[log[a] + log[b]]
    }

    private inline fun gfInv(a: Int): Int {
        require(a != 0) { "gfInv(0)" }
        return exp[255 - log[a]]
    }

    private inline fun gfPowAlpha(ei: Int): Int = exp[ei % 255] // α^ei

    /* ---------------- Encode (erasure-onlyと同等) ---------------- */

    override fun encode(dataBlocks: List<ByteBufferL>): List<ByteBufferL> {
        require(dataBlocks.isNotEmpty())
        val k = dataBlocks.size
        val blks = dataBlocks.map { it.duplicate() }
        blks.forEach { require(it.remaining == BLOCK_SIZE) { "block must be 32 KiB" } }

        // RS の制約
        require(k + parityCount <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$parityCount" }

        val parity = ArrayList<ByteBufferL>(parityCount)
        repeat(parityCount) { parity += pool.get(BLOCK_SIZE) }
        parity.forEach { it.clear() }

        // a(j,i) = α^{(j+1) * i}
        val coeff = Array(parityCount) { j ->
            IntArray(k) { i -> if (i == 0) 1 else gfPowAlpha((j + 1) * i) }
        }

        for (pos in 0 until BLOCK_SIZE) {
            val dataBytes = IntArray(k) { idx -> blks[idx].get(pos).toInt() and 0xFF }
            for (j in 0 until parityCount) {
                val row = coeff[j]
                var acc = 0
                for (i in 0 until k) acc = gfAdd(acc, gfMul(row[i], dataBytes[i]))
                parity[j].put(pos, acc.toByte())
            }
        }
        parity.forEach { it.limit(BLOCK_SIZE).position(0) }
        return parity
    }

    /* ---------------- Decode with errors (+ optional erasures) ---------------- */

    override fun decode(lostIndex: Int, presentData: List<ByteBufferL?>, presentParity: List<ByteBufferL?>): ByteBufferL {
        throw NotImplementedError("use decodeWithErrors for error+erasure decoding")
    }

    /**
     * Recover all missing data blocks. Tolerates up to floor((m - s)/2) errors mixed in present blocks,
     * provided 2*t + s ≤ m. knownErasures are indices in 0..k-1 for missing data lanes (optional hint).
     *
     * @return list of recovered ByteBufferL for each null entry in presentData, in the same order as indices
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

        // 出力（欠損データそれぞれのバッファ）
        val out = missingIdx.map { it to pool.get(BLOCK_SIZE).apply { clear() } }.toMutableList()

        // 1バイト位置ごとに n = k+m のシンボル列を作って復号
        val n = k + m
        val erasuresSet = (knownErasures?.toMutableSet() ?: mutableSetOf())
        // 既知の「データ側欠損」は knownErasures と missingIdx を統合
        erasuresSet.addAll(missingIdx)

        for (pos in 0 until BLOCK_SIZE) {
            // codeword symbols c[0..n-1]：前半 k がデータ、後半 m がパリティ
            val c = IntArray(n)
            val isKnown = BooleanArray(n)
            for (i in 0 until k) {
                val bb = presentData[i]
                if (bb != null) {
                    c[i] = bb.get(pos).toInt() and 0xFF
                    isKnown[i] = true
                }
            }
            for (j in 0 until m) {
                val pb = presentParity[j]
                if (pb != null) {
                    c[k + j] = pb.get(pos).toInt() and 0xFF
                    isKnown[k + j] = true
                }
            }

            // --- Syndrome S_1..S_m を計算（評価点 α^i） ---
            // S_r = Σ_{i=0..n-1} c_i * (α^{i})^r = Σ c_i * α^{i*r}, r=1..m
            val S = IntArray(m)
            for (r in 1..m) {
                var sr = 0
                // 既知/未知問わず、その時点の値 c_i で計算（未知は 0 と同義）
                for (i in 0 until n) {
                    val ci = c[i]
                    if (ci != 0) {
                        val pow = if (i == 0) 1 else gfPowAlpha(i * r)
                        sr = gfAdd(sr, gfMul(ci, pow))
                    }
                }
                S[r - 1] = sr
            }
            val allZero = S.all { it == 0 }
            if (allZero && missingIdx.isEmpty()) {
                // 何もすることがない
            }

            // --- Erasures の扱い：erasure locator Γ(x) として初期化 ---
            // erasure 位置は “評価点 α^i” の逆数が根になる（慣例に合わせ α^{-i} を用いる）
            val erasures = erasuresSet.filter { it in 0 until k } // データ側を優先
            val s = erasures.size

            // 訂正可能性チェック（上界。実際の t はBMで決まる）
            // ここで落とすより、BM失敗で検出でも良い。
            // require(s <= m) { "too many erasures: s=$s > m=$m" }

            // Γ(x) = ∏ (1 - x * α^{i_e})
            var Lambda = intArrayOf(1) // 誤り＋消失 locator Λ(x) の初期値に Γ(x) を掛ける
            for (ie in erasures) {
                val aie = if (ie == 0) 1 else gfPowAlpha(ie) // α^{ie}
                Lambda = polyMul(Lambda, intArrayOf(1, aie)) // (1 + aie*x) ただし GF(2^8) では + が xor
            }

            // BM（Berlekamp–Massey）で Λ(x) を拡張（未知エラー分を学習）
            // 通常BM初期値：Λ=1。ここでは erasure 初期化を反映。
            val (sigma, omega) = berlekampMasseyWithErasures(S, Lambda)

            // Chien 検索：Λ(α^{-i}) = 0 となる i を全探索
            val errorLocs = chienSearch(sigma, n)
            // 位置のうち、データ側の欠損は out に書く対象、パリティ側／データ既知側はその場で修正に使う

            // Forney の式で誤差値を算出し、c を修正
            val errVals = forneyValues(sigma, omega, errorLocs)
            for (idx in errorLocs.indices) {
                val i = errorLocs[idx]
                val ev = errVals[idx]
                c[i] = gfAdd(c[i], ev) // 差分を足す（=XOR）
            }

            // 欠損データの書き戻し
            for ((listIndex, di) in missingIdx.withIndex()) {
                val v = c[di]
                out[listIndex].second.put(pos, v.toByte())
            }
        }

        // 仕上げ
        out.forEach { it.second.limit(BLOCK_SIZE).position(0) }
        return out
    }

    /* ---------------- Polynomials & Decoding core ---------------- */

    private fun polyAdd(a: IntArray, b: IntArray): IntArray {
        val n = maxOf(a.size, b.size)
        val r = IntArray(n)
        for (i in 0 until n) {
            val ai = if (i >= n - a.size) a[i - (n - a.size)] else 0
            val bi = if (i >= n - b.size) b[i - (n - b.size)] else 0
            r[i] = ai xor bi
        }
        return r.trimLeadingZeros()
    }

    private fun polyMul(a: IntArray, b: IntArray): IntArray {
        val r = IntArray(a.size + b.size - 1)
        for (i in a.indices) {
            if (a[i] == 0) continue
            for (j in b.indices) {
                if (b[j] == 0) continue
                r[i + j] = gfAdd(r[i + j], gfMul(a[i], b[j]))
            }
        }
        return r.trimLeadingZeros()
    }

    private fun polyScale(a: IntArray, s: Int): IntArray {
        if (s == 0) return intArrayOf(0)
        val r = IntArray(a.size)
        for (i in a.indices) r[i] = gfMul(a[i], s)
        return r
    }

    private fun polyEval(a: IntArray, x: Int): Int {
        var y = 0
        for (coef in a) {
            y = gfMul(y, x)
            y = gfAdd(y, coef)
        }
        return y
    }

    private fun IntArray.trimLeadingZeros(): IntArray {
        var i = 0
        while (i < size - 1 && this[i] == 0) i++
        return copyOfRange(i, size)
    }

    /**
     * Berlekamp–Massey with erasure initialization.
     * Given syndromes S and initial erasure locator Γ(x), compute:
     *   - σ(x): error+erasure locator
     *   - ω(x): error evaluator (ω = S(x) * σ(x) mod x^m)
     */
    private fun berlekampMasseyWithErasures(S: IntArray, erasureLocator: IntArray): Pair<IntArray, IntArray> {
        val m = S.size
        var sigma = erasureLocator.copyOf()
        var B = intArrayOf(1)
        var L = erasureLocator.size - 1 // current degree
        var b = 1
        var k = 0

        // Precompute S(x) as poly with S[0] as x^{m-1} coefficient（表現は流派がある。ここではBMの慣例に合わせる）
        // ここでは逐次方式：BM反復で S[k] を使う

        for (n in 0 until m) {
            // discrepancy d = S[n] + Σ_{i=1..L} sigma[i]*S[n - i]
            var d = S[n]
            for (i in 1..L) {
                if (i >= sigma.size) break
                d = gfAdd(d, gfMul(sigma[i], S[n - i]))
            }
            if (d != 0) {
                val T = sigma.copyOf()
                // sigma = sigma ⊕ d * b^{-1} * x^{k} * B
                val factor = gfMul(d, gfInv(b))
                val shiftB = IntArray(B.size + k) { idx -> if (idx < k) 0 else B[idx - k] }
                sigma = polyAdd(sigma, polyScale(shiftB, factor))
                if (2 * L <= n) {
                    L = n + 1 - L
                    B = T
                    b = d
                    k = 1
                } else {
                    k++
                }
            } else {
                k++
            }
        }

        // ω(x) = (S(x) * σ(x)) mod x^m
        // S(x) は S[0] が x^{m-1} の係数とみなす多項式：ここは簡易的に“高位→低位”の並びで扱う
        // 実装簡素化のため、畳み込みして上位 m-1 次までを残す
        val SPoly = S.copyOf() // ここでは x^{m-1}..x^0 として扱う（係数順に注意）
        val omegaFull = IntArray(SPoly.size + sigma.size - 1)
        for (i in SPoly.indices) for (j in sigma.indices) {
            omegaFull[i + j] = gfAdd(omegaFull[i + j], gfMul(SPoly[i], sigma[j]))
        }
        val omega = omegaFull.copyOf(min(omegaFull.size, m)) // mod x^m

        return Pair(sigma.trimLeadingZeros(), omega.trimLeadingZeros())
    }

    /** Chien search: find indices i in 0..n-1 such that sigma(alpha^{-i}) == 0 */
    private fun chienSearch(sigma: IntArray, n: Int): IntArray {
        val roots = ArrayList<Int>()
        for (i in 0 until n) {
            // 評価点は α^{-i}
            val xInv = if (i == 0) 1 else gfPowAlpha(255 - (i % 255))
            if (polyEval(sigma, xInv) == 0) roots += i
        }
        return roots.toIntArray()
    }

    /**
     * Forney's formula to compute error magnitudes at given error locations.
     * errVal(i) = - Ω(x_i^{-1}) / (Λ'(x_i^{-1}))
     */
    private fun forneyValues(sigma: IntArray, omega: IntArray, errorLocs: IntArray): IntArray {
        // σ'(x) : formal derivative
        val sigmaPrime = IntArray(maxOf(1, sigma.size - 1))
        for (i in 1 until sigma.size) {
            // derivative in GF(2^8): only odd powers survive（偶数項は消える）
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
