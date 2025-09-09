@file:Suppress("NOTHING_TO_INLINE", "DuplicatedCode", "LocalVariableName")

package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.ParityCoder

/**
 * Reed–Solomon parity coder over GF(2^8) with a Vandermonde generator matrix.
 *
 * Supports any parityCount m ≥ 1. Works with an arbitrary number of data lanes k ≥ 1.
 *
 * Parity j (0-based) is computed as: P_j = Σ_i a(j,i) · D_i
 * where a(j,i) = α^{(j+1)·i} and α is a primitive element of GF(256) with poly 0x11D.
 */
class RSParityCoder(
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
            if (x and 0x100 != 0) x = x xor 0x11D
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

    /* ---------------- 係数キャッシュ（改善 #3） ---------------- */

    @Volatile
    private var cachedK: Int = -1

    /** coeff[j][i] = a(j,i) = α^{(j+1)·i} */
    @Volatile
    private var coeff: Array<IntArray>? = null

    private fun ensureCoeff(k: Int) {
        val curr = coeff
        if (curr != null && cachedK == k) return
        val newCoeff = Array(parityCount) { j ->
            IntArray(k) { i -> coeffA(j, i) }
        }
        coeff = newCoeff
        cachedK = k
    }

    /* ---------------- Encoding (SAXPY; 改善 #1) ---------------- */

    override fun encode(dataBlocks: List<ByteBufferL>): List<ByteBufferL> {
        require(dataBlocks.isNotEmpty())
        val k = dataBlocks.size
        require(k + parityCount <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$parityCount" }

        val blks = dataBlocks.map { it.duplicate() }
        blks.forEach { require(it.remaining == BLOCK_SIZE) { "block must be 32 KiB" } }

        val parity = ArrayList<ByteBufferL>(parityCount)
        repeat(parityCount) { parity += pool.get(BLOCK_SIZE) }
        parity.forEach { it.clear() }

        ensureCoeff(k)
        val c = coeff!! // non-null after ensure

        for (i in 0 until k) {
            val src = blks[i].duplicate()
            for (j in 0 until parityCount) {
                saxpy(parity[j], src, c[j][i])
            }
        }

        parity.forEach { it.limit(BLOCK_SIZE).position(0) }
        return parity
    }

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

    /* ---------------- Decoding (up to m erasures) ---------------- */

    override fun decode(
        lostIndex: Int,
        presentData: List<ByteBufferL?>,
        presentParity: List<ByteBufferL?>
    ): ByteBufferL {
        val k = presentData.size
        val m = parityCount
        require(presentParity.size == m) { "expected $m parity blocks, got ${presentParity.size}" }
        require(k + m <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$m" }

        val missing = presentData.withIndex().filter { it.value == null }.map { it.index }
        require(missing.isNotEmpty()) { "nothing to decode" }
        require(missing.size <= m) { "too many erasures: ${missing.size} > m=$m" }
        require(lostIndex in missing) { "lostIndex=$lostIndex not in missing=$missing" }

        presentData.forEach { d -> require(d == null || d.remaining == BLOCK_SIZE) { "bad data size" } }
        presentParity.forEach { p -> require(p == null || p.remaining == BLOCK_SIZE) { "bad parity size" } }

        val e = missing.size
        val missingSet = missing.toHashSet()
        val knownIdx = (0 until k).filter { it !in missingSet }

        val availParityRows = presentParity.withIndex().filter { it.value != null }.map { it.index }
        require(availParityRows.size >= e) {
            "need at least e=$e parity rows, but only ${availParityRows.size} are available"
        }
        val J = IntArray(e) { availParityRows[it] }

        // M[r][c] = a(J[r], knownIdx[c])
        val M = Array(e) { IntArray(e) }
        for (r in 0 until e) {
            val jrow = J[r]
            for (c in 0 until e) {
                val i = knownIdx[c]
                M[r][c] = coeffA(jrow, i)
            }
        }
        val inv = invertMatrix(M)

        val recovered = Array(e) { pool.get(BLOCK_SIZE).apply { clear() } }

        for (pos in 0 until BLOCK_SIZE) {
            // S_r = P_{J[r]} ⊕ Σ_{i∈known} a(J[r], i)·D_i
            val S = IntArray(e)
            for (r in 0 until e) {
                val jrow = J[r]
                val p = presentParity[jrow]!!
                var accumKnown = 0
                for (i in knownIdx) {
                    val a = coeffA(jrow, i)
                    val d = presentData[i]!!.get(pos).toInt() and 0xFF
                    accumKnown = gfAdd(accumKnown, gfMul(a, d))
                }
                val pr = p.get(pos).toInt() and 0xFF
                S[r] = gfAdd(pr, accumKnown)
            }

            // D_missing = inv(M) · S
            for (t in 0 until e) {
                var v = 0
                for (c in 0 until e) v = gfAdd(v, gfMul(inv[t][c], S[c]))
                recovered[t].put(pos, v.toByte())
            }
        }

        val which = missing.indexOf(lostIndex)
        val out = recovered[which]
        out.limit(BLOCK_SIZE).position(0)
        return out
    }

    /* ---------------- Small GF helpers ---------------- */

    // Gauss–Jordan elimination for small dense matrix over GF(256)
    private fun invertMatrix(a0: Array<IntArray>): Array<IntArray> {
        val n = a0.size
        val a = Array(n) { r -> a0[r].clone() }
        val inv = Array(n) { r -> IntArray(n).apply { this[r] = 1 } }

        var row = 0
        for (col in 0 until n) {
            // pivot search
            var pivot = row
            while (pivot < n && a[pivot][col] == 0) pivot++
            if (pivot == n) continue // singular;

            // swap
            if (pivot != row) {
                val tmp = a[pivot]; a[pivot] = a[row]; a[row] = tmp
                val tmp2 = inv[pivot]; inv[pivot] = inv[row]; inv[row] = tmp2
            }

            // normalize
            val invPivot = gfInv(a[row][col])
            for (c in 0 until n) {
                a[row][c] = gfMul(a[row][c], invPivot)
                inv[row][c] = gfMul(inv[row][c], invPivot)
            }

            // eliminate others
            for (r in 0 until n) if (r != row && a[r][col] != 0) {
                val factor = a[r][col]
                for (c in 0 until n) {
                    a[r][c] = gfAdd(a[r][c], gfMul(factor, a[row][c]))
                    inv[r][c] = gfAdd(inv[r][c], gfMul(factor, inv[row][c]))
                }
            }
            row++
        }
        return inv
    }

    private inline fun coeffA(row: Int, i: Int): Int {
        return if (i == 0) 1 else exp[((row + 1) * i) % 255]
    }
}
