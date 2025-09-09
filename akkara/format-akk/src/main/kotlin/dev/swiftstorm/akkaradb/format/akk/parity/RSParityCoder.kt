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
 * where a(j,i) = α^{(j+1)·i} and α is the primitive element of GF(256).
 *
 * Decoding handles up to m erasures simultaneously. Given a list of lane blocks
 * with some elements null (erasures) and a list of m parity blocks, it solves the
 * linear system to recover the requested missing lane. Other missing lanes are
 * also reconstructed internally, but only the requested one is returned.
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
        log[0] = 0 // unused; guard
    }

    private inline fun gfAdd(a: Int, b: Int): Int = a xor b
    private inline fun gfMul(a: Int, b: Int): Int {
        if (a == 0 || b == 0) return 0
        val r = exp[log[a] + log[b]]
        return r
    }

    private inline fun gfInv(a: Int): Int {
        require(a != 0) { "gfInv(0)" }
        return exp[255 - log[a]]
    }

    /* ---------------- Encoding ---------------- */

    override fun encode(dataBlocks: List<ByteBufferL>): List<ByteBufferL> {
        require(dataBlocks.isNotEmpty())
        val k = dataBlocks.size
        require(k + parityCount <= 255) { "RS(8) requires n=k+m ≤ 255; got k=$k, m=$parityCount" }

        val blks = dataBlocks.map { it.duplicate() }
        blks.forEach { require(it.remaining == BLOCK_SIZE) { "block must be 32 KiB" } }

        val parity = ArrayList<ByteBufferL>(parityCount)
        repeat(parityCount) { parity += pool.get(BLOCK_SIZE) }
        parity.forEach { it.clear() }

        // a(j,i) = α^{(j+1) * i}
        val coeff = Array(parityCount) { j -> IntArray(k) { i -> coeffA(j, i) } }

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
            "not enough parity rows: have=${availParityRows.size}, need=$e; missing=$missing"
        }
        val J = availParityRows.take(e)

        val M = Array(e) { r ->
            IntArray(e) { t ->
                val i = missing[t]
                coeffA(J[r], i)
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

            for (t in 0 until e) {
                var v = 0
                for (c in 0 until e) v = gfAdd(v, gfMul(inv[t][c], S[c]))
                recovered[t].put(pos, v.toByte())
            }
        }

        for (t in 0 until e) recovered[t].limit(BLOCK_SIZE).position(0)
        val idxInMissing = missing.indexOf(lostIndex)
        return recovered[idxInMissing]
    }

    /* ---------------- GF helpers ---------------- */

    private fun invertMatrix(src: Array<IntArray>): Array<IntArray> {
        val n = src.size
        val a = Array(n) { IntArray(n) }
        val inv = Array(n) { IntArray(n) }
        for (i in 0 until n) {
            for (j in 0 until n) a[i][j] = src[i][j]
            inv[i][i] = 1
        }
        // Gauss–Jordan
        var row = 0
        for (col in 0 until n) {
            var pivot = -1
            for (r in row until n) if (a[r][col] != 0) {
                pivot = r; break
            }
            require(pivot != -1) { "singular matrix in RS decode" }
            if (pivot != row) {
                val tmp = a[pivot]; a[pivot] = a[row]; a[row] = tmp
                val tmp2 = inv[pivot]; inv[pivot] = inv[row]; inv[row] = tmp2
            }
            val invPivot = gfInv(a[row][col])
            for (c in 0 until n) {
                a[row][c] = gfMul(a[row][c], invPivot); inv[row][c] = gfMul(inv[row][c], invPivot)
            }
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
