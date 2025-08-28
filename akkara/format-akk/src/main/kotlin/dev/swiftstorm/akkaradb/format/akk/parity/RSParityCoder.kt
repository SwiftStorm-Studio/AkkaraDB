@file:Suppress("NOTHING_TO_INLINE")

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
        val blks = dataBlocks.map { it.duplicate() }
        blks.forEach { require(it.remaining == BLOCK_SIZE) { "block must be 32 KiB" } }

        val parity = ArrayList<ByteBufferL>(parityCount)
        // allocate parity buffers from pool so writer can release them
        repeat(parityCount) { parity += pool.get(BLOCK_SIZE) }
        parity.forEach { it.clear() }

        val coeff = Array(parityCount) { j ->
            IntArray(k) { i ->
                // a(j,i) = α^{(j+1)*i}
                if (i == 0) 1 else exp[((j + 1) * (log[exp[i]])) % 255] // exp[i] = α^i
            }
        }
        // Alternative simpler Vandermonde: a(j,i) = (i+1)^j
        // val coeff = Array(parityCount){ j -> IntArray(k){ i -> gfPow(i+1,j) } }

        // Compute parity byte-wise
        for (pos in 0 until BLOCK_SIZE) {
            val dataBytes = IntArray(k)
            for (i in 0 until k) dataBytes[i] = blks[i].get(pos).toInt() and 0xFF
            for (j in 0 until parityCount) {
                var acc = 0
                val row = coeff[j]
                for (i in 0 until k) acc = gfAdd(acc, gfMul(row[i], dataBytes[i]))
                parity[j].put(pos, acc.toByte())
            }
        }
        parity.forEach { it.position(BLOCK_SIZE); it.flip() }
        return parity
    }

    /* ---------------- Decoding (up to m erasures) ---------------- */

    override fun decode(lostIndex: Int, presentData: List<ByteBufferL?>, presentParity: List<ByteBufferL?>): ByteBufferL {
        val k = presentData.size
        require(presentParity.size == parityCount) { "expected $parityCount parity blocks, got ${presentParity.size}" }
        val missing = presentData.withIndex().filter { it.value == null }.map { it.index }
        require(missing.isNotEmpty()) { "nothing to decode" }
        require(missing.size <= parityCount) { "too many erasures: ${missing.size} > m=$parityCount" }
        require(missing.contains(lostIndex))
        presentParity.forEach { p -> require(p == null || p.remaining == BLOCK_SIZE) }

        // Build coefficient matrix A (m x e) and RHS syndromes S (m vectors)
        val e = missing.size
        val A = Array(parityCount) { IntArray(e) }
        for (j in 0 until parityCount) {
            for (t in 0 until e) {
                val i = missing[t]
                A[j][t] = if (i == 0) 1 else exp[((j + 1) * (log[exp[i]])) % 255]
            }
        }
        // Precompute contributions of known data for each parity row
        val knownIdx = (0 until k).filter { it !in missing.toSet() }

        // Invert A_e (take top e rows for a square system). Use Gaussian elimination in GF(256).
        val M = Array(e) { IntArray(e) }
        for (r in 0 until e) for (c in 0 until e) M[r][c] = A[r][c]
        val inv = invertMatrix(M)

        // Prepare output buffers for all missing blocks (compute all, return requested)
        val recovered = Array(e) { pool.get(BLOCK_SIZE) }
        for (t in 0 until e) recovered[t].clear()

        for (pos in 0 until BLOCK_SIZE) {
            // Syndromes S_r = P_r - Σ_i a(r,i)*D_i
            val S = IntArray(e)
            for (r in 0 until e) {
                var s = 0
                val p = presentParity[r] ?: error("missing required parity block r=$r for e=$e")
                var accumKnown = 0
                for (i in knownIdx) {
                    val a = if (i == 0) 1 else exp[((r + 1) * (log[exp[i]])) % 255]
                    val d = presentData[i]!!.get(pos).toInt() and 0xFF
                    accumKnown = gfAdd(accumKnown, gfMul(a, d))
                }
                val pr = p.get(pos).toInt() and 0xFF
                s = gfAdd(pr, accumKnown) // since P = A·D, moving known terms to RHS
                S[r] = s
            }
            // Solve X = inv * S
            val X = IntArray(e)
            for (r in 0 until e) {
                var v = 0
                for (c in 0 until e) v = gfAdd(v, gfMul(inv[r][c], S[c]))
                X[r] = v
            }
            for (t in 0 until e) recovered[t].put(pos, X[t].toByte())
        }
        for (t in 0 until e) recovered[t].position(BLOCK_SIZE).flip()
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
}
