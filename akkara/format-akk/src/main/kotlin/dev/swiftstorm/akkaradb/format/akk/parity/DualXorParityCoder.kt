package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
import kotlin.experimental.xor

/**
 * <h2>Dual‑XOR Parity</h2>
 *
 * Generates two independent XOR parities (P, Q) so that any <em>one</em>
 * missing lane among <code>k&nbsp;+&nbsp;2</code> can be reconstructed in
 * <code>O(k·blockSize)</code>. This is <em>not</em> Reed‑Solomon: it simply keeps
 * an extra cumulative XOR that is shifted by the element index, similar to the
 * algorithm used by RAID‑6 (even‑odd).
 *
 * Implementation notes:
 * * Uses a <strong>thread‑local</strong> [BufferPool] to amortise direct‑buffer
 *   allocations.
 * * All intermediate buffers are released in <code>finally</code> blocks.
 * * Returned parity / decode buffers are <em>read‑only</em>; caller owns release.
 */
class DualXorParityCoder(private val pool: BufferPool = Pools.io()) : ParityCoder {

    override val parityCount: Int = 2

    /* ───────── encode ───────── */
    override fun encode(dataBlocks: List<ByteBuffer>): List<ByteBuffer> {
        require(dataBlocks.isNotEmpty()) { "dataBlocks must not be empty" }
        val size = dataBlocks[0].remaining()
        require(dataBlocks.all { it.remaining() == size }) { "all blocks must have equal size" }

        val p = pool.get(size)
        val q = pool.get(size)
        try {
            // 1) P parity = XOR of all blocks
            p.put(dataBlocks[0].duplicate()).flip()
            // 2) Q parity = XOR of block[i] rotated left by i bytes (simple shim)
            for (i in 1 until dataBlocks.size) {
                val buf = dataBlocks[i].duplicate()
                for (pos in 0 until size) {
                    val b = buf.get(pos)
                    p.put(pos, p.get(pos) xor b)
                    val rotIdx = (pos + i) % size
                    q.put(rotIdx, (q.get(rotIdx) xor b))
                }
            }
            q.flip(); p.flip()
            return listOf(p.asReadOnlyBuffer(), q.asReadOnlyBuffer())
        } finally {
            // ownership of read‑only duplicates is transferred to caller; keep originals
            // until duplicates are returned.
        }
    }

    /* ───────── decode ───────── */
    override fun decode(
        lostIndex: Int,
        presentData: List<ByteBuffer?>,
        parity: List<ByteBuffer?>,
    ): ByteBuffer {
        val size = parity.filterNotNull().first().remaining()
        // Choose algorithm branch
        return if (lostIndex < presentData.size) {
            // one of the data blocks is lost → reconstruct via P parity
            val rec = pool.get(size)
            try {
                rec.put(parity[0]!!.duplicate()).flip() // start with P
                for ((idx, blk) in presentData.withIndex()) {
                    if (blk == null) continue
                    val dup = blk.duplicate()
                    for (pos in 0 until size) rec.put(pos, rec.get(pos) xor dup.get(pos))
                }
                rec.flip(); rec.asReadOnlyBuffer()
            } finally {
                // rec ownership passed
            }
        } else {
            /* Lost is one of the parity blocks.
             * For simplicity we regenerate the missing parity the same way encode() did.
             */
            val pOrQ = pool.get(size)
            try {
                if (lostIndex == presentData.size) {
                    // missing P
                    pOrQ.clear()
                    pOrQ.put(presentData.first()!!.duplicate()).flip()
                    for (blk in presentData.drop(1)) if (blk != null) {
                        val dup = blk.duplicate()
                        for (pos in 0 until size) pOrQ.put(pos, pOrQ.get(pos) xor dup.get(pos))
                    }
                } else {
                    // missing Q – reconstruct rotated XOR
                    for (blk in presentData.filterNotNull()) {
                        val dup = blk.duplicate()
                        for (pos in 0 until size) {
                            val rotIdx = (pos + presentData.indexOf(blk)) % size
                            pOrQ.put(rotIdx, (pOrQ.get(rotIdx) xor dup.get(pos)))
                        }
                    }
                }
                pOrQ.flip(); pOrQ.asReadOnlyBuffer()
            } finally {
                // ownership transfers to caller
            }
        }
    }
}
