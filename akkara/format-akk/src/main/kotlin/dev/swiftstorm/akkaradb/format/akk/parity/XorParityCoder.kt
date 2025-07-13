package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
import kotlin.experimental.xor

/**
 * <h2>Single‑XOR Parity Coder</h2>
 *
 * Keeps one XOR block (P) so that any <em>single</em> missing data lane among
 * <code>k&nbsp;+&nbsp;1</code> can be reconstructed. This is the classic RAID‑5
 * algorithm – fast and branch‑free.
 *
 * <b>Contract</b>
 * * Returned buffers are <em>read‑only</em>; caller must release them when done.
 * * All temporary direct buffers are leased from the provided [BufferPool] and
 *   released in <code>finally</code> blocks, eliminating leaks.
 */
class XorParityCoder(private val pool: BufferPool = Pools.io()) : ParityCoder {

    override val parityCount: Int = 1

    /* ───────── encode ───────── */
    override fun encode(dataBlocks: List<ByteBuffer>): List<ByteBuffer> {
        require(dataBlocks.isNotEmpty()) { "dataBlocks must not be empty" }
        val size = dataBlocks[0].remaining()
        require(dataBlocks.all { it.remaining() == size }) { "block size mismatch" }

        val parity = pool.get(size)
        try {
            parity.put(dataBlocks[0].duplicate()).flip()        // seed with first block
            for (i in 1 until dataBlocks.size) {
                val src = dataBlocks[i].duplicate()
                for (p in 0 until size) parity.put(p, parity.get(p) xor src.get(p))
            }
            parity.flip()
            return listOf(parity.asReadOnlyBuffer())
        } finally {
            // keep original parity buffer until RO duplicate is returned; do not release here.
        }
    }

    /* ───────── decode ───────── */
    override fun decode(
        lostIndex: Int,
        presentData: List<ByteBuffer?>,
        parity: List<ByteBuffer?>
    ): ByteBuffer {
        val size = parity[0]?.remaining() ?: error("parity must be present for XOR decode")

        // missing data block → use parity XOR of remaining
        val rec = pool.get(size)
        try {
            rec.put(parity[0]!!.duplicate()).flip()
            for ((idx, blk) in presentData.withIndex()) {
                if (idx == lostIndex || blk == null) continue
                val dup = blk.duplicate()
                for (p in 0 until size) rec.put(p, rec.get(p) xor dup.get(p))
            }
            rec.flip()
            return rec.asReadOnlyBuffer()
        } finally {
            // ownership of read‑only duplicate is transferred to caller.
        }
    }
}
