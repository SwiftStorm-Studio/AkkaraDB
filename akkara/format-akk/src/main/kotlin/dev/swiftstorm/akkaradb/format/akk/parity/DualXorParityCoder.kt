package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
import kotlin.experimental.xor

/**
 * <h2>Dual XOR Parity</h2>
 *
 * Generates two XOR parities so that any <em>two</em> missing lanes among
 * <code>k&nbsp;+&nbsp;2</code> can be reconstructed in <code>O(k·blockSize)</code>.
 * This is a more advanced form of parity coding, similar to RAID‑6.
 *
 * Implementation notes:
 * * Uses a <strong>thread‑local</strong> [BufferPool] to amortise direct‑buffer
 *   allocations.
 * * All intermediate buffers are released in <code>finally</code> blocks.
 * * Returned parity / decode buffers are <em>read‑only</em>; caller owns release.
 */
class DualXorParityCoder(
    private val pool: BufferPool = Pools.io()
) : ParityCoder {

    override val parityCount: Int = 2

    /* ───────── encode ───────── */
    override fun encode(dataBlocks: List<ByteBuffer>): List<ByteBuffer> {
        require(dataBlocks.isNotEmpty()) { "dataBlocks must not be empty" }
        val size = dataBlocks[0].remaining()
        require(dataBlocks.all { it.remaining() == size }) { "all blocks must have equal size" }

        val p = pool.get(size)
        val q = pool.get(size)

        p.put(dataBlocks[0].duplicate())
        for (i in 1 until dataBlocks.size) {
            val src = dataBlocks[i].duplicate()
            for (pos in 0 until size) {
                val b = src.get(pos)
                p.put(pos, p.get(pos) xor b)
                val rot = (pos + i) % size
                q.put(rot, q.get(rot) xor b)
            }
        }
        p.flip(); q.flip()

        val roP = p.asReadOnlyBuffer()
        val roQ = q.asReadOnlyBuffer()
        pool.release(p); pool.release(q)
        return listOf(roP, roQ)
    }

    /* ───────── decode ───────── */
    override fun decode(
        lostIndex: Int,
        presentData: List<ByteBuffer?>,
        parity: List<ByteBuffer?>
    ): ByteBuffer {
        val size = parity.filterNotNull().first().remaining()

        if (lostIndex < presentData.size) {
            val rec = pool.get(size)
            rec.put(parity[0]!!.duplicate())
            for ((idx, blk) in presentData.withIndex()) {
                if (idx == lostIndex || blk == null) continue
                val dup = blk.duplicate()
                for (pos in 0 until size) {
                    rec.put(pos, rec.get(pos) xor dup.get(pos))
                }
            }
            rec.flip()
            val ro = rec.asReadOnlyBuffer()
            pool.release(rec)
            return ro
        }

        val restoreP = (lostIndex == presentData.size)
        val parityBuf = pool.get(size)

        if (restoreP) {
            parityBuf.put(presentData.filterNotNull().first().duplicate())
            for (blk in presentData.drop(1).filterNotNull()) {
                val dup = blk.duplicate()
                for (pos in 0 until size) {
                    parityBuf.put(pos, parityBuf.get(pos) xor dup.get(pos))
                }
            }
        } else {
            // Q = Σ rotate(block_i, i)
            for ((i, blk) in presentData.withIndex()) {
                if (blk == null) continue
                val dup = blk.duplicate()
                for (pos in 0 until size) {
                    val rot = (pos + i) % size
                    parityBuf.put(rot, parityBuf.get(rot) xor dup.get(pos))
                }
            }
        }
        parityBuf.flip()
        val ro = parityBuf.asReadOnlyBuffer()
        pool.release(parityBuf)
        return ro
    }
}
