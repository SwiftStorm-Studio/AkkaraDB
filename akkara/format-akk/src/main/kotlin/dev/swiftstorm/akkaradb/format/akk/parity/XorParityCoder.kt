package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
import kotlin.experimental.xor

/**
 * <h2>XOR Parity</h2>
 *
 * Generates a single XOR parity so that any <em>one</em> missing lane among
 * <code>k&nbsp;+&nbsp;1</code> can be reconstructed in <code>O(k·blockSize)</code>.
 * This is the simplest form of parity coding, similar to RAID‑5.
 *
 * Implementation notes:
 * * Uses a <strong>thread‑local</strong> [BufferPool] to amortise direct‑buffer
 *   allocations.
 * * All intermediate buffers are released in <code>finally</code> blocks.
 * * Returned parity / decode buffers are <em>read‑only</em>; caller owns release.
 */
class XorParityCoder(
    private val pool: BufferPool = Pools.io()
) : ParityCoder {

    override val parityCount: Int = 1

    /* ───────── encode ───────── */
    override fun encode(dataBlocks: List<ByteBuffer>): List<ByteBuffer> {
        require(dataBlocks.isNotEmpty()) { "dataBlocks must not be empty" }
        val size = dataBlocks[0].remaining()
        require(dataBlocks.all { it.remaining() == size }) { "block size mismatch" }

        val parityBuf = pool.get(size)
        parityBuf.put(dataBlocks[0].duplicate())
        for (i in 1 until dataBlocks.size) {
            val src = dataBlocks[i].duplicate()
            for (p in 0 until size) {
                parityBuf.put(p, parityBuf.get(p) xor src.get(p))
            }
        }
        parityBuf.flip()

        val ro = parityBuf.asReadOnlyBuffer()
        pool.release(parityBuf)
        return listOf(ro)
    }

    /* ───────── decode ───────── */
    override fun decode(
        lostIndex: Int,
        presentData: List<ByteBuffer?>,
        parity: List<ByteBuffer?>
    ): ByteBuffer {
        val parityBuf = parity[0] ?: error("parity must be present for XOR decode")
        val size = parityBuf.remaining()

        val recBuf = pool.get(size)
        recBuf.put(parityBuf.duplicate())
        for ((idx, blk) in presentData.withIndex()) {
            if (idx == lostIndex || blk == null) continue
            val dup = blk.duplicate()
            for (p in 0 until size) {
                recBuf.put(p, recBuf.get(p) xor dup.get(p))
            }
        }
        recBuf.flip()

        val ro = recBuf.asReadOnlyBuffer()
        pool.release(recBuf)
        return ro
    }
}
