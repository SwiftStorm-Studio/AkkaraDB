package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
import kotlin.experimental.xor

class DualXorParityCoder(
    private val pool: BufferPool = Pools.io()
) : ParityCoder {

    override val parityCount: Int = 2

    /* ───────── encode ───────── */
    override fun encode(dataBlocks: List<ByteBuffer>): List<ByteBuffer> {
        require(dataBlocks.isNotEmpty()) { "dataBlocks must not be empty" }
        val size = dataBlocks[0].remaining()
        require(dataBlocks.all { it.remaining() == size }) { "all blocks must have equal size" }

        // P = Σ block_i,   Q = Σ rotate(block_i, i)
        val p = pool.get(size)
        val q = pool.get(size)
        // buffers come cleared; ensure correct limit
        p.limit(size); q.limit(size)

        for ((i, data) in dataBlocks.withIndex()) {
            val src = data.duplicate()
            for (pos in 0 until size) {
                val b = src.get(pos)
                p.put(pos, p.get(pos) xor b)

                val rot = (pos + i) % size
                q.put(rot, q.get(rot) xor b)
            }
        }
        p.flip(); q.flip()

        // ❗️ Caller must release returned buffers.
        return listOf(p.asReadOnlyBuffer(), q.asReadOnlyBuffer())
    }

    /* ───────── decode ───────── */
    override fun decode(
        lostIndex: Int,
        presentData: List<ByteBuffer?>,
        parity: List<ByteBuffer?>
    ): ByteBuffer {
        require(parityCount == parity.size) { "parity size mismatch" }
        val size = parity.filterNotNull().first().remaining()

        // ─── Case 1: Lost block is DATA ──────────────────────────────
        if (lostIndex < presentData.size) {
            val rec = pool.get(size)
            rec.limit(size)
            // Start with P parity block
            val p = parity[0]!!.duplicate()
            rec.put(p)

            // XOR all present data blocks (except the lost one)
            for ((idx, blk) in presentData.withIndex()) {
                if (idx == lostIndex || blk == null) continue
                val dup = blk.duplicate()
                for (pos in 0 until size) {
                    rec.put(pos, rec.get(pos) xor dup.get(pos))
                }
            }
            rec.flip()
            return rec.asReadOnlyBuffer()  // caller releases
        }

        // ─── Case 2: Lost block is P or Q parity ────────────────────
        val restoreP = (lostIndex == presentData.size) // true if P is lost, else Q
        val out = pool.get(size)
        out.limit(size)

        if (restoreP) {
            // P  = Σ block_i
            // Initialize with first present data block
            val first = presentData.first { it != null }!!.duplicate()
            out.put(first)
            for (blk in presentData.dropWhile { it == null }.drop(1)) {
                if (blk == null) continue
                val dup = blk.duplicate()
                for (pos in 0 until size) {
                    out.put(pos, out.get(pos) xor dup.get(pos))
                }
            }
        } else {
            // Q = Σ rotate(block_i, i)
            for ((i, blk) in presentData.withIndex()) {
                if (blk == null) continue
                val dup = blk.duplicate()
                for (pos in 0 until size) {
                    val rot = (pos + i) % size
                    out.put(rot, out.get(rot) xor dup.get(pos))
                }
            }
        }

        out.flip()
        return out.asReadOnlyBuffer() // caller releases
    }
}
