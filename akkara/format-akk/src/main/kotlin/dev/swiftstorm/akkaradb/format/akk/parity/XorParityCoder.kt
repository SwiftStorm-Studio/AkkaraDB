@file:Suppress("DuplicatedCode")

package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
import kotlin.experimental.xor

/**
 * Single-XOR parity coder (m = 1) operating directly on {@link ByteBuffer}s.
 *
 * Zero-copy version: uses absolute get/put so no intermediate heap array
 * is allocated.  All input buffers must share the same `remaining()` size.
 */
class XorParityCoder : ParityCoder {

    override val parityCount: Int = 1

    /* ---------- encode ---------- */

    override fun encode(dataBlocks: List<ByteBuffer>): List<ByteBuffer> {
        require(dataBlocks.isNotEmpty()) { "dataBlocks must not be empty" }
        val size = dataBlocks[0].remaining()
        require(dataBlocks.all { it.remaining() == size }) { "All data blocks must have equal length" }

        val pool   = Pools.io()
        val parity = pool.get(size)

        try {
            val longs = size ushr 3

            parity.put(dataBlocks[0].duplicate()).flip()

            for (idx in 1 until dataBlocks.size) {
                val blk = dataBlocks[idx].duplicate()

                var i = 0
                while (i < longs) {
                    val v = blk.getLong(i * 8)
                    parity.putLong(i * 8, parity.getLong(i * 8) xor v)
                    i++
                }
                var pos = longs * 8
                while (pos < size) {
                    parity.put(pos, (parity.get(pos) xor blk.get(pos)).toByte())
                    pos++
                }
            }

            parity.flip()
            return listOf(parity.asReadOnlyBuffer())
        } finally {
            pool.release(parity)
        }
    }

    /* ---------- decode ---------- */

    override fun decode(
        lostIndex: Int,
        presentData: List<ByteBuffer?>,
        presentParity: List<ByteBuffer?>
    ): ByteBuffer {
        val ref = presentParity.firstOrNull { it != null }
            ?: presentData.first { it != null }!!
        val size = ref.remaining()

        val out = ByteBuffer.allocateDirect(size)

        // XOR all available data & parity directly into 'out'
        fun xorInto(buf: ByteBuffer?) {
            if (buf == null) return
            val dup = buf.duplicate()
            for (pos in 0 until size) {
                out.put(pos, out.get(pos) xor dup.get(pos))
            }
        }
        presentData.forEach(::xorInto)
        presentParity.forEach(::xorInto)

        out.flip()
        return out.asReadOnlyBuffer()
    }
}
