@file:Suppress("DuplicatedCode")

package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.format.ParityCoder
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
        require(dataBlocks.all { it.remaining() == size }) {
            "All data blocks must have equal length"
        }

        val parity = ByteBuffer.allocateDirect(size)

        // initial copy from first block
        val first = dataBlocks[0].duplicate()
        parity.put(first).flip()

        // XOR remaining blocks (absolute get/put, no temp array)
        for (idx in 1 until dataBlocks.size) {
            val buf = dataBlocks[idx].duplicate()
            for (pos in 0 until size) {
                parity.put(pos, parity.get(pos) xor buf.get(pos))
            }
        }
        return listOf(parity.asReadOnlyBuffer())
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
