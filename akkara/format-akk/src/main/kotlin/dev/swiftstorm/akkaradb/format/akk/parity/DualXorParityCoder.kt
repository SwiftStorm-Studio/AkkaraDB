@file:Suppress("DuplicatedCode")

package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
import kotlin.experimental.xor

/**
 * Dual-XOR parity coder (m = 2, “P + Q”).
 *
 * Encoding
 * --------
 *   P = Σ Di                 (byte-wise XOR)
 *
 *   Q = Σ rotLeft(Di, i+1)   (i = data-index, 0-based)
 *
 * Recovers any single-lane loss: one data block *or* either parity.
 * All buffers must share identical `remaining()` length.
 */
class DualXorParityCoder : ParityCoder {

    override val parityCount: Int = 2      // P and Q

    /* ---------- encode ---------- */

    override fun encode(dataBlocks: List<ByteBuffer>): List<ByteBuffer> {
        require(dataBlocks.isNotEmpty()) { "dataBlocks must not be empty" }
        val sz = dataBlocks[0].remaining()
        require(dataBlocks.all { it.remaining() == sz }) {
            "All data blocks must have equal length"
        }

        val p = ByteBuffer.allocateDirect(sz)
        val q = ByteBuffer.allocateDirect(sz)

        /* accumulate P & Q directly */
        dataBlocks.forEachIndexed { idx, src ->
            val sh = (idx + 1) and 7
            val dup = src.duplicate()
            for (pos in 0 until sz) {
                val b = dup.get(pos)
                p.put(pos, p.get(pos) xor b)
                q.put(pos, q.get(pos) xor b.rotateLeft(sh))
            }
        }

        p.flip(); q.flip()
        return listOf(p.asReadOnlyBuffer(), q.asReadOnlyBuffer())
    }

    /* ---------- decode ---------- */

    override fun decode(
        lostIndex: Int,
        presentData: List<ByteBuffer?>,
        presentParity: List<ByteBuffer?>
    ): ByteBuffer {
        val ref = presentParity.firstOrNull { it != null }
            ?: presentData.first { it != null }!!
        val sz = ref.remaining()

        val pCalc = ByteBuffer.allocateDirect(sz)
        val qCalc = ByteBuffer.allocateDirect(sz)

        presentData.forEachIndexed { idx, buf ->
            if (buf != null) {
                val sh = (idx + 1) and 7
                val dup = buf.duplicate()
                for (pos in 0 until sz) {
                    val b = dup.get(pos)
                    pCalc.put(pos, pCalc.get(pos) xor b)
                    qCalc.put(pos, qCalc.get(pos) xor b.rotateLeft(sh))
                }
            }
        }

        return when {
            /* ---- A. lost data_i ---- */
            lostIndex < presentData.size -> {
                val sh = (lostIndex + 1) and 7
                val pMissing = xorBuffers(pCalc, presentParity[0]!!)
                val qMissing = xorBuffers(qCalc, presentParity[1]!!)

                val out = ByteBuffer.allocateDirect(sz)
                for (pos in 0 until sz) {
                    val recovered = (pMissing.get(pos) xor
                            qMissing.get(pos).rotateRight(sh))
                        .rotateRight(8 - sh)
                    out.put(recovered)
                }
                out.flip(); out.asReadOnlyBuffer()
            }

            /* ---- B. lost P parity ---- */
            lostIndex == presentData.size -> pCalc.asReadOnlyBuffer()

            /* ---- C. lost Q parity ---- */
            else -> qCalc.asReadOnlyBuffer()
        }
    }

    /* ---------- helpers ---------- */

    /** dst = a xor b (lengths are identical) */
    private fun xorBuffers(a: ByteBuffer, b: ByteBuffer): ByteBuffer {
        val sz = a.remaining()
        val out = ByteBuffer.allocateDirect(sz)
        val ad = a.duplicate(); val bd = b.duplicate()
        for (pos in 0 until sz) {
            out.put(pos, (ad.get(pos) xor bd.get(pos)))
        }
        out.flip(); return out
    }

    private fun Byte.rotateLeft(bits: Int): Byte =
        (((this.toInt() and 0xFF) shl bits) or
                ((this.toInt() and 0xFF) ushr (8 - bits))).toByte()

    private fun Byte.rotateRight(bits: Int): Byte =
        (((this.toInt() and 0xFF) ushr bits) or
                ((this.toInt() and 0xFF) shl (8 - bits))).toByte()
}
