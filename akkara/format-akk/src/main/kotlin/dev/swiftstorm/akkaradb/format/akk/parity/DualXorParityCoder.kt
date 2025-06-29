@file:Suppress("DuplicatedCode")

package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.borrow
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
class DualXorParityCoder(
    private val pool: BufferPool = Pools.io()
) : ParityCoder {

    override val parityCount: Int = 2      // P and Q

    /* ---------- encode ---------- */

    override fun encode(dataBlocks: List<ByteBuffer>): List<ByteBuffer> {
        require(dataBlocks.isNotEmpty())
        val sz = dataBlocks[0].remaining()
        require(dataBlocks.all { it.remaining() == sz }) { "All data blocks must have equal length" }

        val p = pool.get(sz)
        val q = pool.get(sz)

        val longs = sz ushr 3
        dataBlocks.forEachIndexed { idx, src ->
            val sh = (idx + 1) and 7
            val dl = src.duplicate()

            var i = 0
            while (i < longs) {
                val v = dl.getLong(i * 8)
                p.putLong(i * 8, p.getLong(i * 8) xor v)
                q.putLong(i * 8, q.getLong(i * 8) xor v.rotateLeft(sh))
                i++
            }

            var pos = longs * 8
            while (pos < sz) {
                val b = dl.get(pos)
                p.put(pos, (p.get(pos) xor b).toByte())
                q.put(pos, (q.get(pos) xor b.rotateLeft(sh)).toByte())
                pos++
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

        val pCalc = pool.get(sz)
        val qCalc = pool.get(sz)

        try {
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
                lostIndex < presentData.size -> {
                    val sh = (lostIndex + 1) and 7
                    val pMissing = xorBuffers(pCalc, presentParity[0]!!)
                    val qMissing = xorBuffers(qCalc, presentParity[1]!!)

                    val out = pool.get(sz)
                    for (pos in 0 until sz) {
                        val recovered = (pMissing.get(pos) xor
                                qMissing.get(pos).rotateRight(sh))
                            .rotateRight(8 - sh)
                        out.put(pos, recovered)
                    }
                    out.flip(); out.asReadOnlyBuffer()
                }

                lostIndex == presentData.size -> {
                    pCalc.flip()                         // make it ready for read
                    pool.release(qCalc)
                    pCalc.asReadOnlyBuffer()
                }

                else -> {
                    qCalc.flip()
                    pool.release(pCalc)
                    qCalc.asReadOnlyBuffer()
                }
            }
        } catch (e: Exception) {
            pool.release(pCalc)
            pool.release(qCalc)
            throw e
        }
    }

    /* ---------- helpers ---------- */

    /** dst = a xor b (lengths are identical) */
    private fun xorBuffers(a: ByteBuffer, b: ByteBuffer): ByteBuffer =
        pool.borrow(a.remaining()) { scratch ->
            val ad = a.duplicate();
            val bd = b.duplicate()
            for (pos in 0 until a.remaining()) {
                scratch.put(pos, (ad.get(pos) xor bd.get(pos)))
            }
            scratch.flip()

            val out = ByteBuffer.allocateDirect(a.remaining())
            out.put(scratch); out.flip()
            out.asReadOnlyBuffer()
        }

    private fun Byte.rotateLeft(bits: Int): Byte =
        (((this.toInt() and 0xFF) shl bits) or
                ((this.toInt() and 0xFF) ushr (8 - bits))).toByte()

    private fun Byte.rotateRight(bits: Int): Byte =
        (((this.toInt() and 0xFF) ushr bits) or
                ((this.toInt() and 0xFF) shl (8 - bits))).toByte()
}
