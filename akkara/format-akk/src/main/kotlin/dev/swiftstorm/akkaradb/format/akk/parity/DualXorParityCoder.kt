@file:Suppress("DuplicatedCode")

package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.format.api.ParityCoder
import kotlin.experimental.xor

class DualXorParityCoder : ParityCoder {

    override val parityCount: Int = 2   // P と Q

    /** ---------- encode ---------- */

    override fun encode(dataBlocks: List<ByteArray>): List<ByteArray> {
        val sz = dataBlocks[0].size
        val p = ByteArray(sz)
        val q = ByteArray(sz)

        dataBlocks.forEachIndexed { idx, blk ->
            val sh = (idx + 1) and 7
            for (i in 0 until sz) {
                val b = blk[i]
                p[i] = p[i] xor b
                q[i] = q[i] xor b.rotateLeft(sh)
            }
        }
        return listOf(p, q)
    }

    /** ---------- decode ---------- */

    override fun decode(
        lostIndex: Int,
        presentData: List<ByteArray?>,
        presentParity: List<ByteArray?>
    ): ByteArray {
        val sz = (presentParity.firstOrNull { it != null } ?: presentData.first { it != null })!!.size

        val pCalc = ByteArray(sz)
        val qCalc = ByteArray(sz)

        presentData.forEachIndexed { idx, blk ->
            if (blk != null) {
                val sh = (idx + 1) and 7
                for (i in 0 until sz) {
                    val b = blk[i]
                    pCalc[i] = pCalc[i] xor b
                    qCalc[i] = qCalc[i] xor b.rotateLeft(sh)
                }
            }
        }

        return when {
            /*--------- case A: lost data_i ---------*/
            lostIndex < presentData.size -> {
                val sh = (lostIndex + 1) and 7

                val pMissing = pCalc xor presentParity[0]!!
                val qMissing = qCalc xor presentParity[1]!!

                ByteArray(sz) { off ->
                    (pMissing[off] xor qMissing[off].rotateRight(sh)).rotateRight(8 - sh)
                }
            }

            /*--------- case B: lost P parity ---------*/
            lostIndex == presentData.size -> pCalc

            /*--------- case C: lost Q parity ---------*/
            else -> qCalc
        }
    }

    /** ---------- small helpers ---------- */

    private fun Byte.rotateLeft(bits: Int): Byte =
        (((this.toInt() and 0xFF) shl bits) or
                ((this.toInt() and 0xFF) ushr (8 - bits))).toByte()

    private fun Byte.rotateRight(bits: Int): Byte =
        (((this.toInt() and 0xFF) ushr bits) or
                ((this.toInt() and 0xFF) shl (8 - bits))).toByte()

    private infix fun ByteArray.xor(other: ByteArray): ByteArray =
        ByteArray(size) { i -> this[i] xor other[i] }
}
