package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.format.ParityCoder
import kotlin.experimental.xor

class XorParityCoder : ParityCoder {
    override val parityCount: Int = 1

    override fun encode(dataBlocks: List<ByteArray>): List<ByteArray> {
        val parity = ByteArray(dataBlocks[0].size)
        dataBlocks.forEach { blk ->
            for (i in parity.indices) parity[i] = parity[i] xor blk[i]
        }
        return listOf(parity)
    }

    override fun decode(
        lostIndex: Int,
        presentData: List<ByteArray?>,
        presentParity: List<ByteArray?>
    ): ByteArray {
        val size = presentParity[0]!!.size
        val out = ByteArray(size)
        for (i in 0 until size) {
            var acc: Byte = 0
            presentData.forEach { blk -> if (blk != null) acc = acc xor blk[i] }
            presentParity.forEach { p -> if (p != null) acc = acc xor p[i] }
            out[i] = acc
        }
        return out
    }
}