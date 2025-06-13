package dev.swiftstorm.akkaradb.format

import java.nio.ByteBuffer

interface ParityCoder {
    val parityCount: Int
    fun encode(dataBlocks: List<ByteBuffer>): List<ByteBuffer>
    fun decode(
        lostIndex: Int,
        presentData: List<ByteBuffer?>,
        presentParity: List<ByteBuffer?>
    ): ByteBuffer
}