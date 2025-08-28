package dev.swiftstorm.akkaradb.format.api

import dev.swiftstorm.akkaradb.common.ByteBufferL

interface ParityCoder {
    val parityCount: Int
    fun encode(dataBlocks: List<ByteBufferL>): List<ByteBufferL>
    fun decode(
        lostIndex: Int,
        presentData: List<ByteBufferL?>,
        presentParity: List<ByteBufferL?>
    ): ByteBufferL
}