package dev.swiftstorm.akkaradb.format.api

interface ParityCoder {
    val parityCount: Int
    fun encode(dataBlocks: List<ByteArray>): List<ByteArray>
    fun decode(
        lostIndex: Int,
        presentData: List<ByteArray?>,
        presentParity: List<ByteArray?>
    ): ByteArray
}