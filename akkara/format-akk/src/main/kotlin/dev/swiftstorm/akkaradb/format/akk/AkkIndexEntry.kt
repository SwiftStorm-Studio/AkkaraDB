package dev.swiftstorm.akkaradb.format.akk

import java.nio.ByteBuffer

/**
 * Fixed-length binary index entry in the Akk V2 shard format.
 * Total size: 32 bytes
 */
data class AkkIndexEntry(
    val keyHash: Int,
    val offset: Long,
    val length: Int,
    val crc32: Int,
    val flags: Byte
) {
    companion object {
        const val SIZE = 32

        fun fromBytes(buffer: ByteBuffer): AkkIndexEntry {
            val keyHash = buffer.int
            val offset = buffer.long
            val length = buffer.int
            val crc32 = buffer.int
            val flags = buffer.get()
            buffer.position(buffer.position() + 11) // skip reserved
            return AkkIndexEntry(keyHash, offset, length, crc32, flags)
        }
    }

    fun toBytes(): ByteArray {
        val buffer = ByteBuffer.allocate(SIZE)
        buffer.putInt(keyHash)
        buffer.putLong(offset)
        buffer.putInt(length)
        buffer.putInt(crc32)
        buffer.put(flags)
        buffer.put(ByteArray(11)) // reserved
        return buffer.array()
    }

    fun isDeleted(): Boolean = (flags.toInt() and 0b00000001) != 0
    fun isCompressed(): Boolean = (flags.toInt() and 0b00000010) != 0
}