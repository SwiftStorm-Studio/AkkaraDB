package dev.swiftstorm.akkaradb.format.akk

import java.nio.ByteBuffer
import java.util.zip.CRC32

object BlockPacker {
    fun pack(payload: ByteArray): ByteArray {
        require(payload.size <= BlockConst.MAX_PAYLOAD) { "Record chunk too large" }

        val buf = ByteBuffer.allocate(BlockConst.BLOCK_SIZE)
        buf.putInt(payload.size)
        buf.put(payload)
        buf.position(BlockConst.BLOCK_SIZE - 4)
        buf.putInt(crc(payload))
        return buf.array()
    }

    internal fun crc(data: ByteArray): Int =
        CRC32().apply { update(data) }.value.toInt()
}