@file:Suppress("DuplicatedCode")

package dev.swiftstorm.akkaradb.format.akk

import java.nio.ByteBuffer

object BlockUnpacker {
    fun unpack(block: ByteArray): ByteArray {
        require(block.size == BlockConst.BLOCK_SIZE)
        val buf = ByteBuffer.wrap(block)
        val len  = buf.int
        require(len >= 0 && len <= BlockConst.MAX_PAYLOAD)
        val payload = ByteArray(len)
        buf.get(payload)
        val expected = ByteBuffer.wrap(block, BlockConst.BLOCK_SIZE - 4, 4).int
        require(expected == BlockPacker.crc(payload)) { "CRC mismatch" }
        return payload
    }

    fun unpackOrNull(block: ByteArray): ByteArray? {
        if (block.all { it == 0.toByte() }) return null          // 全 0
        val buf = ByteBuffer.wrap(block)
        val len = buf.int
        if (len == 0) return null                                // len=0 はパディング
        require(len in 1..BlockConst.MAX_PAYLOAD)
        val payload = ByteArray(len)
        buf.get(payload)
        val expected = ByteBuffer.wrap(block, BlockConst.BLOCK_SIZE - 4, 4).int
        require(expected == BlockPacker.crc(payload)) { "CRC mismatch" }
        return payload
    }
}