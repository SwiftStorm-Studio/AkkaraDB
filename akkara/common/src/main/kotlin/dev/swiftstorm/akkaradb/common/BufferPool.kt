package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import java.nio.ByteBuffer

interface BufferPool : AutoCloseable {
    fun get(size: Int = BLOCK_SIZE): ByteBuffer
    fun release(buf: ByteBuffer)
}
