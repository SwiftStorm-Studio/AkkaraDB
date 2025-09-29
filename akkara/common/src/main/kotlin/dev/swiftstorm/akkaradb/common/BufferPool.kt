package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import java.io.Closeable

interface BufferPool : Closeable {
    fun get(size: Int = BLOCK_SIZE): ByteBufferL
    fun release(buf: ByteBufferL)
}