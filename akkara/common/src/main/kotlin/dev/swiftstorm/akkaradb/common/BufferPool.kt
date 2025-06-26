package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import java.nio.ByteBuffer

interface BufferPool : AutoCloseable {
    fun get(size: Int = BLOCK_SIZE): ByteBuffer
    fun release(buf: ByteBuffer)

    data class Stats(val hits: Long, val misses: Long, val created: Long)
    fun stats(): Stats = Stats(0,0,0)
}

inline fun <T> BufferPool.borrow(
    size: Int = BLOCK_SIZE,
    block: (ByteBuffer) -> T
): T {
    val buf = get(size)
    try {
        return block(buf)
    } finally {
        release(buf)
    }
}