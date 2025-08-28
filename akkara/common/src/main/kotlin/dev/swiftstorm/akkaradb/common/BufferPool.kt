package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE

interface BufferPool : AutoCloseable {
    fun get(size: Int = BLOCK_SIZE): ByteBufferL
    fun release(buf: ByteBufferL)

    data class Stats(
        val hits: Long,
        val misses: Long,
        val created: Long,
        val dropped: Long,
        val retained: Int
    ) {
        val hitRate: Double
            get() = if (hits + misses == 0L) 0.0 else hits.toDouble() / (hits + misses)
    }

    fun stats(): Stats
}

inline fun <T> BufferPool.borrow(
    size: Int = BLOCK_SIZE,
    block: (ByteBufferL) -> T
): T {
    val buf = get(size)
    try {
        return block(buf)
    } finally {
        release(buf)
    }
}