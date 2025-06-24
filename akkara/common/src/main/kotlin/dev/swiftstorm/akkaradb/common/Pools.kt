package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE

/**
 * Global access to shared BufferPool instances.
 * Replace `provider` in tests if you need a mock pool.
 */
object Pools {
    private val defaultPool by lazy { FixedBufferPool(capacity = 128, bufSize = BLOCK_SIZE) }

    /** Function to obtain the active pool (can be swapped in tests). */
    @Volatile
    var provider: () -> BufferPool = { defaultPool }

    fun io(): BufferPool = provider()
}