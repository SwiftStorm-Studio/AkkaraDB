package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import java.util.concurrent.atomic.AtomicReference

/**
 * Global access to shared [BufferPool] instances.
 */
object Pools {

    // ————————————————— default —————————————————
    private val defaultPool by lazy {
        FixedBufferPool(
            capacity = System.getProperty(
                "akkaradb.pool.capacity"
            )?.toInt() ?: 128,
            bucketBase = BLOCK_SIZE
        )
    }

    private val providerRef = AtomicReference<() -> BufferPool> { defaultPool }

    fun io(): BufferPool = providerRef.get().invoke()

    fun nio(capacity: Int = 128, bucketBase: Int = BLOCK_SIZE): BufferPool =
        FixedBufferPool(capacity, bucketBase)

    @Synchronized
    fun setProvider(newPool: () -> BufferPool) {
        val p1 = newPool()
        val p2 = newPool()
        require(p1 === p2) {
            "provider must return the SAME BufferPool instance on each call"
        }

        val old = providerRef.getAndSet(newPool)()
        if (old !== p1) old.close()
    }
}
