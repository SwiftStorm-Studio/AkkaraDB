package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import java.util.concurrent.atomic.AtomicReference

/**
 * Global access to shared [BufferPool] instances.
 */
object Pools {
    private val defaultSupplier = {
        FixedBufferPool(
            capacity   = System.getProperty("akkaradb.pool.capacity")?.toInt() ?: 128,
            bucketBase = BLOCK_SIZE
        )
    }
    private val supplierRef = AtomicReference<() -> BufferPool>(defaultSupplier)

    private val TL = ThreadLocal.withInitial { supplierRef.get().invoke() }

    fun io(): BufferPool = TL.get()

    fun nio(capacity: Int = 128,
            bucketBase: Int = BLOCK_SIZE): BufferPool =
        FixedBufferPool(capacity, bucketBase)

    fun setProvider(newSupplier: () -> BufferPool) {
        supplierRef.set(newSupplier)
        TL.remove()
    }
}
