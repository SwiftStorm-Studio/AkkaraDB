package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.pool.FixedBufferPool
import java.io.Closeable
import java.util.concurrent.atomic.AtomicReference

/**
 * Global access to shared [BufferPool] instances.
 */
object Pools {
    private val defaultSupplier = {
        FixedBufferPool(
            capacity = 512,
            bucketBase = BLOCK_SIZE
        )
    }
    private val supplierRef = AtomicReference<() -> BufferPool>(defaultSupplier)

    private val TL = ThreadLocal.withInitial { supplierRef.get().invoke() }

    fun io(): BufferPool = TL.get()

    fun nio(
        capacity: Int = 128,
        bucketBase: Int = BLOCK_SIZE
    ): BufferPool =
        FixedBufferPool(capacity, bucketBase)

    /**
     * Swap the global [BufferPool] provider.
     *
     * - Closes the existing Thread‑Local pool instance (avoids FD/native‑mem leaks).
     * - Atomically installs the new supplier.
     * - Clears the ThreadLocal so the next access lazily instantiates
     *   a pool from the new supplier.
     */
    fun setProvider(newSupplier: () -> BufferPool) {
        // 1) Close current pool bound to this thread, if any
        TL.get().let { (it as? Closeable)?.close() }

        // 2) Swap supplier atomically
        supplierRef.set(newSupplier)

        // 3) Remove ThreadLocal to force re‑creation with the new supplier
        TL.remove()
    }

}