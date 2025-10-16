/*
 * AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

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