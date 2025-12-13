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
import dev.swiftstorm.akkaradb.common.Pools.io
import dev.swiftstorm.akkaradb.common.pool.FixedBufferPool
import java.io.Closeable

/**
 * Global access to shared [BufferPool] instances.
 *
 * Design notes:
 * - Uses a single global pool instead of ThreadLocal to ensure buffers are
 *   properly returned to the same pool regardless of which thread releases them.
 * - This fixes the issue where buffers acquired in one thread (e.g., main) and
 *   released in another thread (e.g., WAL flusher) would leak because ThreadLocal
 *   pools are separate per thread.
 * - The global pool is thread-safe via internal synchronization in FixedBufferPool.
 */
object Pools {
    /**
     * Global shared buffer pool for I/O operations.
     *
     * Capacity is set to 2048 to handle high-throughput scenarios like
     * WAL writes where many buffers may be in-flight simultaneously.
     */
    private val globalPool: BufferPool = FixedBufferPool(
        capacity = 4096,
        bucketBase = BLOCK_SIZE
    )

    /**
     * Returns the global I/O buffer pool.
     *
     * Thread-safe: can be called from any thread. Buffers obtained from this pool
     * can be released from any thread and will be returned to the same pool.
     */
    fun io(): BufferPool = globalPool

    /**
     * Creates a new independent buffer pool for specialized use cases.
     *
     * Use this when you need a dedicated pool with custom settings that won't
     * interfere with the global pool (e.g., for testing or isolated subsystems).
     *
     * @param capacity Maximum number of buffers to retain in the pool
     * @param bucketBase Minimum buffer size (rounded up to power of 2)
     * @return A new BufferPool instance (caller is responsible for closing it)
     */
    fun nio(
        capacity: Int = 128,
        bucketBase: Int = BLOCK_SIZE
    ): BufferPool = FixedBufferPool(capacity, bucketBase)

    /**
     * Closes the global pool and releases all retained buffers.
     *
     * Should be called during application shutdown to ensure clean resource cleanup.
     * After calling this, [io] should not be used.
     */
    fun close() {
        (globalPool as? Closeable)?.close()
    }

    /**
     * Returns statistics for the global I/O pool.
     *
     * Useful for monitoring pool efficiency:
     * - High hit rate = good buffer reuse
     * - High miss rate = may need larger capacity
     * - High dropped count = capacity is limiting retention
     */
    fun stats(): BufferPool.Stats = globalPool.stats()
}