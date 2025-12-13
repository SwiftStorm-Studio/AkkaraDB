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

package dev.swiftstorm.akkaradb.common.pool

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Collections
import java.util.IdentityHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.locks.LockSupport
import kotlin.Any
import kotlin.Boolean
import kotlin.IllegalArgumentException
import kotlin.Int
import kotlin.collections.ArrayDeque
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.forEach
import kotlin.let
import kotlin.require
import kotlin.synchronized

/**
 * A fixed-capacity buffer pool with power-of-2 bucketing for efficient buffer reuse.
 *
 * Design:
 * - Buffers are organized into buckets by their rounded-up power-of-2 capacity.
 * - When requesting a buffer, the pool first tries an exact match, then a ceiling match.
 * - If no suitable buffer is available, a new one is allocated (subject to [maxOutstanding] limit).
 * - Released buffers are returned to the appropriate bucket for reuse.
 *
 * Memory safety:
 * - [capacity] limits how many buffers are retained in the pool when released.
 * - [maxOutstanding] limits total buffers that can be in-flight (leased + retained).
 *   This prevents OOM when producers outpace consumers (e.g., WAL fastMode).
 *
 * Thread safety:
 * - All operations are thread-safe via fine-grained synchronization.
 * - Buffers can be acquired and released from different threads.
 *
 * @param capacity Maximum number of buffers to retain in the pool (soft limit)
 * @param bucketBase Minimum buffer size; requests smaller than this are rounded up
 * @param normalizeToDirect If true, always use direct ByteBuffers for better I/O performance
 * @param strict If true, enforces that only leased buffers can be released (catches bugs)
 * @param maxOutstanding Maximum total buffers allowed (leased + retained). 0 = unlimited.
 *                       When limit is reached, [get] will block until buffers are released.
 */
class FixedBufferPool(
    private val capacity: Int,
    private val bucketBase: Int = BLOCK_SIZE,
    private val normalizeToDirect: Boolean = true,
    private val strict: Boolean = true,
    private val maxOutstanding: Int = 0
) : BufferPool {

    /** Buckets organized by rounded capacity (power of 2) -> deque of available buffers */
    private val buckets = ConcurrentSkipListMap<Int, ArrayDeque<ByteBuffer>>()

    /** Tracks all currently leased buffers for strict mode validation */
    private val leased = Collections.newSetFromMap(IdentityHashMap<ByteBuffer, Boolean>())
    private val leasedLock = Any()

    /** Number of buffers currently retained in the pool (not leased) */
    private val retained = AtomicInteger(0)

    /** Number of buffers currently leased out (for maxOutstanding tracking) */
    private val leasedCount = AtomicInteger(0)

    // Statistics
    private val hits = LongAdder()
    private val misses = LongAdder()
    private val created = LongAdder()
    private val dropped = LongAdder()

    /**
     * Acquires a buffer of at least [size] bytes from the pool.
     *
     * The returned buffer:
     * - Has capacity >= size (rounded up to power of 2, minimum [bucketBase])
     * - Is cleared (position=0, limit=capacity)
     * - Is in LITTLE_ENDIAN byte order
     * - MUST be returned via [release] when done
     *
     * If [maxOutstanding] is set and the limit is reached, this method blocks
     * until a buffer becomes available.
     *
     * @param size Minimum required buffer size in bytes
     * @return A ByteBufferL wrapper around the acquired buffer
     */
    override fun get(size: Int): ByteBufferL {
        val need = maxOf(size, bucketBase)
        val rounded = roundPow2(need)

        // Try exact bucket match first (most efficient)
        buckets[rounded]?.let { dq ->
            synchronized(dq) {
                dq.removeFirstOrNull()?.let { buf ->
                    retained.decrementAndGet()
                    leasedCount.incrementAndGet()
                    hits.increment()
                    val view = ByteBufferL.wrap(buf.clear())
                    synchronized(leasedLock) { leased.add(view.buf) }
                    return view
                }
            }
        }

        // Try ceiling bucket (larger buffer is acceptable)
        buckets.ceilingEntry(rounded)?.let { (_, dq) ->
            synchronized(dq) {
                dq.removeFirstOrNull()?.let { buf ->
                    if (buf.capacity() >= rounded) {
                        retained.decrementAndGet()
                        leasedCount.incrementAndGet()
                        hits.increment()
                        val view = ByteBufferL.wrap(buf.clear())
                        synchronized(leasedLock) { leased.add(view.buf) }
                        return view
                    }
                }
            }
        }

        // Cache miss - need to allocate a new buffer
        // First, wait if we've hit the maxOutstanding limit (backpressure)
        if (maxOutstanding > 0) {
            while (leasedCount.get() + retained.get() >= maxOutstanding) {
                LockSupport.parkNanos(1_000) // 1µs spin wait
            }
        }

        misses.increment()
        created.increment()
        leasedCount.incrementAndGet()

        val bb = if (normalizeToDirect) {
            ByteBuffer.allocateDirect(rounded)
        } else {
            ByteBuffer.allocate(rounded)
        }
        bb.clear().order(ByteOrder.LITTLE_ENDIAN)

        val view = ByteBufferL.wrap(bb)
        synchronized(leasedLock) { leased.add(view.buf) }
        return view
    }

    /**
     * Returns a buffer to the pool for reuse.
     *
     * The buffer will be retained in the pool if under [capacity], otherwise dropped.
     * In strict mode, throws if the buffer wasn't leased from this pool.
     *
     * @param buf The buffer to release (must have been obtained via [get])
     * @throws IllegalArgumentException in strict mode if buffer wasn't leased or is read-only
     */
    override fun release(buf: ByteBufferL) {
        // Validate in strict mode
        if (strict) {
            val removed = synchronized(leasedLock) { leased.remove(buf.buf) }
            if (!removed) {
                throw IllegalArgumentException("release: not a leased buffer (slice/duplicate/foreign)")
            }
            if (buf.buf.isReadOnly) {
                throw IllegalArgumentException("release: read-only buffer not allowed")
            }
        } else {
            synchronized(leasedLock) { leased.remove(buf.buf) }
        }

        leasedCount.decrementAndGet()

        // Normalize to direct buffer if needed
        var pooled = buf.buf
        if ((normalizeToDirect && !pooled.isDirect) || pooled.isReadOnly) {
            created.increment()
            pooled = ByteBuffer.allocateDirect(pooled.capacity())
        }

        // Reset buffer state for reuse
        pooled.clear()
        if (pooled.order() != ByteOrder.LITTLE_ENDIAN) {
            pooled.order(ByteOrder.LITTLE_ENDIAN)
        }

        // Check if we should retain this buffer or drop it
        if (retained.incrementAndGet() > capacity) {
            retained.decrementAndGet()
            dropped.increment()
            return // Let GC reclaim it
        }

        // Return to appropriate bucket
        val bucketKey = roundPow2(maxOf(pooled.capacity(), bucketBase))
        val dq = buckets.computeIfAbsent(bucketKey) { ArrayDeque() }
        synchronized(dq) { dq.addLast(pooled) }
    }

    /**
     * Returns current pool statistics for monitoring and debugging.
     */
    override fun stats(): BufferPool.Stats = BufferPool.Stats(
        hits = hits.sum(),
        misses = misses.sum(),
        created = created.sum(),
        dropped = dropped.sum(),
        retained = retained.get()
    )

    /**
     * Closes the pool and releases all retained buffers.
     *
     * After calling this, the pool should not be used.
     * Any leased buffers become orphaned (will be GC'd when released).
     */
    override fun close() {
        buckets.values.forEach { dq -> synchronized(dq) { dq.clear() } }
        buckets.clear()
        synchronized(leasedLock) { leased.clear() }
        retained.set(0)
        leasedCount.set(0)
    }

    /**
     * Returns the number of buffers currently leased out.
     * Useful for debugging and monitoring.
     */
    fun leasedCount(): Int = leasedCount.get()

    /* ───────── helpers ───────── */

    /**
     * Rounds up to the nearest power of 2.
     * Examples: 1->1, 2->2, 3->4, 5->8, 1000->1024
     */
    private fun roundPow2(v: Int): Int {
        require(v > 0) { "roundPow2 requires positive value" }
        if (v <= 1) return 1
        return Integer.highestOneBit(v - 1) shl 1
    }
}