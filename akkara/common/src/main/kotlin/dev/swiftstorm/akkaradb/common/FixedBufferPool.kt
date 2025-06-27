package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class FixedBufferPool(
    private val capacity: Int,
    private val bucketBase: Int = BLOCK_SIZE
) : BufferPool {

    /** key = rounded capacity, value = deque of buffers */
    private val buckets = ConcurrentSkipListMap<Int, ArrayDeque<ByteBuffer>>()

    /** current number of buffers retained in the pool */
    private val retained = AtomicInteger(0)

    private val hits = AtomicLong(0)
    private val misses = AtomicLong(0)
    private val created = AtomicLong(0)

    /* ---------------- public API ---------------- */

    override fun get(size: Int): ByteBuffer {
        val rounded = roundPow2(maxOf(size, bucketBase))
        val bucketEntry = buckets.ceilingEntry(rounded)
        if (bucketEntry != null) {
            val deque = bucketEntry.value
            val buf = synchronized(deque) { deque.removeFirstOrNull() }
            if (buf != null) {
                retained.decrementAndGet()
                hits.incrementAndGet()
                buf.clear()
                return buf
            }
        }
        // cache miss → new direct buffer
        misses.incrementAndGet()
        created.incrementAndGet()
        return ByteBuffer.allocateDirect(rounded)
    }

    override fun release(buf: ByteBuffer) {
        buf.clear()
        if (retained.get() >= capacity) return   // overflow → drop

        val rounded = roundPow2(maxOf(buf.capacity(), bucketBase))
        val deque = buckets.computeIfAbsent(rounded) { ArrayDeque() }
        synchronized(deque) { deque.addLast(buf) }
        retained.incrementAndGet()
    }

    override fun stats(): BufferPool.Stats {
        return BufferPool.Stats(
            hits = hits.get(),
            misses = misses.get(),
            created = created.get()
        )
    }

    override fun close() {
        buckets.values.forEach { deque ->
            synchronized(deque) { deque.clear() }
        }
        buckets.clear()
        retained.set(0)
    }

    /* ---------------- helpers ---------------- */

    /** Rounds up to the next power‑of‑two. */
    private fun roundPow2(v: Int): Int {
        return Integer.highestOneBit(v - 1 shl 1)
    }
}
