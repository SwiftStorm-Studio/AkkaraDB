package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.LongAdder

/**
 * Fixed-capacity ByteBuffer pool.
 */
class FixedBufferPool(
    private val capacity: Int,
    private val bucketBase: Int = BLOCK_SIZE
) : BufferPool {

    /** key = rounded capacity, value = deque of buffers */
    private val buckets = ConcurrentSkipListMap<Int, ArrayDeque<ByteBuffer>>()

    private val retained = AtomicInteger(0)

    private val hits = LongAdder()
    private val misses = LongAdder()
    private val created = LongAdder()
    private val dropped = LongAdder()

    /* ───────────────── public API ───────────────── */

    override fun get(size: Int): ByteBuffer {
        val rounded = roundPow2(maxOf(size, bucketBase))

        val bucketEntry = buckets.ceilingEntry(rounded)
        if (bucketEntry != null && bucketEntry.key <= rounded * 4) {
            val deque = bucketEntry.value
            val buf = synchronized(deque) { deque.removeFirstOrNull() }
            if (buf != null) {
                retained.decrementAndGet()
                hits.increment()
                return buf.clear()
            }
        }

        // cache miss → fresh direct buffer
        misses.increment()
        created.increment()
        return ByteBuffer.allocateDirect(rounded)
    }

    override fun release(buf: ByteBuffer) {
        buf.clear()

        if (retained.get() >= capacity) {
            dropped.increment()
            return
        }

        val rounded = roundPow2(maxOf(buf.capacity(), bucketBase))
        val deque = buckets.computeIfAbsent(rounded) { ArrayDeque() }
        synchronized(deque) { deque.addLast(buf) }
        retained.incrementAndGet()
    }

    override fun stats(): BufferPool.Stats = BufferPool.Stats(
        hits = hits.sum(),
        misses = misses.sum(),
        created = created.sum(),
        dropped = dropped.sum(),
        retained = retained.get()
    )

    override fun close() {
        buckets.values.forEach { deque ->
            synchronized(deque) { deque.clear() }
        }
        buckets.clear()
        retained.set(0)
    }

    /* ───────────────── helpers ───────────────── */

    private fun roundPow2(v: Int): Int {
        if (v <= 1) return 1
        return Integer.highestOneBit(v - 1) shl 1
    }
}
