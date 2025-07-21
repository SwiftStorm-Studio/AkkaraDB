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

    /**
     * Borrow a buffer of at least `size` bytes.
     * Race‑safe: retained counter is decremented **inside** the same mutex that
     * removes the buffer, guaranteeing we never dip below 0.
     */
    override fun get(size: Int): ByteBuffer {
        val rounded = roundPow2(maxOf(size, bucketBase))

        val bucketEntry = buckets.ceilingEntry(rounded)
        if (bucketEntry != null && bucketEntry.key <= rounded * 4) {
            val deque = bucketEntry.value
            synchronized(deque) {
                val buf = deque.removeFirstOrNull()
                if (buf != null) {
                    retained.decrementAndGet()   // safe: paired with release() increment
                    hits.increment()
                    return buf.clear()
                }
            }
        }

        // cache miss → fresh direct buffer (retained remains unchanged)
        misses.increment()
        created.increment()
        return ByteBuffer.allocateDirect(rounded)
    }

    override fun release(buf: ByteBuffer) {
        buf.clear()

        // Reserve a slot; roll back if over capacity
        if (retained.incrementAndGet() > capacity) {
            retained.decrementAndGet()
            dropped.increment()
            return
        }

        val rounded = roundPow2(maxOf(buf.capacity(), bucketBase))
        val deque = buckets.computeIfAbsent(rounded) { ArrayDeque() }
        synchronized(deque) { deque.addLast(buf) }
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
        require(v > 0)
        if (v <= 1) return 1
        return Integer.highestOneBit(v - 1) shl 1
    }
}
