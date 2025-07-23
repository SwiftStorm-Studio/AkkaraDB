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
        val entry = buckets.ceilingEntry(rounded)
        if (entry != null && entry.key >= rounded) {
            val deque = entry.value
            synchronized(deque) {
                val buf = deque.removeFirstOrNull()
                if (buf != null && buf.capacity() >= rounded) {
                    retained.decrementAndGet()
                    hits.increment()
                    return buf.clear()
                }
            }
        }
        misses.increment()
        created.increment()
        return ByteBuffer.allocateDirect(rounded)
    }

    /**
     * Release a buffer back to the pool.
     * If the buffer is read-only, a new direct buffer is allocated to avoid
     * modifying the original.
     * If the pool is at capacity, the buffer is dropped.
     */
    override fun release(buf: ByteBuffer) {
        val toPool = if (buf.isReadOnly) {
            ByteBuffer.allocateDirect(buf.capacity())
        } else {
            buf
        }
        toPool.clear()

        // Reserve a slot; roll back if over capacity
        if (retained.incrementAndGet() > capacity) {
            retained.decrementAndGet()
            dropped.increment()
            return
        }

        val rounded = roundPow2(maxOf(toPool.capacity(), bucketBase))
        val deque = buckets.computeIfAbsent(rounded) { ArrayDeque() }
        synchronized(deque) { deque.addLast(toPool) }
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
