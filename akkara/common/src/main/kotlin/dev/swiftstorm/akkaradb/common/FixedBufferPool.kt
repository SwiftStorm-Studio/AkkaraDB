package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger

class FixedBufferPool(
    private val capacity: Int,
    private val bucketBase: Int = BLOCK_SIZE
) : BufferPool {

    /** key = rounded capacity, value = deque of buffers */
    private val buckets = ConcurrentSkipListMap<Int, ArrayDeque<ByteBuffer>>()

    /** current number of buffers retained in the pool */
    private val retained = AtomicInteger(0)

    /* ---------------- public API ---------------- */

    override fun get(size: Int): ByteBuffer {
        val rounded = roundPow2(maxOf(size, bucketBase))
        val bucketEntry = buckets.ceilingEntry(rounded)
        if (bucketEntry != null) {
            val deque = bucketEntry.value
            val buf = synchronized(deque) { deque.removeFirstOrNull() }
            if (buf != null) {
                retained.decrementAndGet()
                buf.clear()
                return buf
            }
        }
        // cache miss → new direct buffer
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
        var x = v - 1
        x = x or (x ushr 1)
        x = x or (x ushr 2)
        x = x or (x ushr 4)
        x = x or (x ushr 8)
        x = x or (x ushr 16)
        return x + 1
    }
}
