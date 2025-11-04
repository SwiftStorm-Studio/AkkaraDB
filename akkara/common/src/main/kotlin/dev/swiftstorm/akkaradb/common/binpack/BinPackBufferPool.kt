package dev.swiftstorm.akkaradb.common.binpack

import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max
import kotlin.math.min

/**
 * BinPackBufferPool
 *
 * A simple multi-bucket pool for power-of-two-sized buffers backed by ByteBufferL.
 * Public API exclusively exposes ByteBufferL to eliminate ByteBuffer usage.
 */
object BinPackBufferPool {

    // ---- tunables ----
    private const val MIN_CAP = 32
    private const val MAX_CAP = 8 * 1024 * 1024
    private const val MAX_PER_BUCKET = 64

    private val MIN_POW = pow2CeilPow(MIN_CAP)
    private val MAX_POW = pow2CeilPow(MAX_CAP)
    private val BUCKETS = (MIN_POW..MAX_POW).count()

    private val queues: Array<ConcurrentLinkedDeque<ByteBufferL>> =
        Array(BUCKETS) { ConcurrentLinkedDeque<ByteBufferL>() }
    private val counts: Array<AtomicInteger> =
        Array(BUCKETS) { AtomicInteger(0) }

    /**
     * Get a reusable [ByteBufferL] of at least the specified [size].
     * The returned buffer is cleared.
     */
    fun get(size: Int): ByteBufferL {
        require(size >= 0) { "size must be >= 0: $size" }
        val cap = clampToClass(roundUpToPow2(max(size, MIN_CAP)))
        val idx = capToIndex(cap)

        val q = queues[idx]
        val buf = q.pollFirst()
        if (buf != null) {
            counts[idx].decrementAndGet()
            buf.clear()
            return buf
        }

        // Allocate a new LE buffer via ByteBufferL factory.
        return ByteBufferL.allocate(capacity = cap, direct = true)
    }

    /**
     * Release a [ByteBufferL] back to the pool.
     */
    fun release(buffer: ByteBufferL) {
        val cap = buffer.capacity
        if (!isPow2(cap) || cap < MIN_CAP || cap > MAX_CAP) {
            return
        }
        val idx = capToIndex(cap)
        val cnt = counts[idx]
        if (cnt.get() >= MAX_PER_BUCKET) return

        buffer.clear()
        queues[idx].addFirst(buffer)
        cnt.incrementAndGet()
    }

    // ---- helpers ----

    private fun clampToClass(cap: Int): Int = min(max(cap, MIN_CAP), MAX_CAP)

    private fun roundUpToPow2(x: Int): Int {
        var v = max(1, x)
        v--
        v = v or (v ushr 1)
        v = v or (v ushr 2)
        v = v or (v ushr 4)
        v = v or (v ushr 8)
        v = v or (v ushr 16)
        v++
        return v
    }

    private fun isPow2(x: Int): Boolean = x > 0 && (x and (x - 1)) == 0

    private fun pow2CeilPow(x: Int): Int {
        var p = 0
        var v = 1
        while (v < x) {
            v = v shl 1; p++
        }
        return p
    }

    private fun capToIndex(cap: Int): Int {
        val pow = pow2CeilPow(cap)
        return pow - MIN_POW
    }
}
