package dev.swiftstorm.akkaradb.common.internal.binpack

import dev.swiftstorm.akkaradb.common.internal.binpack.BinPackBufferPool.ENFORCE_LITTLE_ENDIAN
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max
import kotlin.math.min

object BinPackBufferPool {

    // ---- tunables ----
    private const val MIN_CAP = 32                      // Minimum capacity of a buffer in bytes
    private const val MAX_CAP = 8 * 1024 * 1024         // Never allocate buffers larger than this (8 MiB)
    private const val MAX_PER_BUCKET = 64               // Maximum amount of each bucket (For GC inhibition)
    private const val ENFORCE_LITTLE_ENDIAN = true      // Set the Endian when returning buffers

    private val MIN_POW = pow2CeilPow(MIN_CAP)
    private val MAX_POW = pow2CeilPow(MAX_CAP)
    private val BUCKETS = (MIN_POW..MAX_POW).count()

    private val queues: Array<ConcurrentLinkedDeque<ByteBuffer>> =
        Array(BUCKETS) { ConcurrentLinkedDeque<ByteBuffer>() }
    private val counts: Array<AtomicInteger> =
        Array(BUCKETS) { AtomicInteger(0) }

    /**
     * Get a reusable [ByteBuffer] of at least the specified [size].
     * The returned buffer will be cleared and set to little-endian order if [ENFORCE_LITTLE_ENDIAN] is true.
     */
    fun get(size: Int): ByteBuffer {
        require(size >= 0) { "size must be >= 0: $size" }
        val cap = clampToClass(roundUpToPow2(max(size, MIN_CAP)))
        val idx = capToIndex(cap)

        val q = queues[idx]
        val buf = q.pollFirst()
        if (buf != null) {
            counts[idx].decrementAndGet()
            buf.clear()
            if (ENFORCE_LITTLE_ENDIAN) buf.order(ByteOrder.LITTLE_ENDIAN)
            return buf
        }

        val allocated = ByteBuffer.allocateDirect(cap)
        if (ENFORCE_LITTLE_ENDIAN) allocated.order(ByteOrder.LITTLE_ENDIAN)
        return allocated
    }

    /**
     * Release a [ByteBuffer] back to the pool.
     * The buffer must have a capacity that is a power of two and within the defined limits.
     * If the buffer is too large or not a power of two, it will be ignored.
     */
    fun release(buffer: ByteBuffer) {
        val cap = buffer.capacity()
        if (!isPow2(cap) || cap < MIN_CAP || cap > MAX_CAP) {
            return
        }
        val idx = capToIndex(cap)
        val cnt = counts[idx]
        if (cnt.get() >= MAX_PER_BUCKET) return

        buffer.clear()
        if (ENFORCE_LITTLE_ENDIAN) buffer.order(ByteOrder.LITTLE_ENDIAN)
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
        // cap = 2^(MIN_POW + idx)
        val pow = pow2CeilPow(cap)
        return pow - MIN_POW
    }
}
