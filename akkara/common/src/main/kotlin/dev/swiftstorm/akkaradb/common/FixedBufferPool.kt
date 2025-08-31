package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Collections
import java.util.IdentityHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.LongAdder
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

class FixedBufferPool(
    private val capacity: Int,
    private val bucketBase: Int = BLOCK_SIZE,
    private val normalizeToDirect: Boolean = true,
    private val strict: Boolean = true
) : BufferPool {

    /** key = rounded capacity (pow2), value = deque of buffers */
    private val buckets = ConcurrentSkipListMap<Int, ArrayDeque<ByteBuffer>>()

    private val leased = Collections.newSetFromMap(IdentityHashMap<ByteBuffer, Boolean>())
    private val leasedLock = Any()

    private val retained = AtomicInteger(0)
    private val hits = LongAdder()
    private val misses = LongAdder()
    private val created = LongAdder()
    private val dropped = LongAdder()

    override fun get(size: Int): ByteBufferL {
        val need = maxOf(size, bucketBase)
        val rounded = roundPow2(need)

        // exact bucket
        buckets[rounded]?.let { dq ->
            synchronized(dq) {
                dq.removeFirstOrNull()?.let { buf ->
                    retained.decrementAndGet()
                    hits.increment()
                    val view = ByteBufferL.wrap(buf.clear())
                    synchronized(leasedLock) { leased.add(view.b) }
                    return view
                }
            }
        }

        // ceiling bucket
        buckets.ceilingEntry(rounded)?.let { (_, dq) ->
            synchronized(dq) {
                dq.removeFirstOrNull()?.let { buf ->
                    if (buf.capacity() >= rounded) {
                        retained.decrementAndGet()
                        hits.increment()
                        val view = ByteBufferL.wrap(buf.clear())
                        synchronized(leasedLock) { leased.add(view.b) }
                        return view
                    }
                }
            }
        }

        // miss → allocate
        misses.increment()
        created.increment()
        val bb = if (normalizeToDirect) ByteBuffer.allocateDirect(rounded) else ByteBuffer.allocate(rounded)
        bb.clear()
        val view = ByteBufferL.wrap(bb)
        synchronized(leasedLock) { leased.add(view.b) }
        return view
    }

    override fun release(buf: ByteBufferL) {
        if (strict) {
            val removed = synchronized(leasedLock) { leased.remove(buf.b) }
            if (!removed) {
                throw IllegalArgumentException("release: not the leased buffer (slice/duplicate/foreign)")
            }
            if (buf.b.isReadOnly) {
                throw IllegalArgumentException("release: read-only buffer not allowed")
            }
        } else {
            synchronized(leasedLock) { leased.remove(buf.b) }
        }

        var pooled = buf.b
        if ((normalizeToDirect && !pooled.isDirect) || pooled.isReadOnly) {
            created.increment()
            pooled = ByteBuffer.allocateDirect(pooled.capacity())
        }
        pooled.clear()
        if (pooled.order() != ByteOrder.LITTLE_ENDIAN) {
            pooled.order(ByteOrder.LITTLE_ENDIAN)
        }

        if (retained.incrementAndGet() > capacity) {
            retained.decrementAndGet()
            dropped.increment()
            return
        }

        val rounded = roundPow2(kotlin.math.max(pooled.capacity(), bucketBase))
        val dq = buckets.computeIfAbsent(rounded) { ArrayDeque() }
        synchronized(dq) { dq.addLast(pooled) }
    }

    override fun stats(): BufferPool.Stats = BufferPool.Stats(
        hits = hits.sum(),
        misses = misses.sum(),
        created = created.sum(),
        dropped = dropped.sum(),
        retained = retained.get()
    )

    override fun close() {
        buckets.values.forEach { dq -> synchronized(dq) { dq.clear() } }
        buckets.clear()
        synchronized(leasedLock) { leased.clear() }
        retained.set(0)
    }

    /* ───────── helpers ───────── */
    private fun roundPow2(v: Int): Int {
        require(v > 0) { "roundPow2 requires positive" }
        if (v <= 1) return 1
        return Integer.highestOneBit(v - 1) shl 1
    }
}
