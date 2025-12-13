package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Reference-counted SSTableReader wrapper.
 *
 * Responsibilities:
 *  - Manage reference counting for shared SSTableReader instances.
 *  - Ensure proper resource cleanup when no longer in use.
 */
class RefCountedSSTableReader(
    private val inner: SSTableReader
) : Closeable {
    // Reference count starts at 1
    private val refCount = AtomicInteger(1)
    private val closed = AtomicBoolean(false)

    /** Attempt to acquire a reference. Returns false if already closed. */
    fun acquire(): Boolean {
        while (true) {
            val cur = refCount.get()
            if (cur <= 0) return false
            if (refCount.compareAndSet(cur, cur + 1)) return true
        }
    }

    /** Release a previously acquired reference. */
    fun release() {
        val newVal = refCount.decrementAndGet()
        if (newVal == 0 && closed.get()) {
            inner.close()
        }
    }

    /** Close this reference. Actual close of inner happens when refCount reaches zero. */
    override fun close() {
        if (closed.compareAndSet(false, true)) {
            val newVal = refCount.decrementAndGet()
            if (newVal == 0) {
                inner.close()
            }
        }
    }

    /** Delegate get to inner SSTableReader. */
    fun get(key: ByteBufferL) = inner.get(key)

    /** Delegate range to inner SSTableReader. */
    fun range(start: ByteBufferL, end: ByteBufferL?) = inner.range(start, end)
}