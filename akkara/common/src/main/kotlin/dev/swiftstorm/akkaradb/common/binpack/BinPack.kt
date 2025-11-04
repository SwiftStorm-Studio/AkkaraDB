@file:Suppress("unused")

package dev.swiftstorm.akkaradb.common.binpack

import dev.swiftstorm.akkaradb.common.ByteBufferL
import kotlin.reflect.typeOf

/**
 * BinPack (ByteBufferL-only)
 *
 * Public and internal APIs operate exclusively on ByteBufferL.
 * There is no ByteBuffer-based overload anymore.
 */
object BinPack {

    /* ───────── Public API (ByteBufferL only) ───────── */

    /** Encode into a pooled buffer and return it (detached from pool ownership). */
    inline fun <reified T : Any> encode(value: T): ByteBufferL {
        val adapter = adapter<T>()
        var cap = adapter.estimateSize(value).coerceAtLeast(MIN_CAP)
        while (true) {
            val pooled = borrowPooled(cap)
            try {
                val buf = pooled.buffer
                buf.clear()
                adapter.write(value, buf)
                // Flip is not a concept on ByteBufferL; callers are expected to use position/limit
                // semantics from their adapters/consumers. If a "read-mode" is desired, duplicate().
                pooled.detach()
                return buf
            } catch (e: java.nio.BufferOverflowException) {
                pooled.close()
                cap = nextCapacity(cap)
            } catch (t: Throwable) {
                pooled.close()
                throw t
            }
        }
    }

    /** Encode into a caller-provided ByteBufferL. */
    inline fun <reified T : Any> encodeInto(value: T, buffer: ByteBufferL) {
        val adapter = adapter<T>()
        adapter.write(value, buffer)
    }

    /** Decode a value from the given ByteBufferL. */
    inline fun <reified T : Any> decode(buffer: ByteBufferL): T {
        val adapter = adapter<T>()
        return adapter.read(buffer)
    }

    /** Deep copy via encode→decode round-trip using ByteBufferL pooling. */
    inline fun <reified T : Any> deepCopy(value: T): T = adapter<T>().copy(value)

    /* ───────── Internal helpers ───────── */

    @PublishedApi
    internal inline fun <reified T : Any> adapter(): TypeAdapter<T> {
        @Suppress("UNCHECKED_CAST")
        return AdapterResolver.getAdapterForType(typeOf<T>()) as TypeAdapter<T>
    }

    @PublishedApi
    internal fun nextCapacity(cur: Int): Int {
        val doubled = cur.toLong() shl 1
        return if (doubled > Int.MAX_VALUE) Int.MAX_VALUE else doubled.toInt()
    }

    @PublishedApi
    internal fun borrowPooled(capacity: Int): PooledBuffer =
        PooledBuffer(BinPackBufferPool.get(capacity))

    @PublishedApi
    internal class PooledBuffer internal constructor(@PublishedApi internal val buffer: ByteBufferL) : AutoCloseable {
        private var detached = false
        fun detach() {
            detached = true
        }

        override fun close() {
            if (!detached) {
                try {
                    BinPackBufferPool.release(buffer)
                } catch (_: Throwable) { /* swallow */
                }
            }
        }
    }

    @PublishedApi
    internal const val MIN_CAP = 32
}
