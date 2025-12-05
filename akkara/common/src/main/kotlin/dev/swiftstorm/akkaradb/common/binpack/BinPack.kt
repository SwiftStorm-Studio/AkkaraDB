@file:Suppress("unused")

package dev.swiftstorm.akkaradb.common.binpack

import dev.swiftstorm.akkaradb.common.ByteBufferL
import kotlin.reflect.typeOf

/**
 * BinPack (ByteBufferL-only, v1.0.0)
 *
 * High-performance binary serialization for Kotlin/Java with:
 * - Zero-config data class support
 * - Custom adapter registration via AdapterRegistry
 * - Efficient buffer pooling
 * - Optional performance statistics
 *
 * Public and internal APIs operate exclusively on ByteBufferL.
 * There is no ByteBuffer-based overload anymore.
 */
object BinPack {

    /* ───────── Public API (ByteBufferL only) ───────── */

    /**
     * Encode into a pooled buffer and return it (detached from pool ownership).
     * The returned buffer is positioned at the start of the encoded data.
     *
     * @param value The value to encode
     * @return A ByteBufferL containing the encoded data
     * @throws BinPackFormatException if encoding fails
     */
    inline fun <reified T : Any> encode(value: T): ByteBufferL {
        val adapter = adapter<T>()
        var cap = adapter.estimateSize(value).coerceAtLeast(MIN_CAP)

        while (true) {
            val pooled = borrowPooled(cap)
            try {
                val buf = pooled.buffer
                buf.clear()
                adapter.write(value, buf)

                // Prepare buffer for reading: position=0, limit=written bytes
                val bytesWritten = buf.position
                buf.limit = bytesWritten
                buf.position = 0

                // Update statistics if enabled
                if (AdapterSetting.enableStatistics) {
                    AdapterSetting.encodeCount.incrementAndGet()
                    AdapterSetting.totalBytesEncoded.addAndGet(bytesWritten.toLong())
                }

                pooled.detach()
                return buf
            } catch (e: java.nio.BufferOverflowException) {
                pooled.close()
                // Double capacity for next attempt
                cap = nextCapacity(cap)
            } catch (e: BinPackFormatException) {
                pooled.close()
                throw e
            } catch (t: Throwable) {
                pooled.close()
                if (AdapterSetting.enableDetailedErrors) {
                    throw BinPackFormatException(
                        "Failed to encode value of type ${T::class.qualifiedName}: ${t.message}",
                        t
                    )
                } else {
                    throw BinPackFormatException("Encode failed: ${t.message}", t)
                }
            }
        }
    }

    /**
     * Encode into a caller-provided ByteBufferL.
     * The buffer's position will be advanced by the number of bytes written.
     *
     * @param value The value to encode
     * @param buffer The buffer to write into
     * @throws BinPackFormatException if encoding fails
     * @throws java.nio.BufferOverflowException if buffer is too small
     */
    inline fun <reified T : Any> encodeInto(value: T, buffer: ByteBufferL) {
        try {
            val adapter = adapter<T>()
            val startPos = buffer.position
            adapter.write(value, buffer)

            // Update statistics if enabled
            if (AdapterSetting.enableStatistics) {
                AdapterSetting.encodeCount.incrementAndGet()
                val bytesWritten = buffer.position - startPos
                AdapterSetting.totalBytesEncoded.addAndGet(bytesWritten.toLong())
            }
        } catch (e: BinPackFormatException) {
            throw e
        } catch (t: Throwable) {
            if (AdapterSetting.enableDetailedErrors) {
                throw BinPackFormatException(
                    "Failed to encode value of type ${T::class.qualifiedName}: ${t.message}",
                    t
                )
            } else {
                throw BinPackFormatException("Encode failed: ${t.message}", t)
            }
        }
    }

    /**
     * Decode a value from the given ByteBufferL.
     * The buffer's position will be advanced by the number of bytes read.
     *
     * @param buffer The buffer to read from
     * @return The decoded value
     * @throws BinPackFormatException if decoding fails
     */
    inline fun <reified T : Any> decode(buffer: ByteBufferL): T {
        try {
            val adapter = adapter<T>()
            val startPos = buffer.position
            val result = adapter.read(buffer)

            // Update statistics if enabled
            if (AdapterSetting.enableStatistics) {
                AdapterSetting.decodeCount.incrementAndGet()
                val bytesRead = buffer.position - startPos
                AdapterSetting.totalBytesDecoded.addAndGet(bytesRead.toLong())
            }

            return result
        } catch (e: BinPackFormatException) {
            throw e
        } catch (t: Throwable) {
            if (AdapterSetting.enableDetailedErrors) {
                throw BinPackFormatException(
                    "Failed to decode value of type ${T::class.qualifiedName}: ${t.message}",
                    t
                )
            } else {
                throw BinPackFormatException("Decode failed: ${t.message}", t)
            }
        }
    }

    /**
     * Deep copy via encode→decode round-trip using ByteBufferL pooling.
     * This is more efficient than manual serialization for complex objects.
     *
     * @param value The value to copy
     * @return A deep copy of the value
     * @throws BinPackFormatException if copying fails
     */
    inline fun <reified T : Any> deepCopy(value: T): T {
        if (AdapterSetting.enableStatistics) {
            AdapterSetting.deepCopyCount.incrementAndGet()
        }
        return adapter<T>().copy(value)
    }

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
