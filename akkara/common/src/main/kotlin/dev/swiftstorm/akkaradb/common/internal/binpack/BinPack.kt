@file:Suppress("unused")

package dev.swiftstorm.akkaradb.common.internal.binpack

import java.nio.BufferOverflowException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.reflect.typeOf

object BinPack {

    /* ───────── Public API ───────── */

    inline fun <reified T : Any> encode(value: T): ByteBuffer {
        val adapter = adapter<T>()
        var cap = adapter.estimateSize(value).coerceAtLeast(MIN_CAP)
        while (true) {
            val pooled = borrowPooled(cap)
            try {
                val buf = pooled.buffer
                buf.clear().order(ByteOrder.LITTLE_ENDIAN)
                adapter.write(value, buf)
                buf.flip()
                pooled.detach()
                return buf
            } catch (_: BufferOverflowException) {
                pooled.close()
                cap = nextCapacity(cap)
            } catch (t: Throwable) {
                pooled.close()
                throw t
            }
        }
    }

    inline fun <reified T : Any> encodeInto(value: T, buffer: ByteBuffer) {
        @Suppress("UNCHECKED_CAST")
        val adapter = adapter<T>()
        buffer.order(ByteOrder.LITTLE_ENDIAN)
        adapter.write(value, buffer)
    }

    inline fun <reified T : Any> encodeReadOnly(value: T): ByteBuffer =
        encode<T>(value).duplicate().asReadOnlyBuffer()

    @Deprecated(
        message = "Prefer ByteBuffer to avoid extra copy.",
        replaceWith = ReplaceWith("BinPack.encode<T>(value)"),
        level = DeprecationLevel.WARNING
    )
    inline fun <reified T : Any> encodeToByteArray(value: T): ByteArray {
        @Suppress("UNCHECKED_CAST")
        val adapter = adapter<T>()
        var cap = adapter.estimateSize(value).coerceAtLeast(MIN_CAP)
        while (true) {
            val pooled = borrowPooled(cap)
            try {
                val buf = pooled.buffer
                buf.clear().order(ByteOrder.LITTLE_ENDIAN)
                adapter.write(value, buf)
                buf.flip()
                val out = ByteArray(buf.remaining())
                buf.get(out)
                pooled.close()
                return out
            } catch (_: BufferOverflowException) {
                pooled.close()
                cap = nextCapacity(cap)
            } catch (t: Throwable) {
                pooled.close()
                throw t
            }
        }
    }

    inline fun <reified T : Any> decodeFromBuffer(buffer: ByteBuffer): T {
        @Suppress("UNCHECKED_CAST")
        val adapter = adapter<T>()
        buffer.order(ByteOrder.LITTLE_ENDIAN)
        return adapter.read(buffer)
    }

    @Deprecated(
        message = "Prefer ByteBuffer to avoid extra copy.",
        replaceWith = ReplaceWith("BinPack.decodeFromBytes<T>(bytes)"),
        level = DeprecationLevel.WARNING
    )
    inline fun <reified T : Any> decodeFromBytes(bytes: ByteArray): T {
        @Suppress("UNCHECKED_CAST")
        val adapter = adapter<T>()
        val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        return adapter.read(buf)
    }

    @Deprecated(
        message = "Prefer ByteBuffer to avoid extra copy.",
        replaceWith = ReplaceWith("BinPack.decodeFromBytesRange<T>(bytes, offset, length)"),
        level = DeprecationLevel.WARNING
    )
    @JvmName("decodeFromBytesRange")
    inline fun <reified T : Any> decodeFromBytes(bytes: ByteArray, offset: Int, length: Int): T {
        require(offset >= 0 && length >= 0 && offset + length <= bytes.size) {
            "Invalid range: offset=$offset, length=$length, size=${bytes.size}"
        }
        @Suppress("UNCHECKED_CAST")
        val adapter = adapter<T>()
        val buf = ByteBuffer.wrap(bytes, offset, length).order(ByteOrder.LITTLE_ENDIAN)
        return adapter.read(buf)
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
    internal class PooledBuffer internal constructor(@PublishedApi internal val buffer: ByteBuffer) : AutoCloseable {
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
