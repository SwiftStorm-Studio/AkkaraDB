package dev.swiftstorm.akkaradb.common.internal.binpack

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * TypeAdapter<T>
 *
 * Contract:
 * - ByteOrder: LITTLE_ENDIAN is assumed.
 * - write(value, buffer):
 *     - Must advance buffer.position() by the number of bytes written.
 *     - Must respect buffer.limit(); throw BufferOverflowException if not enough space.
 * - read(buffer):
 *     - Must advance buffer.position() by the number of bytes consumed.
 *     - Should throw BinPackFormatException (or a subtype) on malformed input.
 * - estimateSize(value):
 *     - Should return an upper bound if possible. If it underestimates,
 *       copy() will resize and retry.
 */
interface TypeAdapter<T> {
    /** Estimated serialized size. Prefer an upper bound if feasible. */
    fun estimateSize(value: T): Int

    /** Serialize value into buffer (advance position; respect limit). */
    fun write(value: T, buffer: ByteBuffer)

    /** Deserialize value from buffer (advance position; respect limit). */
    fun read(buffer: ByteBuffer): T

    /**
     * Deep copy via encode→decode round-trip.
     * - Uses BinPackBufferPool.get/release.
     * - Retries with doubled capacity if estimateSize() underestimates.
     * - Enforces LITTLE_ENDIAN.
     * - Optionally verifies full consumption (no trailing bytes) to catch adapter bugs.
     */
    fun copy(value: T): T {
        var cap = estimateSize(value).coerceAtLeast(MIN_CAP)
        while (true) {
            val buf = BinPackBufferPool.get(cap)
            try {
                buf.clear().order(ByteOrder.LITTLE_ENDIAN)
                write(value, buf)
                buf.flip().order(ByteOrder.LITTLE_ENDIAN)
                val out = read(buf)
                // Detect trailing bytes from mismatched write/read (useful during development).
                if (buf.hasRemaining()) {
                    throw BinPackFormatException(
                        "Copy left ${buf.remaining()} trailing bytes (cap=$cap)"
                    )
                }
                BinPackBufferPool.release(buf)
                return out
            } catch (e: java.nio.BufferOverflowException) {
                // Underestimation: release and retry with doubled capacity.
                BinPackBufferPool.release(buf)
                cap = nextCapacity(cap)
                continue
            } catch (t: Throwable) {
                BinPackBufferPool.release(buf)
                throw t
            }
        }
    }

    companion object {
        private const val MIN_CAP = 32
        private fun nextCapacity(cur: Int): Int {
            val doubled = cur.toLong() shl 1
            return if (doubled > Int.MAX_VALUE) Int.MAX_VALUE else doubled.toInt()
        }
    }
}

/** Thrown when input bytes do not conform to the expected BinPack wire format. */
class BinPackFormatException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
