package dev.swiftstorm.akkaradb.common.binpack

import dev.swiftstorm.akkaradb.common.ByteBufferL

/**
 * TypeAdapter<T> (ByteBufferL-only)
 *
 * Contract:
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
    fun write(value: T, buffer: ByteBufferL)

    /** Deserialize value from buffer (advance position; respect limit). */
    fun read(buffer: ByteBufferL): T

    /**
     * Deep copy via encode→decode round-trip.
     * Internally uses ByteBufferL-based pooling only.
     */
    fun copy(value: T): T {
        var cap = estimateSize(value).coerceAtLeast(MIN_CAP)
        while (true) {
            val buf = BinPackBufferPool.get(cap)
            try {
                buf.clear()
                write(value, buf)
                // Prepare a read view: use a read-only duplicate with same limits/positions if needed.
                val ro = buf.asReadOnlyDuplicate().apply {
                    // position/limit already reflect written bytes due to relative puts
                    position = 0
                    limit = buf.position
                }
                val out = read(ro)
                if (ro.remaining != 0) {
                    throw BinPackFormatException("Copy left ${ro.remaining} trailing bytes (cap=$cap)")
                }
                BinPackBufferPool.release(buf)
                return out
            } catch (e: java.nio.BufferOverflowException) {
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
