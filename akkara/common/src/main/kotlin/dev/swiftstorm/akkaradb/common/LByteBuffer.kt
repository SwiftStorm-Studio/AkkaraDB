@file:Suppress("NOTHING_TO_INLINE", "unused")

package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.LByteBuffer.Companion.allocate
import dev.swiftstorm.akkaradb.common.LByteBuffer.Companion.wrap
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.ReadOnlyBufferException

/**
 * A **little-endian–enforcing** wrapper around [ByteBuffer] with a lean, allocation-free API surface.
 *
 * This value class guarantees that **all views returned from and used within this wrapper are
 * LITTLE_ENDIAN**. It is designed for binary formats that standardize on LE encoding and want to
 * avoid accidental BE operations when working with the JDK NIO buffers.
 *
 * ### Key properties
 * - **Side-effect free wrap:** [wrap] does not mutate the source buffer's byte order; it wraps a
 *   `duplicate()` with LE order applied, so external code using the original buffer is not affected.
 * - **View creation is always LE:** [duplicate], [slice], [slice(at, len)], [asReadOnly], and the
 *   exported `ByteBuffer` views ([asReadOnlyByteBuffer], [toMutableByteBuffer]) all apply
 *   `order(LITTLE_ENDIAN)` **as the last step**.
 * - **Zero/low overhead:** Declared as a `@JvmInline value class`; hot accessors are `inline` to
 *   enable the JIT to elide call overhead in critical paths.
 * - **Safety helpers:** [requireRemaining] for bounds checks, [assertLittleEndian] for debug time
 *   assertions, and [checkedPutInt] as a sample “read-only aware” write helper.
 *
 * ### Concurrency
 * Mirrors [ByteBuffer]'s thread-unsafety: instances and their views are **not** thread-safe for
 * concurrent mutation. Coordinate access externally.
 *
 * ### Usage
 * ```
 * val lb = LByteBuffer.allocate(1024, direct = true)
 * lb.putInt(0xABCD).putLong(42L).flip()
 * val x = lb.getInt()
 * val y = lb.getLong()
 * ```
 *
 * @constructor Use factory methods [wrap] or [allocate]. The primary constructor is private.
 * @since 1.0
 */
@JvmInline
value class LByteBuffer private constructor(
    /**
     * The underlying buffer. Marked as `internal` + `@PublishedApi` so public inline members can
     * legally reference it under Kotlin explicit API rules.
     */
    @PublishedApi internal val b: ByteBuffer
) {

    companion object {
        /**
         * Wraps an existing [ByteBuffer] **without mutating** its state for the caller.
         *
         * Internally performs `buf.duplicate()` and applies `order(LITTLE_ENDIAN)` to the duplicate,
         * ensuring a LE view while keeping the original buffer untouched.
         *
         * @param buf the source buffer to wrap
         * @return an [LByteBuffer] whose underlying buffer is a LE-ordered duplicate of [buf]
         */
        fun wrap(buf: ByteBuffer): LByteBuffer {
            val v = buf.duplicate()
            v.order(ByteOrder.LITTLE_ENDIAN)
            return LByteBuffer(v)
        }

        /**
         * Allocates a new LE-ordered buffer via the provided [allocator].
         *
         * By default this uses [ByteBufferAllocator.Default] (JDK heap/direct).
         * You can plug in a pool-backed allocator with zero overhead for the default case.
         *
         * @param capacity capacity in bytes (>= 0)
         * @param direct whether to prefer a direct buffer
         * @param allocator strategy to allocate the underlying buffer
         */
        fun allocate(
            capacity: Int,
            direct: Boolean = false,
            allocator: ByteBufferAllocator = ByteBufferAllocator.Default
        ): LByteBuffer {
            require(capacity >= 0) { "capacity < 0: $capacity" }
            val bb = allocator.allocate(capacity, direct)
            bb.order(ByteOrder.LITTLE_ENDIAN)
            return LByteBuffer(bb)
        }
    }

    /* ===================== Basic properties ===================== */

    /** The underlying capacity, delegated to [ByteBuffer.capacity]. */
    inline val capacity: Int get() = b.capacity()

    /** The current position, delegated to [ByteBuffer.position]. */
    inline val position: Int get() = b.position()

    /** The current limit, delegated to [ByteBuffer.limit]. */
    inline val limit: Int get() = b.limit()

    /** Bytes remaining between position and limit, delegated to [ByteBuffer.remaining]. */
    inline val remaining: Int get() = b.remaining()

    /** Whether this buffer is read-only, delegated to [ByteBuffer.isReadOnly]. */
    inline val isReadOnly: Boolean get() = b.isReadOnly

    /** Whether there are remaining bytes, delegated to [ByteBuffer.hasRemaining]. */
    inline fun hasRemaining(): Boolean = b.hasRemaining()

    /* ===================== Positioning ===================== */

    /**
     * Sets the position.
     * @return `this` for chaining
     * @throws IllegalArgumentException if the new position is invalid
     */
    inline fun position(newPos: Int): LByteBuffer {
        b.position(newPos); return this
    }

    /**
     * Sets the limit.
     * @return `this` for chaining
     * @throws IllegalArgumentException if the new limit is invalid
     */
    inline fun limit(newLimit: Int): LByteBuffer {
        b.limit(newLimit); return this
    }

    /** Clears the buffer (pos=0, limit=capacity). */
    inline fun clear(): LByteBuffer {
        b.clear(); return this
    }

    /** Flips the buffer for reading (limit=pos, pos=0). */
    inline fun flip(): LByteBuffer {
        b.flip(); return this
    }

    /** Rewinds the buffer (pos=0, limit unchanged). */
    inline fun rewind(): LByteBuffer {
        b.rewind(); return this
    }

    /** Marks the current position. */
    inline fun mark(): LByteBuffer {
        b.mark(); return this
    }

    /** Resets to the last mark. */
    inline fun reset(): LByteBuffer {
        b.reset(); return this
    }

    /**
     * Advances the position by [n] bytes.
     * @return `this` for chaining
     * @throws IllegalArgumentException if the resulting position would be invalid
     */
    inline fun advance(n: Int): LByteBuffer {
        b.position(b.position() + n); return this
    }

    /* ===================== Views (LE enforced last) ===================== */

    /**
     * Creates a **mutable** duplicate view with LE order enforced.
     * Position, limit, and mark are copied per [ByteBuffer.duplicate].
     */
    fun duplicate(): LByteBuffer {
        val v = b.duplicate()
        v.order(ByteOrder.LITTLE_ENDIAN)
        return LByteBuffer(v)
    }

    /**
     * Creates a **read-only** duplicate view with LE order enforced.
     * Position, limit, and mark are copied per [ByteBuffer.asReadOnlyBuffer].
     */
    fun asReadOnly(): LByteBuffer {
        val v = b.asReadOnlyBuffer()
        v.order(ByteOrder.LITTLE_ENDIAN)
        return LByteBuffer(v)
    }

    /**
     * Slices the remaining range `[position, limit)` with LE order enforced.
     * The resulting view has independent position/limit starting at 0..remaining.
     */
    fun slice(): LByteBuffer {
        val v = b.slice()
        v.order(ByteOrder.LITTLE_ENDIAN)
        return LByteBuffer(v)
    }

    /**
     * Returns the underlying byte array.
     *
     * @throws UnsupportedOperationException if the underlying buffer is not backed by an array
     * @throws ReadOnlyBufferException if the underlying buffer is read-only
     */
    fun array(): ByteArray {
        return b.array()
    }

    /**
     * Slices an arbitrary range `[at, at+len)` with LE order enforced.
     *
     * Internally duplicates, positions/limits to the requested window, then slices to obtain a
     * compact 0..len view.
     *
     * @throws IllegalArgumentException if the requested range is out of bounds
     */
    fun slice(at: Int, len: Int): LByteBuffer {
        require(at >= 0 && len >= 0 && at + len <= capacity) { "slice range out of bounds" }
        val v = b.duplicate().order(ByteOrder.LITTLE_ENDIAN)
        v.position(at).limit(at + len)
        val s = v.slice()
        s.order(ByteOrder.LITTLE_ENDIAN)
        return LByteBuffer(s)
    }

    /**
     * Returns a **read-only** `ByteBuffer` view with LE order enforced.
     *
     * This is suitable for passing into APIs that accept `ByteBuffer` while preserving RO semantics.
     */
    fun asReadOnlyByteBuffer(): ByteBuffer {
        val v = b.asReadOnlyBuffer()
        v.order(ByteOrder.LITTLE_ENDIAN)
        return v
    }

    /**
     * Returns a **mutable** `ByteBuffer` view with LE order enforced.
     *
     * The returned buffer is a `duplicate()`; mutating its position/limit will not affect other
     * views unless they share the same underlying content semantics as per NIO.
     */
    fun toMutableByteBuffer(): ByteBuffer {
        val v = b.duplicate()
        v.order(ByteOrder.LITTLE_ENDIAN)
        return v
    }

    /**
     * Returns the underlying byte array if available; otherwise returns a copy of the remaining
     * bytes.
     *
     * Note that this may allocate a new array if the underlying buffer is not backed by an array.
     * Use [hasArray] to check if `array()` is safe to call without catching exceptions.
     *
     * The returned array always contains exactly the remaining bytes from position to limit.
     * The position of this buffer is not modified.
     */
    fun hasArray(): Boolean = b.hasArray()

    /* ===================== Relative get/put (LE) ===================== */

    /** Puts all bytes from [src]. */
    inline fun put(src: ByteArray): LByteBuffer {
        b.put(src); return this
    }

    /** Puts [len] bytes from [src] starting at [off]. */
    inline fun put(src: ByteArray, off: Int, len: Int): LByteBuffer {
        b.put(src, off, len); return this
    }

    /** Gets into [dst] filling it completely. */
    inline fun get(dst: ByteArray): LByteBuffer {
        b.get(dst); return this
    }

    /** Gets [len] bytes into [dst] starting at [off]. */
    inline fun get(dst: ByteArray, off: Int, len: Int): LByteBuffer {
        b.get(dst, off, len); return this
    }

    /** Puts remaining bytes from [src]. */
    inline fun put(src: ByteBuffer): LByteBuffer {
        b.put(src); return this
    }

    /** Gets one byte. */
    inline fun get(): Byte = b.get()

    /** Puts one byte. */
    inline fun put(v: Byte): LByteBuffer {
        b.put(v); return this
    }

    /** Gets a little-endian `short`. */
    inline fun getShort(): Short = b.getShort()

    /** Puts a little-endian `short`. */
    inline fun putShort(v: Short): LByteBuffer {
        b.putShort(v); return this
    }

    /** Gets a little-endian `int`. */
    inline fun getInt(): Int = b.getInt()

    /** Puts a little-endian `int`. */
    inline fun putInt(v: Int): LByteBuffer {
        b.putInt(v); return this
    }

    /** Gets a little-endian `long`. */
    inline fun getLong(): Long = b.getLong()

    /** Puts a little-endian `long`. */
    inline fun putLong(v: Long): LByteBuffer {
        b.putLong(v); return this
    }

    /** Gets a little-endian `float`. */
    inline fun getFloat(): Float = b.getFloat()

    /** Puts a little-endian `float`. */
    inline fun putFloat(v: Float): LByteBuffer {
        b.putFloat(v); return this
    }

    /** Gets a little-endian `double`. */
    inline fun getDouble(): Double = b.getDouble()

    /** Puts a little-endian `double`. */
    inline fun putDouble(v: Double): LByteBuffer {
        b.putDouble(v); return this
    }

    /* ===================== Absolute get/put (LE) ===================== */

    /** Gets a little-endian `short` at absolute [at]. */
    inline fun getShort(at: Int): Short = b.getShort(at)

    /** Puts a little-endian `short` at absolute [at]. */
    inline fun putShort(at: Int, v: Short): LByteBuffer {
        b.putShort(at, v); return this
    }

    /** Gets a little-endian `int` at absolute [at]. */
    inline fun getInt(at: Int): Int = b.getInt(at)

    /** Puts a little-endian `int` at absolute [at]. */
    inline fun putInt(at: Int, v: Int): LByteBuffer {
        b.putInt(at, v); return this
    }

    /** Gets a little-endian `long` at absolute [at]. */
    inline fun getLong(at: Int): Long = b.getLong(at)

    /** Puts a little-endian `long` at absolute [at]. */
    inline fun putLong(at: Int, v: Long): LByteBuffer {
        b.putLong(at, v); return this
    }

    /* ===================== Diagnostics & guards ===================== */

    /**
     * Asserts at runtime (typically in debug builds) that the underlying order is LE.
     * @throws IllegalStateException if the underlying view is not LE
     */
    fun assertLittleEndian(): LByteBuffer {
        check(b.order() == ByteOrder.LITTLE_ENDIAN) { "Non-LE view detected" }
        return this
    }

    /**
     * Ensures that at least [n] bytes remain; useful in decoders before bulk reads.
     * @throws IllegalArgumentException if [n] is negative or exceeds [remaining]
     */
    inline fun requireRemaining(n: Int): LByteBuffer {
        require(n >= 0 && remaining >= n) { "required=$n, remaining=$remaining" }
        return this
    }
}

/* ===================== Convenience extensions ===================== */

/**
 * Writes a length-prefixed UTF-8 string as:
 * `int (LE, number of bytes)` followed by raw UTF-8 bytes.
 *
 * The length is stored as a signed 32-bit integer; callers should enforce any
 * application-specific maximum length prior to calling this method.
 *
 * @param s the string to encode as UTF-8
 * @return `this` for chaining
 * @throws java.nio.BufferOverflowException if insufficient space remains
 */
inline fun LByteBuffer.putStringLE(s: String): LByteBuffer {
    val bytes = s.encodeToByteArray() // UTF-8
    putInt(bytes.size)
    return apply { this@putStringLE.put(bytes) }
}

/**
 * Reads a length-prefixed UTF-8 string written by [putStringLE].
 *
 * Validates that the length is non-negative and that enough bytes remain.
 *
 * @return the decoded string
 * @throws IllegalArgumentException if the stored length is negative
 * @throws java.nio.BufferUnderflowException if not enough bytes remain
 */
inline fun LByteBuffer.getStringLE(): String {
    val len = getInt()
    require(len >= 0) { "negative length: $len" }
    requireRemaining(len)
    val arr = ByteArray(len)
    get(arr)
    return arr.decodeToString()
}

/**
 * Example of a read-only aware writer: throws [ReadOnlyBufferException] if this
 * view is read-only before attempting to write.
 *
 * @return `this` for chaining
 * @throws ReadOnlyBufferException if the underlying buffer is read-only
 */
inline fun LByteBuffer.checkedPutInt(v: Int): LByteBuffer {
    if (isReadOnly) throw ReadOnlyBufferException()
    return putInt(v)
}
