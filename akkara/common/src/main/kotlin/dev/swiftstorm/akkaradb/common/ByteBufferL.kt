@file:Suppress("NOTHING_TO_INLINE", "unused")

package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.ByteBufferL.Companion.allocate
import dev.swiftstorm.akkaradb.common.ByteBufferL.Companion.wrap
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.LongBuffer
import java.nio.ReadOnlyBufferException
import kotlin.math.min

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
 * val lb = ByteBufferL.allocate(1024, direct = true)
 * lb.putInt(0xABCD).putLong(42L).flip()
 * val x = lb.getInt()
 * val y = lb.getLong()
 * ```
 *
 * @constructor Use factory methods [wrap] or [allocate]. The primary constructor is private.
 * @since 1.0
 */
@JvmInline
value class ByteBufferL private constructor(
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
         * @return an [ByteBufferL] whose underlying buffer is a LE-ordered duplicate of [buf]
         */
        fun wrap(buf: ByteBuffer): ByteBufferL {
            buf.order(ByteOrder.LITTLE_ENDIAN)
            return ByteBufferL(buf)
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
            direct: Boolean = true,
            allocator: ByteBufferAllocator = ByteBufferAllocator.Default
        ): ByteBufferL {
            require(capacity >= 0) { "capacity < 0: $capacity" }
            val bb = allocator.allocate(capacity, direct)
            bb.order(ByteOrder.LITTLE_ENDIAN)
            return ByteBufferL(bb)
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

    inline val short: Short get() = b.getShort()

    inline val int: Int get() = b.getInt()

    inline val long: Long get() = b.getLong()

    inline val float: Float get() = b.getFloat()

    inline val double: Double get() = b.getDouble()

    /** Whether there are remaining bytes, delegated to [ByteBuffer.hasRemaining]. */
    inline fun hasRemaining(): Boolean = b.hasRemaining()

    /* ===================== Positioning ===================== */

    /**
     * Sets the position.
     * @return `this` for chaining
     * @throws IllegalArgumentException if the new position is invalid
     */
    inline fun position(newPos: Int): ByteBufferL {
        b.position(newPos); return this
    }

    /**
     * Sets the limit.
     * @return `this` for chaining
     * @throws IllegalArgumentException if the new limit is invalid
     */
    inline fun limit(newLimit: Int): ByteBufferL {
        b.limit(newLimit); return this
    }

    /** Clears the buffer (pos=0, limit=capacity). */
    inline fun clear(): ByteBufferL {
        b.clear(); return this
    }

    /** Flips the buffer for reading (limit=pos, pos=0). */
    inline fun flip(): ByteBufferL {
        b.flip(); return this
    }

    /** Rewinds the buffer (pos=0, limit unchanged). */
    inline fun rewind(): ByteBufferL {
        b.rewind(); return this
    }

    /** Marks the current position. */
    inline fun mark(): ByteBufferL {
        b.mark(); return this
    }

    /** Resets to the last mark. */
    inline fun reset(): ByteBufferL {
        b.reset(); return this
    }

    /**
     * Advances the position by [n] bytes.
     * @return `this` for chaining
     * @throws IllegalArgumentException if the resulting position would be invalid
     */
    inline fun advance(n: Int): ByteBufferL {
        val p = b.position()
        val newPos = p + n
        require(newPos in 0..b.limit()) {
            "advance out of bounds: pos=$p, n=$n, newPos=$newPos, limit=${b.limit()}"
        }
        b.position(newPos)
        return this
    }

    /* ===================== Views (LE enforced last) ===================== */

    /**
     * Creates a **mutable** duplicate view with LE order enforced.
     * Position, limit, and mark are copied per [ByteBuffer.duplicate].
     */
    fun duplicate(): ByteBufferL {
        val v = b.duplicate()
        v.order(ByteOrder.LITTLE_ENDIAN)
        return ByteBufferL(v)
    }

    /**
     * Creates a **read-only** duplicate view with LE order enforced.
     * Position, limit, and mark are copied per [ByteBuffer.asReadOnlyBuffer].
     */
    fun asReadOnly(): ByteBufferL {
        val v = b.asReadOnlyBuffer()
        v.order(ByteOrder.LITTLE_ENDIAN)
        return ByteBufferL(v)
    }

    /**
     * Slices the remaining range `[position, limit)` with LE order enforced.
     * The resulting view has independent position/limit starting at 0..remaining.
     */
    fun slice(): ByteBufferL {
        val v = b.slice()
        v.order(ByteOrder.LITTLE_ENDIAN)
        return ByteBufferL(v)
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
     * Compares remaining bytes with [other] and returns the index of the first mismatch,
     * or `-1` if all remaining bytes are equal.
     *
     * Both buffers' positions are unchanged.
     *
     * @throws IllegalArgumentException if either buffer has no remaining bytes
     */
    fun mismatch(other: ByteBufferL): Int {
        return b.mismatch(other.b)
    }

    /**
     * Returns `[array, offset, length]` for the current `[position, limit)` **without copy**,
     * or `null` if the buffer has no accessible array.
     *
     * If non-null, the returned triple maps exactly to the *remaining* bytes.
     */
    fun arraySliceOrNull(): Triple<ByteArray, Int, Int>? {
        if (!b.hasArray()) return null
        val arr = b.array() // may throw if read-only; hasArray() が true なら基本OK
        val off = b.arrayOffset() + b.position()
        val len = b.remaining()
        return Triple(arr, off, len)
    }

    /**
     * Returns the array offset of the underlying buffer.
     *
     * @throws UnsupportedOperationException if the underlying buffer is not backed by an array
     */
    fun arrayOffset(): Int {
        if (!b.hasArray()) throw UnsupportedOperationException("No accessible array")
        return b.arrayOffset()
    }

    /**
     * Copies the current `[position, limit)` into a new [ByteArray] and returns it.
     * Works for both heap/direct and read-only buffers.
     */
    fun toByteArrayRemaining(): ByteArray {
        val out = ByteArray(remaining)
        val tmp = b.duplicate()
        tmp.get(out)
        return out
    }

    /**
     * Slices an arbitrary range `[at, at+len)` with LE order enforced.
     *
     * Internally duplicates, positions/limits to the requested window, then slices to obtain a
     * compact 0..len view.
     *
     * @throws IllegalArgumentException if the requested range is out of bounds
     */
    fun slice(at: Int, len: Int): ByteBufferL {
        require(at >= 0 && len >= 0 && at + len <= capacity) {
            "slice range out of bounds: at=$at, len=$len, cap=$capacity"
        }
        val v = b.duplicate()
        v.position(at).limit(at + len)
        val s = v.slice()
        s.order(ByteOrder.LITTLE_ENDIAN)
        return ByteBufferL(s)
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

    /** Whether the underlying buffer exposes a backing array. */
    fun hasArray(): Boolean = b.hasArray()

    /* ===================== Relative get/put (LE) ===================== */

    /** Puts all bytes from [src]. */
    inline fun put(src: ByteArray): ByteBufferL {
        b.put(src); return this
    }

    /** Puts [len] bytes from [src] starting at [off]. */
    inline fun put(src: ByteArray, off: Int, len: Int): ByteBufferL {
        b.put(src, off, len); return this
    }

    /** Gets into [dst] filling it completely. */
    inline fun get(dst: ByteArray): ByteBufferL {
        b.get(dst); return this
    }

    /** Gets [len] bytes into [dst] starting at [off]. */
    inline fun get(dst: ByteArray, off: Int, len: Int): ByteBufferL {
        b.get(dst, off, len); return this
    }

    /**
     * Transfers bytes from **this** to [dst] up to `min(this.remaining, dst.remaining)`.
     * Both buffers' positions advance accordingly. No overflow is thrown.
     */
    fun get(dst: ByteBuffer): ByteBufferL {
        val n = min(remaining, dst.remaining())
        if (n == 0) return this
        val tmp = b.duplicate()
        tmp.limit(tmp.position() + n)
        dst.put(tmp)
        b.position(b.position() + n)
        return this
    }

    /** Puts remaining bytes from [src]. */
    inline fun put(src: ByteBuffer): ByteBufferL {
        b.put(src); return this
    }

    /** Puts remaining bytes from [src]. */
    inline fun put(src: ByteBufferL): ByteBufferL {
        b.put(src.b); return this
    }

    /** Gets one byte. */
    inline fun get(): Byte = b.get()

    /** Puts one byte. */
    inline fun put(v: Byte): ByteBufferL {
        b.put(v); return this
    }

    /** Puts a little-endian `short`. */
    inline fun putShort(v: Short): ByteBufferL {
        b.putShort(v); return this
    }

    /** Puts a little-endian `int`. */
    inline fun putInt(v: Int): ByteBufferL {
        b.putInt(v); return this
    }

    inline fun putInts(src: IntArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        require(off >= 0 && len >= 0 && off + len <= src.size) { "bad range" }
        require((b.position() and 3) == 0) { "position must be 4-byte aligned" }
        requireRemaining(len * 4)

        val view = b.duplicate()
        val start = view.position()
        view.limit(start + len * 4)
        val ib: java.nio.IntBuffer = view.slice().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()

        ib.put(src, off, len)

        b.position(start + len * 4)
        return this
    }

    /** Puts a little-endian `long`. */
    inline fun putLong(v: Long): ByteBufferL {
        b.putLong(v); return this
    }

    /** Puts multiple little-endian `long`s from [src] starting at [off] for [len] elements. */
    inline fun putLongs(src: LongArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        require(off >= 0 && len >= 0 && off + len <= src.size) { "bad range" }
        require((b.position() and 7) == 0) { "position must be 8-byte aligned" }
        requireRemaining(len * 8)

        val view = b.duplicate()
        val start = view.position()
        view.limit(start + len * 8)
        val lb: LongBuffer = view.slice().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer()

        lb.put(src, off, len)

        b.position(start + len * 8)
        return this
    }

    /** Puts a little-endian `float`. */
    inline fun putFloat(v: Float): ByteBufferL {
        b.putFloat(v); return this
    }

    /** Puts a little-endian `double`. */
    inline fun putDouble(v: Double): ByteBufferL {
        b.putDouble(v); return this
    }

    /** Compacts the buffer (moves remaining bytes to the beginning). */
    inline fun compact(): ByteBufferL {
        b.compact(); return this
    }

    /** Puts all remaining bytes from [src]. */
    inline fun putAll(src: ByteBufferL): ByteBufferL {
        require(b !== src.b) { "Cannot copy from self (same underlying buffer)" }
        return put(src.asReadOnlyByteBuffer().slice())
    }

    /** Gets one byte at absolute [at]. */
    inline fun get(at: Int): Byte = b.get(at)

    inline fun put(at: Int, v: Byte): ByteBufferL {
        b.put(at, v); return this
    }

    /* ===================== Absolute get/put (LE) ===================== */

    /** Gets a little-endian `short` at absolute [at]. */
    inline fun getShort(at: Int): Short = b.getShort(at)

    /** Puts a little-endian `short` at absolute [at]. */
    inline fun putShort(at: Int, v: Short): ByteBufferL {
        b.putShort(at, v); return this
    }

    /** Gets a little-endian `int` at absolute [at]. */
    inline fun getInt(at: Int): Int = b.getInt(at)

    /** Puts a little-endian `int` at absolute [at]. */
    inline fun putInt(at: Int, v: Int): ByteBufferL {
        b.putInt(at, v); return this
    }

    /** Gets a little-endian `long` at absolute [at]. */
    inline fun getLong(at: Int): Long = b.getLong(at)

    /** Puts a little-endian `long` at absolute [at]. */
    inline fun putLong(at: Int, v: Long): ByteBufferL {
        b.putLong(at, v); return this
    }

    /* ===================== Diagnostics & guards ===================== */

    /**
     * Asserts at runtime (typically in debug builds) that the underlying order is LE.
     * @throws IllegalStateException if the underlying view is not LE
     */
    fun assertLittleEndian(): ByteBufferL {
        check(b.order() == ByteOrder.LITTLE_ENDIAN) { "Non-LE view detected" }
        return this
    }

    /**
     * Ensures that at least [n] bytes remain; useful in decoders before bulk reads.
     * @throws IllegalArgumentException if [n] is negative or exceeds [remaining]
     */
    inline fun requireRemaining(n: Int): ByteBufferL {
        require(n >= 0 && remaining >= n) { "required=$n, remaining=$remaining" }
        return this
    }
}

operator fun ByteBufferL.compareTo(other: ByteBufferL): Int = compareLexUnsigned(other)
operator fun ByteBufferL.compareTo(other: ByteBuffer): Int = compareLexUnsigned(wrap(other))

fun ByteBufferL.compareLexUnsigned(other: ByteBufferL): Int {
    val a = this.asReadOnlyByteBuffer().slice()
    val b = other.asReadOnlyByteBuffer().slice()

    val n = minOf(a.remaining(), b.remaining())
    val mm = a.mismatch(b)

    return when (mm) {
        -1 -> 0
        n -> a.remaining().compareTo(b.remaining())
        else -> {
            val av = a.get(mm).toInt() and 0xFF
            val bv = b.get(mm).toInt() and 0xFF
            av.compareTo(bv)
        }
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
 * @param maxBytes a safety cap to avoid oversized allocations (default: unlimited)
 * @return `this` for chaining
 * @throws java.nio.BufferOverflowException if insufficient space remains
 * @throws IllegalArgumentException if [s] exceeds [maxBytes]
 */
inline fun ByteBufferL.putStringLE(
    s: String,
    maxBytes: Int = Int.MAX_VALUE
): ByteBufferL {
    val bytes = s.encodeToByteArray() // UTF-8
    require(bytes.size <= maxBytes) { "string too long: ${bytes.size} > $maxBytes" }
    putInt(bytes.size)
    return put(bytes)
}

/**
 * Reads a length-prefixed UTF-8 string written by [putStringLE].
 *
 * Validates that the length is non-negative and that enough bytes remain.
 *
 * @param maxBytes safety cap for the stored byte length (default: 1 MiB)
 * @return the decoded string
 * @throws IllegalArgumentException if the stored length is negative or exceeds [maxBytes]
 * @throws java.nio.BufferUnderflowException if not enough bytes remain
 */
inline fun ByteBufferL.getStringLE(maxBytes: Int = 1 shl 20): String {
    val len = int
    require(len >= 0 && len <= maxBytes) { "invalid length: $len (max=$maxBytes)" }
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
inline fun ByteBufferL.checkedPutInt(v: Int): ByteBufferL {
    if (isReadOnly) throw ReadOnlyBufferException()
    return putInt(v)
}

/**
 * Advances the position to the next multiple of [boundary].
 *
 * The [boundary] must be a positive power of two.
 * If the position is already aligned, no action is taken.
 * If padding is needed, zero bytes are written to fill the gap.
 *
 * @param boundary the alignment boundary (must be > 0 and a power of two)
 * @return `this` for chaining
 * @throws IllegalArgumentException if [boundary] is not a positive power of two
 * @throws java.nio.BufferOverflowException if there is insufficient space to write padding
 */
inline fun ByteBufferL.align(boundary: Int): ByteBufferL {
    require(boundary > 0 && (boundary and (boundary - 1)) == 0) { "power-of-two required" }
    val p = position
    val pad = (-p) and (boundary - 1)
    if (pad != 0) {
        requireRemaining(pad)
        repeat(pad) { put(0) }
    }
    return this
}

fun ByteBufferL.getByteBuffer(): ByteBuffer = b

inline fun <R> ByteBufferL.withPosition(pos: Int, block: (ByteBufferL) -> R): R {
    val saved = position
    try {
        position(pos); return block(this)
    } finally {
        position(saved)
    }
}

fun ByteBufferL.debugHex(): String {
    val bb = this.duplicate().apply { rewind() }.asReadOnlyByteBuffer()
    val sb = StringBuilder()
    while (bb.hasRemaining()) {
        sb.append(String.format("%02X ", bb.get()))
    }
    return sb.toString()
}
