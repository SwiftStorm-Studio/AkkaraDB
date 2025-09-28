@file:Suppress("NOTHING_TO_INLINE", "unused")

package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.ByteBufferL.Companion.wrap
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * # ByteBufferL — Little‑Endian enforcing wrapper for NIO ByteBuffer (Java 17)
 *
 * A small, allocation‑free façade over [ByteBuffer] that **always operates in LITTLE_ENDIAN**.
 *
 * ## Design goals
 * - **No hidden allocations** on the hot path (no temporary arrays / views unless explicitly asked).
 * - **Never mutate external buffers**: [wrap] duplicates first, then applies LE.
 * - **Java 17 friendly**: relies on NIO intrinsics (absolute `getInt/putInt`,
 *   `System.arraycopy` via bulk methods, `asIntBuffer/asLongBuffer` for batched writes).
 * - **Safe and predictable**: every view created by this class has LE order **re‑asserted**.
 *
 * ## When to use
 * - Binary formats and on‑disk structures that are standardized on **little‑endian**.
 * - I/O pipelines where you want a **single direct buffer** to be reused and passed to
 *   channels without round‑trips through heap arrays.
 *
 * ## Performance notes (Java 17)
 * - Prefer **absolute accessors** (`getInt(at)`, `putLong(at, v)`) in tight loops; this pattern
 *   composes well with HotSpot BCE and loop optimizations.
 * - Keep **view creation** (`duplicate/slice`) out of inner loops; create once and reuse.
 * - For bulk primitives, `asIntBuffer()/asLongBuffer()` is efficient when offsets are aligned.
 * - Do not ping‑pong data between heap arrays and direct buffers—batch copies when necessary.
 */
class ByteBufferL private constructor(
    @PublishedApi internal val buf: ByteBuffer
) {
    /* ============================= Factory ============================= */

    companion object {
        private val LE: ByteOrder = ByteOrder.LITTLE_ENDIAN

        /**
         * Wraps [src] *without* mutating it: duplicates then applies LE.
         */
        @JvmStatic
        fun wrap(src: ByteBuffer): ByteBufferL {
            val dup = src.duplicate()
            dup.order(LE)
            return ByteBufferL(dup)
        }

        /**
         * Allocates a new LE‑ordered buffer (heap or direct) and wraps it.
         */
        @JvmStatic
        fun allocate(capacity: Int, direct: Boolean = true): ByteBufferL {
            require(capacity >= 0) { "capacity < 0: $capacity" }
            val b = if (direct) ByteBuffer.allocateDirect(capacity) else ByteBuffer.allocate(capacity)
            b.order(LE)
            return ByteBufferL(b)
        }
    }

    /* ========================== Basic properties ========================== */

    inline val capacity: Int get() = buf.capacity()
    inline val position: Int get() = buf.position()
    inline val limit: Int get() = buf.limit()
    inline val remaining: Int get() = buf.remaining()
    inline val isReadOnly: Boolean get() = buf.isReadOnly

    /* ============================= Cursor ops ============================= */

    inline fun position(newPos: Int): ByteBufferL {
        buf.position(newPos); return this
    }

    inline fun limit(newLimit: Int): ByteBufferL {
        buf.limit(newLimit); return this
    }

    inline fun clear(): ByteBufferL {
        buf.clear(); return this
    }

    inline fun flip(): ByteBufferL {
        buf.flip(); return this
    }

    inline fun rewind(): ByteBufferL {
        buf.rewind(); return this
    }

    inline fun compact(): ByteBufferL {
        buf.compact(); return this
    }

    /** Moves the position forward by [n] bytes (bounds‑checked). */
    fun advance(n: Int): ByteBufferL {
        val p = buf.position()
        val np = p + n
        require(np in 0..buf.limit()) { "advance out of bounds: pos=$p, n=$n, newPos=$np, limit=${buf.limit()}" }
        buf.position(np); return this
    }

    /* =============================== Views =============================== */

    /** Mutable duplicate; LE order is re‑asserted. */
    fun duplicate(): ByteBufferL {
        val v = buf.duplicate(); v.order(LE); return ByteBufferL(v)
    }

    /** Read‑only duplicate; LE order is re‑asserted. */
    fun asReadOnly(): ByteBufferL {
        val v = buf.asReadOnlyBuffer(); v.order(LE); return ByteBufferL(v)
    }

    /** Slice of remaining region; LE order is re‑asserted. */
    fun slice(): ByteBufferL {
        val v = buf.slice(); v.order(LE); return ByteBufferL(v)
    }

    /** Slice of `[at, at+len)`; LE order is re‑asserted. */
    fun slice(at: Int, len: Int): ByteBufferL {
        require(at >= 0 && len >= 0 && at + len <= buf.capacity()) { "slice OOB: at=$at, len=$len, cap=${buf.capacity()}" }
        val v = buf.duplicate()
        v.position(at).limit(at + len)
        val s = v.slice(); s.order(LE)
        return ByteBufferL(s)
    }

    /** A read‑only `ByteBuffer` view (LE). */
    fun asReadOnlyByteBuffer(): ByteBuffer {
        val v = buf.asReadOnlyBuffer(); v.order(LE); return v
    }

    /** A mutable `ByteBuffer` view (LE). */
    fun toMutableByteBuffer(): ByteBuffer {
        val v = buf.duplicate(); v.order(LE); return v
    }

    /* =========================== Bulk transfers =========================== */

    /** Puts all remaining bytes from [src]. */
    inline fun put(src: ByteBuffer): ByteBufferL {
        buf.put(src); return this
    }

    /** Puts all bytes of [src]. */
    inline fun put(src: ByteArray): ByteBufferL {
        buf.put(src); return this
    }

    /** Puts [len] bytes from [src] starting at [off]. */
    inline fun put(src: ByteArray, off: Int, len: Int): ByteBufferL {
        buf.put(src, off, len); return this
    }

    /** Gets into [dst] exactly [dst].size bytes. */
    inline fun get(dst: ByteArray): ByteBufferL {
        buf.get(dst); return this
    }

    /** Gets [len] bytes into [dst] starting at [off]. */
    inline fun get(dst: ByteArray, off: Int, len: Int): ByteBufferL {
        buf.get(dst, off, len); return this
    }

    /**
     * Transfers up to `min(this.remaining, dst.remaining)` bytes from this buffer to [dst].
     * Positions of both advance accordingly.
     */
    fun transferTo(dst: ByteBuffer): ByteBufferL {
        val n = remaining.coerceAtMost(dst.remaining())
        if (n == 0) return this
        val tmp = buf.duplicate(); tmp.limit(tmp.position() + n)
        dst.put(tmp); buf.position(buf.position() + n)
        return this
    }

    /* ======================= Relative primitive I/O ======================= */

    var byte: Byte
        get() = buf.get()
        set(v) {
            buf.put(v)
        }

    var short: Short
        get() = buf.getShort()
        set(v) {
            buf.putShort(v)
        }

    var int: Int
        get() = buf.getInt()
        set(v) {
            buf.putInt(v)
        }

    var long: Long
        get() = buf.getLong()
        set(v) {
            buf.putLong(v)
        }

    var float: Float
        get() = buf.getFloat()
        set(v) {
            buf.putFloat(v)
        }

    var double: Double
        get() = buf.getDouble()
        set(v) {
            buf.putDouble(v)
        }

    /* ======================= Absolute primitive I/O ======================= */

    inline fun get(at: Int): Byte = buf.get(at)
    inline fun put(at: Int, v: Byte): ByteBufferL {
        buf.put(at, v); return this
    }

    inline fun getShort(at: Int): Short = buf.getShort(at)
    inline fun putShort(at: Int, v: Short): ByteBufferL {
        buf.putShort(at, v); return this
    }

    inline fun getInt(at: Int): Int = buf.getInt(at)
    inline fun putInt(at: Int, v: Int): ByteBufferL {
        buf.putInt(at, v); return this
    }

    inline fun getLong(at: Int): Long = buf.getLong(at)
    inline fun putLong(at: Int, v: Long): ByteBufferL {
        buf.putLong(at, v); return this
    }

    inline fun getFloat(at: Int): Float = buf.getFloat(at)
    inline fun putFloat(at: Int, v: Float): ByteBufferL {
        buf.putFloat(at, v); return this
    }

    inline fun getDouble(at: Int): Double = buf.getDouble(at)
    inline fun putDouble(at: Int, v: Double): ByteBufferL {
        buf.putDouble(at, v); return this
    }

    /* ======================== Batched primitive put ======================== */

    /**
     * Writes [len] ints from [src] starting at [off]. Requires 4‑byte alignment of current position.
     */
    fun putInts(src: IntArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        require(off >= 0 && len >= 0 && off + len <= src.size) { "bad range" }
        require((buf.position() and 3) == 0) { "position must be 4‑byte aligned" }
        requireRemaining(len * 4)

        val v = buf.duplicate()
        val start = v.position(); v.limit(start + len * 4)
        val ib = v.slice().order(LE).asIntBuffer()
        ib.put(src, off, len)
        buf.position(start + len * 4)
        return this
    }

    /**
     * Writes [len] longs from [src] starting at [off]. Requires 8‑byte alignment of current position.
     */
    fun putLongs(src: LongArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        require(off >= 0 && len >= 0 && off + len <= src.size) { "bad range" }
        require((buf.position() and 7) == 0) { "position must be 8‑byte aligned" }
        requireRemaining(len * 8)

        val v = buf.duplicate()
        val start = v.position(); v.limit(start + len * 8)
        val lb = v.slice().order(LE).asLongBuffer()
        lb.put(src, off, len)
        buf.position(start + len * 8)
        return this
    }

    /* ============================ Utilities ============================ */

    /** Ensures at least [n] bytes remain. */
    fun requireRemaining(n: Int): ByteBufferL {
        require(n >= 0 && remaining >= n) { "required=$n, remaining=$remaining" }
        return this
    }

    /** Asserts that underlying order is LE (helpful in tests / debug builds). */
    fun assertLittleEndian(): ByteBufferL {
        check(buf.order() == LE) { "Non‑LE view detected" }
        return this
    }

    /** Aligns position to the next multiple of [boundary] by writing zero padding. */
    fun align(boundary: Int): ByteBufferL {
        require(boundary > 0 && (boundary and (boundary - 1)) == 0) { "power‑of‑two required" }
        val p = position
        val pad = (-p) and (boundary - 1)
        if (pad != 0) {
            requireRemaining(pad)
            repeat(pad) { buf.put(0) }
        }
        return this
    }

    /** Returns the underlying buffer (LE‑ordered view). Mutating position/limit affects this wrapper. */
    fun toByteBuffer(): ByteBuffer = buf

    /** Returns true if this buffer is backed by a heap array. */
    fun hasArray(): Boolean = buf.hasArray()

    /**
     * Returns the underlying array if present. Throws if this buffer is not backed by an array.
     *
     * ⚠ Note: use [arrayOffset] and [position] to interpret correct indexes.
     */
    fun array(): ByteArray = buf.array()

    /** Returns the starting offset into [array] where this buffer's data begins. */
    fun arrayOffset(): Int = buf.arrayOffset()
}

/* ======================= Convenience top‑level APIs ======================= */

operator fun ByteBufferL.compareTo(other: ByteBufferL): Int = compareLexUnsigned(this, other)
operator fun ByteBufferL.compareTo(other: ByteBuffer): Int = compareLexUnsigned(this, wrap(other))

/**
 * Lexicographically compares remaining bytes of [a] and [b] as **unsigned** bytes.
 * Positions of inputs are preserved (comparison uses duplicates).
 */
fun compareLexUnsigned(a: ByteBufferL, b: ByteBufferL): Int {
    val x = a.asReadOnlyByteBuffer().slice()
    val y = b.asReadOnlyByteBuffer().slice()
    val n = minOf(x.remaining(), y.remaining())
    val mm = x.mismatch(y)
    return when (mm) {
        -1 -> 0
        n -> x.remaining().compareTo(y.remaining())
        else -> ((x.get(mm).toInt() and 0xFF) - (y.get(mm).toInt() and 0xFF)).coerceIn(-1, 1)
    }
}
