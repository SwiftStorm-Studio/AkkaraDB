@file:Suppress("NOTHING_TO_INLINE", "unused", "duplicatedCode")

package dev.swiftstorm.akkaradb.common.vh

import java.lang.invoke.MethodHandles
import java.lang.invoke.VarHandle
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import java.util.zip.CRC32C
import kotlin.math.min

/**
 * LE — Little-Endian VarHandle core for ByteBuffer hot paths.
 *
 * Goals:
 *  - Always Little-Endian semantics (independent of buffer.order()).
 *  - No view buffers on hot paths (no asIntBuffer/asLongBuffer).
 *  - Absolute + relative (cursor) primitive I/O, aligned bulk ops.
 *  - Zero-copy CRC32C over ByteBuffer ranges.
 *  - Deterministic writeFully/readFully, zero fill, power-of-two align.
 *
 * Non-Goals (deliberately excluded for portability/perf):
 *  - Any DB/domain-specific layouts (record headers, flags, hashing, etc.)
 *  - Endian switching (keep LE only; branching hurts P99).
 *
 * Notes:
 *  - Bounds checks use capacity (absolute) / remaining (relative) for predictability.
 *  - Bulk methods require alignment (4B for ints, 8B for longs).
 */
object LE {

    // ---------------- VarHandles (LE fixed) ----------------
    @PublishedApi
    internal object VH {
        val I16: VarHandle = MethodHandles.byteBufferViewVarHandle(ShortArray::class.java, ByteOrder.LITTLE_ENDIAN)
        val I32: VarHandle = MethodHandles.byteBufferViewVarHandle(IntArray::class.java, ByteOrder.LITTLE_ENDIAN)
        val I64: VarHandle = MethodHandles.byteBufferViewVarHandle(LongArray::class.java, ByteOrder.LITTLE_ENDIAN)
        val F32: VarHandle = MethodHandles.byteBufferViewVarHandle(FloatArray::class.java, ByteOrder.LITTLE_ENDIAN)
        val F64: VarHandle = MethodHandles.byteBufferViewVarHandle(DoubleArray::class.java, ByteOrder.LITTLE_ENDIAN)
    }

    // ---------------- Absolute primitive I/O (always LE) ----------------
    @JvmStatic
    inline fun getU8(buf: ByteBuffer, at: Int): Int {
        rangeCheck(buf, at, 1); return buf.get(at).toInt() and 0xFF
    }

    @JvmStatic
    inline fun putU8(buf: ByteBuffer, at: Int, v: Int) {
        rangeCheck(buf, at, 1); buf.put(at, (v and 0xFF).toByte())
    }

    @JvmStatic
    inline fun getShort(buf: ByteBuffer, at: Int): Short {
        rangeCheck(buf, at, 2); return VH.I16.get(buf, at) as Short
    }

    @JvmStatic
    inline fun putShort(buf: ByteBuffer, at: Int, v: Short) {
        rangeCheck(buf, at, 2); VH.I16.set(buf, at, v)
    }

    @JvmStatic
    inline fun getInt(buf: ByteBuffer, at: Int): Int {
        rangeCheck(buf, at, 4); return VH.I32.get(buf, at) as Int
    }

    @JvmStatic
    inline fun putInt(buf: ByteBuffer, at: Int, v: Int) {
        rangeCheck(buf, at, 4); VH.I32.set(buf, at, v)
    }

    @JvmStatic
    inline fun getLong(buf: ByteBuffer, at: Int): Long {
        rangeCheck(buf, at, 8); return VH.I64.get(buf, at) as Long
    }

    @JvmStatic
    inline fun putLong(buf: ByteBuffer, at: Int, v: Long) {
        rangeCheck(buf, at, 8); VH.I64.set(buf, at, v)
    }

    @JvmStatic
    inline fun getFloat(buf: ByteBuffer, at: Int): Float {
        rangeCheck(buf, at, 4); return VH.F32.get(buf, at) as Float
    }

    @JvmStatic
    inline fun putFloat(buf: ByteBuffer, at: Int, v: Float) {
        rangeCheck(buf, at, 4); VH.F32.set(buf, at, v)
    }

    @JvmStatic
    inline fun getDouble(buf: ByteBuffer, at: Int): Double {
        rangeCheck(buf, at, 8); return VH.F64.get(buf, at) as Double
    }

    @JvmStatic
    inline fun putDouble(buf: ByteBuffer, at: Int, v: Double) {
        rangeCheck(buf, at, 8); VH.F64.set(buf, at, v)
    }

    // ---------------- Bulk (absolute; aligned) ----------------
    /** Put [len] ints from [src][off..off+len) at [at]. Requires 4-byte alignment. */
    @JvmStatic
    fun putInts(buf: ByteBuffer, at: Int, src: IntArray, off: Int = 0, len: Int = src.size - off) {
        require(off >= 0 && len >= 0 && off + len <= src.size)
        require((at and 3) == 0) { "unaligned int write: at=$at" }
        rangeCheck(buf, at, len shl 2)
        var p = at;
        var i = off;
        val end = off + len
        while (i < end) {
            VH.I32.set(buf, p, src[i]); p += 4; i++
        }
    }

    /** Put [len] longs from [src][off..off+len) at [at]. Requires 8-byte alignment. */
    @JvmStatic
    fun putLongs(buf: ByteBuffer, at: Int, src: LongArray, off: Int = 0, len: Int = src.size - off) {
        require(off >= 0 && len >= 0 && off + len <= src.size)
        require((at and 7) == 0) { "unaligned long write: at=$at" }
        rangeCheck(buf, at, len shl 3)
        var p = at;
        var i = off;
        val end = off + len
        while (i < end) {
            VH.I64.set(buf, p, src[i]); p += 8; i++
        }
    }

    // ---------------- Relative cursor (shares buf.position/limit) ----------------
    @JvmStatic
    fun cursor(buf: ByteBuffer): Cursor = Cursor(buf)

    class Cursor internal constructor(@PublishedApi internal val buf: ByteBuffer) {
        inline fun remaining(): Int = buf.remaining()

        inline fun getU8(): Int {
            ensure(1);
            val p = buf.position(); buf.position(p + 1); return buf.get(p).toInt() and 0xFF
        }

        inline fun putU8(v: Int): Cursor {
            ensure(1);
            val p = buf.position(); buf.put(p, (v and 0xFF).toByte()); buf.position(p + 1); return this
        }

        inline fun getShort(): Short {
            ensure(2);
            val p = buf.position(); buf.position(p + 2); return VH.I16.get(buf, p) as Short
        }

        inline fun putShort(v: Short): Cursor {
            ensure(2);
            val p = buf.position(); VH.I16.set(buf, p, v); buf.position(p + 2); return this
        }

        inline fun getInt(): Int {
            ensure(4);
            val p = buf.position(); buf.position(p + 4); return VH.I32.get(buf, p) as Int
        }

        inline fun putInt(v: Int): Cursor {
            ensure(4);
            val p = buf.position(); VH.I32.set(buf, p, v); buf.position(p + 4); return this
        }

        inline fun getLong(): Long {
            ensure(8);
            val p = buf.position(); buf.position(p + 8); return VH.I64.get(buf, p) as Long
        }

        inline fun putLong(v: Long): Cursor {
            ensure(8);
            val p = buf.position(); VH.I64.set(buf, p, v); buf.position(p + 8); return this
        }

        inline fun getFloat(): Float {
            ensure(4);
            val p = buf.position(); buf.position(p + 4); return VH.F32.get(buf, p) as Float
        }

        inline fun putFloat(v: Float): Cursor {
            ensure(4);
            val p = buf.position(); VH.F32.set(buf, p, v); buf.position(p + 4); return this
        }

        inline fun getDouble(): Double {
            ensure(8);
            val p = buf.position(); buf.position(p + 8); return VH.F64.get(buf, p) as Double
        }

        inline fun putDouble(v: Double): Cursor {
            ensure(8);
            val p = buf.position(); VH.F64.set(buf, p, v); buf.position(p + 8); return this
        }

        fun putBytes(src: ByteArray, off: Int = 0, len: Int = src.size - off): Cursor {
            require(off >= 0 && len >= 0 && off + len <= src.size); ensure(len)
            buf.put(src, off, len); return this
        }

        /** 4-byte aligned int bulk write (relative). */
        fun putInts(src: IntArray, off: Int = 0, len: Int = src.size - off): Cursor {
            require(off >= 0 && len >= 0 && off + len <= src.size)
            val bytes = len shl 2; ensure(bytes)
            val p0 = buf.position(); require((p0 and 3) == 0) { "unaligned int write: pos=$p0" }
            var p = p0;
            var i = off;
            val end = off + len
            while (i < end) {
                VH.I32.set(buf, p, src[i]); p += 4; i++
            }
            buf.position(p); return this
        }

        /** 8-byte aligned long bulk write (relative). */
        fun putLongs(src: LongArray, off: Int = 0, len: Int = src.size - off): Cursor {
            require(off >= 0 && len >= 0 && off + len <= src.size)
            val bytes = len shl 3; ensure(bytes)
            val p0 = buf.position(); require((p0 and 7) == 0) { "unaligned long write: pos=$p0" }
            var p = p0;
            var i = off;
            val end = off + len
            while (i < end) {
                VH.I64.set(buf, p, src[i]); p += 8; i++
            }
            buf.position(p); return this
        }

        fun align(pow2: Int): Cursor {
            align(buf, pow2); return this
        }

        inline fun position(): Int = buf.position()
        inline fun position(newPos: Int): Cursor {
            buf.position(newPos); return this
        }

        inline fun limit(): Int = buf.limit()
        inline fun limit(newLimit: Int): Cursor {
            buf.limit(newLimit); return this
        }

        inline fun mark(): Cursor {
            buf.mark(); return this
        }

        inline fun reset(): Cursor {
            buf.reset(); return this
        }

        @PublishedApi
        internal inline fun ensure(n: Int) {
            require(n >= 0 && n <= buf.remaining()) { "need=$n, remaining=${buf.remaining()}" }
        }
    }

    // ---------------- CRC32C (zero-copy over range) ----------------
    @JvmStatic
    fun crc32c(buf: ByteBuffer, start: Int, len: Int): Int {
        rangeCheck(buf, start, len)
        val dup = buf.duplicate()
        dup.position(start).limit(start + len)
        val c = CRC32C()
        c.update(dup)
        return c.value.toInt()
    }

    // ---------------- Channels (writeFully/readFully) ----------------
    /** Write exactly [len] bytes from buffer's current position to [ch], advancing position. */
    @JvmStatic
    fun writeFully(ch: WritableByteChannel, buf: ByteBuffer, len: Int): Int {
        require(len >= 0 && len <= buf.remaining()) { "len=$len, remaining=${buf.remaining()}" }
        var left = len
        while (left > 0) {
            val v = buf.duplicate().limit(buf.position() + left)
            val n = ch.write(v)
            require(n >= 0) { "channel closed" }
            buf.position(buf.position() + n)
            left -= n
        }
        return len
    }

    /** Read exactly [len] bytes into buffer from [ch], advancing position. */
    @JvmStatic
    fun readFully(ch: ReadableByteChannel, buf: ByteBuffer, len: Int): Int {
        require(len >= 0 && len <= buf.remaining()) { "len=$len, remaining=${buf.remaining()}" }
        var left = len
        while (left > 0) {
            val v = buf.duplicate().limit(buf.position() + left)
            val n = ch.read(v)
            if (n < 0) throw java.io.EOFException()
            buf.position(buf.position() + n)
            left -= n
        }
        return len
    }

    // ---------------- Zero fill / align ----------------
    private val ZERO = ByteArray(1024)

    /** Zero-fill [n] bytes at current position, advancing position. */
    @JvmStatic
    fun fillZero(buf: ByteBuffer, n: Int) {
        require(n >= 0 && n <= buf.remaining())
        var left = n
        while (left > 0) {
            val k = min(left, ZERO.size)
            buf.put(ZERO, 0, k)
            left -= k
        }
    }

    /** Align current position to [pow2] by zero-filling (pow2 must be power-of-two). */
    @JvmStatic
    fun align(buf: ByteBuffer, pow2: Int) {
        require(pow2 > 0 && (pow2 and (pow2 - 1)) == 0) { "align must be power-of-two" }
        val p = buf.position()
        val aligned = (p + (pow2 - 1)) and (pow2 - 1).inv()
        val pad = aligned - p
        if (pad > 0) fillZero(buf, pad)
    }

    // ---------------- Internal ----------------
    @PublishedApi
    internal inline fun rangeCheck(buf: ByteBuffer, at: Int, len: Int) {
        require(at >= 0 && len >= 0 && at + len <= buf.capacity()) {
            "range OOB: at=$at len=$len cap=${buf.capacity()}"
        }
    }
}
