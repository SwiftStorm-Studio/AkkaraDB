/*
 * AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */
@file:Suppress("NOTHING_TO_INLINE", "unused")

package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.types.U32
import dev.swiftstorm.akkaradb.common.types.U64
import dev.swiftstorm.akkaradb.common.vh.LE
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

/**
 * ByteBufferL — thin Little-Endian buffer wrapper for AkkaraDB.
 *
 * Design goals:
 *  - Always Little-Endian semantics without relying on external callers to set buffer.order().
 *  - Keep position/limit management as Kotlin properties for readability.
 *  - Prefer property-based relative and absolute primitive I/O (i8/i16/i32/i64/f32/f64 + u32/u64).
 *  - Avoid view buffers on hot paths; use duplicate/slice for safe slicing.
 *  - Public methods NEVER return raw ByteBuffer; they return ByteBufferL to preserve LE safety.
 *  - Raw ByteBuffer exposure is available as `internal` helpers (raw*), with intentionally unsafe semantics.
 *
 * Usage tips:
 *  - Use `at(index)` for absolute property-based access.
 *  - Use `slice()` / `sliceAt(off,len)` to obtain LE-safe subviews as ByteBufferL.
 *  - Prefer your BufferPool in production; `allocate()` is for convenience.
 */
class ByteBufferL private constructor(
    @PublishedApi internal val buf: ByteBuffer
) {
    // ---------------- Factory ----------------
    companion object {
        /** Wrap an existing buffer. Position/limit are respected; order is not relied on. */
        @JvmStatic
        fun wrap(buffer: ByteBuffer): ByteBufferL = ByteBufferL(buffer)

        /** Allocate a new buffer. Prefer your BufferPool in production. */
        @JvmStatic
        fun allocate(capacity: Int, direct: Boolean = true): ByteBufferL {
            val bb = if (direct) ByteBuffer.allocateDirect(capacity) else ByteBuffer.allocate(capacity)
            // Not required by LE code paths, but sets an explicit default for any incidental relative ops.
            bb.order(ByteOrder.LITTLE_ENDIAN)
            return ByteBufferL(bb)
        }
    }

    // ---------------- Basics as properties ----------------
    /** Buffer total capacity. */
    val capacity: Int get() = buf.capacity()

    /** Short alias for capacity. */
    val cap: Int get() = buf.capacity()

    /** Current position. */
    var position: Int
        get() = buf.position()
        set(value) {
            buf.position(value)
        }

    /** Current limit. */
    var limit: Int
        get() = buf.limit()
        set(value) {
            buf.limit(value)
        }

    /** Remaining bytes derived from position/limit. */
    val remaining: Int get() = buf.remaining()

    /** Whether the underlying buffer is direct. */
    val isDirect: Boolean get() = buf.isDirect

    /** for internal use only: raw ByteBuffer exposure (unsafe w.r.t. LE semantics). */
    @Deprecated("Exposes raw ByteBuffer; unsafe w.r.t. LE semantics. Use LE-safe methods instead.", ReplaceWith("this"))
    val byte: ByteBuffer
        get() = buf

    // ---------------- LE-safe views (public) ----------------
    /**
     * Read-only duplicate as a LE-safe ByteBufferL view.
     * Note: Writes via this view will throw, as the underlying buffer is read-only.
     */
    fun asReadOnlyDuplicate(): ByteBufferL =
        ByteBufferL(buf.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN))

    /** Duplicate with independent position/limit as a LE-safe ByteBufferL view. */
    fun duplicate(): ByteBufferL =
        ByteBufferL(buf.duplicate().order(ByteOrder.LITTLE_ENDIAN))

    /** Slice of the remaining region as a LE-safe ByteBufferL view. */
    fun slice(): ByteBufferL =
        ByteBufferL(buf.slice().order(ByteOrder.LITTLE_ENDIAN))

    /**
     * Absolute slice at [at] with [len] as a LE-safe ByteBufferL view.
     * Throws if the requested range is out-of-bounds.
     */
    fun sliceAt(at: Int, len: Int): ByteBufferL {
        require(at >= 0 && len >= 0 && at + len <= buf.capacity()) { "slice OOB" }
        val d = buf.duplicate()
        d.position(at).limit(at + len)
        return ByteBufferL(d.slice().order(ByteOrder.LITTLE_ENDIAN))
    }

    /** Whether there are remaining bytes. */
    fun has(): Boolean = remaining > 0

    // ---------------- Intentional raw exposure (internal) ----------------
    /** Internal: expose raw underlying ByteBuffer (unsafe w.r.t. LE semantics). */
    internal fun rawBuffer(): ByteBuffer = buf

    /** Internal: raw duplicate with no enforced LE safety on callers. */
    internal fun rawDuplicate(): ByteBuffer = buf.duplicate()

    /** Internal: raw slice with no enforced LE safety on callers. */
    internal fun rawSlice(): ByteBuffer = buf.slice()

    /** Internal: raw absolute slice with no enforced LE safety on callers. */
    internal fun rawSliceAt(at: Int, len: Int): ByteBuffer {
        val d = buf.duplicate()
        d.position(at).limit(at + len)
        return d.slice()
    }

    // ---------------- for method chaining ----------------
    /** Set position and return this for chaining. */
    fun position(newPos: Int): ByteBufferL {
        position = newPos; return this
    }

    /** Set limit and return this for chaining. */
    fun limit(newLim: Int): ByteBufferL {
        limit = newLim; return this
    }

    /** Clear (position=0, limit=capacity) and return this for chaining. */
    fun clear(): ByteBufferL {
        buf.clear(); return this
    }

    // ---------------- Relative primitives via properties ----------------
    /**
     * Relative read/write properties:
     * - get: reads a value at current position and advances position
     * - set: writes a value at current position and advances position
     *
     * Naming:
     * - i8 : unsigned 8-bit (Int 0..255)
     * - i16: Short (LE)
     * - i32: Int   (LE)
     * - i64: Long  (LE)
     * - u32: U32   (LE, unsigned 32-bit)
     * - u64: U64   (LE, unsigned 64-bit)
     * - f32: Float (LE)
     * - f64: Double(LE)
     */
    var i8: Int
        get() = LE.cursor(buf).getU8()
        set(v) {
            LE.cursor(buf).putU8(v)
        }

    var i16: Short
        get() = LE.cursor(buf).getShort()
        set(v) {
            LE.cursor(buf).putShort(v)
        }

    var i32: Int
        get() = LE.cursor(buf).getInt()
        set(v) {
            LE.cursor(buf).putInt(v)
        }

    var i64: Long
        get() = LE.cursor(buf).getLong()
        set(v) {
            LE.cursor(buf).putLong(v)
        }

    var u32: U32
        get() = LE.cursor(buf).getU32()
        set(v) {
            LE.cursor(buf).putU32(v)
        }

    var u64: U64
        get() = LE.cursor(buf).getU64()
        set(v) {
            LE.cursor(buf).putU64(v)
        }

    var f32: Float
        get() = LE.cursor(buf).getFloat()
        set(v) {
            LE.cursor(buf).putFloat(v)
        }

    var f64: Double
        get() = LE.cursor(buf).getDouble()
        set(v) {
            LE.cursor(buf).putDouble(v)
        }

    /** Relative bulk write of bytes. */
    fun putBytes(src: ByteArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        LE.cursor(buf).putBytes(src, off, len); return this
    }

    /** Relative bulk write from another ByteBufferL (direct→direct, non-destructive). */
    fun put(src: ByteBufferL, len: Int = src.remaining): ByteBufferL {
        require(len in 0..src.remaining) { "len out of range: $len > ${src.remaining}" }

        val dstBB = this.buf
        val srcBB = src.buf

        val dstPos = dstBB.position()
        val srcPos = srcBB.position()

        val slice = srcBB.slice()
        slice.limit(len)
        dstBB.put(slice) // ByteBuffer→ByteBuffer copy

        dstBB.position(dstPos + len)
        srcBB.position(srcPos)
        return this
    }

    /** Relative bulk write of Ints (aligned). */
    fun putInts(src: IntArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        LE.cursor(buf).putInts(src, off, len); return this
    }

    /** Relative bulk write of Longs (aligned). */
    fun putLongs(src: LongArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        LE.cursor(buf).putLongs(src, off, len); return this
    }

    // ---------------- Absolute primitives via view ----------------
    /**
     * Property-based absolute access view at a fixed [index].
     * Example:
     *   buf.at(128).i32 = 42
     *   val x = buf.at(136).i64
     */
    fun at(index: Int): At = At(index)

    inner class At internal constructor(private val base: Int) {
        var i8: Int
            get() = LE.getU8(buf, base)
            set(v) {
                LE.putU8(buf, base, v)
            }

        var i16: Short
            get() = LE.getShort(buf, base)
            set(v) {
                LE.putShort(buf, base, v)
            }

        var i32: Int
            get() = LE.getInt(buf, base)
            set(v) {
                LE.putInt(buf, base, v)
            }

        var i64: Long
            get() = LE.getLong(buf, base)
            set(v) {
                LE.putLong(buf, base, v)
            }

        var u32: U32
            get() = LE.getU32(buf, base)
            set(v) {
                LE.putU32(buf, base, v)
            }

        var u64: U64
            get() = LE.getU64(buf, base)
            set(v) {
                LE.putU64(buf, base, v)
            }

        var f32: Float
            get() = LE.getFloat(buf, base)
            set(v) {
                LE.putFloat(buf, base, v)
            }

        var f64: Double
            get() = LE.getDouble(buf, base)
            set(v) {
                LE.putDouble(buf, base, v)
            }

        /** Absolute bulk write helpers pinned to this base (for parity stripes etc.). */
        fun putInts(src: IntArray, off: Int = 0, len: Int = src.size - off): At {
            LE.putInts(buf, base, src, off, len); return this
        }

        fun putLongs(src: LongArray, off: Int = 0, len: Int = src.size - off): At {
            LE.putLongs(buf, base, src, off, len); return this
        }
    }

    // ---------------- AkkaraDB header bridge (32B) ----------------
    /** Absolute write of 32B header (U32/U64 API). */
    fun putHeader32(at: Int, kLen: Int, vLen: U32, seq: U64, flags: Int, keyFP64: U64, miniKey: U64): ByteBufferL {
        AKHdr32.write(buf, at, kLen, vLen, seq, flags, keyFP64, miniKey); return this
    }

    /** Relative write of 32B header at current position; advances by 32 (U32/U64 API). */
    fun putHeader32(kLen: Int, vLen: U32, seq: U64, flags: Int, keyFP64: U64, miniKey: U64): ByteBufferL {
        AKHdr32.writeRel(buf, kLen, vLen, seq, flags, keyFP64, miniKey); return this
    }

    fun putHeader32(at: Int, kLen: Int, vLen: Long, seq: Long, flags: Int, keyFP64: Long, miniKey: Long): ByteBufferL =
        putHeader32(at, kLen, U32.of(vLen), U64.fromSigned(seq), flags, U64.fromSigned(keyFP64), U64.fromSigned(miniKey))

    fun putHeader32(kLen: Int, vLen: Long, seq: Long, flags: Int, keyFP64: Long, miniKey: Long): ByteBufferL =
        putHeader32(kLen, U32.of(vLen), U64.fromSigned(seq), flags, U64.fromSigned(keyFP64), U64.fromSigned(miniKey))

    /** Absolute read of 32B header. */
    fun readHeader32(at: Int): AKHdr32.Header = AKHdr32.read(buf, at)

    /** Relative read of 32B header at current position; advances by 32. */
    fun readHeader32(): AKHdr32.Header = AKHdr32.readRel(buf)

    // ---------------- Align / padding / CRC ----------------
    /** Power-of-two alignment by zero fill (e.g., 32*1024 for block size). */
    fun align(pow2: Int): ByteBufferL {
        LE.align(buf, pow2); return this
    }

    /** Zero-fill [n] bytes at current position. */
    fun fillZero(n: Int): ByteBufferL {
        LE.fillZero(buf, n); return this
    }

    /** CRC32C over [start, start+len). Zero-copy via ByteBuffer slice. */
    fun crc32cRange(start: Int, len: Int): Int = LE.crc32c(buf, start, len)

    // ---------------- Channels ----------------
    /** Write exactly [len] bytes from current position to [ch], advancing position. */
    fun writeFully(ch: WritableByteChannel, len: Int = remaining): Int = LE.writeFully(ch, buf, len)

    /** Read exactly [len] bytes into this buffer from [ch], advancing position. */
    fun readFully(ch: ReadableByteChannel, len: Int): Int = LE.readFully(ch, buf, len)

    // ---------------- Convenience (header + key/value) ----------------
    /**
     * Write header32 + key + value (relative).
     * Returns absolute offset to the start of the header for convenience.
     */
    fun putRecord32(
        key: ByteArray,
        value: ByteArray,
        seq: U64,
        flags: Int,
        keyFP64: U64,
        miniKey: U64
    ): Int {
        val hdrPos = position
        putHeader32(key.size, U32.of(value.size.toLong()), seq, flags, keyFP64, miniKey)
        putBytes(key)
        putBytes(value)
        return hdrPos
    }

    /** Overload for callers with primitives. */
    fun putRecord32(
        key: ByteArray,
        value: ByteArray,
        seq: Long,
        flags: Int,
        keyFP64: Long,
        miniKey: Long
    ): Int = putRecord32(key, value, U64.fromSigned(seq), flags, U64.fromSigned(keyFP64), U64.fromSigned(miniKey))

    /**
     * Read header32 at [recOff] and return LE-safe slices for key/value.
     * Result pair: (header, Pair(keySliceL, valueSliceL))
     */
    fun readRecord32(recOff: Int): Pair<AKHdr32.Header, Pair<ByteBufferL, ByteBufferL>> {
        val h = readHeader32(recOff)
        val keyOff = recOff + AKHdr32.SIZE
        val valOff = keyOff + h.kLen
        val keySlice = sliceAt(keyOff, h.kLen)
        val vLenInt = h.vLen.toIntExact() // current chunk size should fit into Int
        val valSlice = sliceAt(valOff, vLenInt)
        return h to (keySlice to valSlice)
    }

    // ---------------- Unsafe direct access (internal) ----------------
    inline fun getI64At(off: Int): Long = LE.getLong(buf, off)
    inline fun putI64At(off: Int, v: Long) {
        LE.putLong(buf, off, v)
    }

    inline fun getU8At(off: Int): Int = LE.getU8(buf, off)
    inline fun putU8At(off: Int, v: Int) {
        LE.putU8(buf, off, v)
    }
}
