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
import dev.swiftstorm.akkaradb.common.vh.LE.rangeCheck
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

/**
 * ByteBufferL — thin Little-Endian buffer wrapper for AkkaraDB.
 *
 * Invariants:
 * - All public views (duplicate/slice) returned by this class are explicitly LITTLE_ENDIAN.
 * - Relative primitive properties advance position using LE helpers without relying on buffer.order().
 * - API compatibility is preserved; absolute fast-path helpers are added.
 */
class ByteBufferL private constructor(
    @PublishedApi internal val buf: ByteBuffer
) {
    companion object {
        /** Wrap an existing buffer. Position/limit are respected; order is not relied on. */
        @JvmStatic
        fun wrap(buffer: ByteBuffer): ByteBufferL = ByteBufferL(buffer)

        /** Allocate a new buffer. Prefer your BufferPool in production. */
        @JvmStatic
        fun allocate(capacity: Int, direct: Boolean = true): ByteBufferL {
            val bb = if (direct) ByteBuffer.allocateDirect(capacity) else ByteBuffer.allocate(capacity)
            // Set an explicit default order for incidental relative ops done outside LE helpers.
            bb.order(ByteOrder.LITTLE_ENDIAN)
            return ByteBufferL(bb)
        }

        @JvmStatic
        fun getInts(buf: ByteBuffer, at: Int, dst: IntArray, off: Int = 0, len: Int = dst.size - off) {
            require(off >= 0 && len >= 0 && off + len <= dst.size)
            require((at and 3) == 0) { "unaligned int read: at=$at" }
            rangeCheck(buf, at, len shl 2)
            var p = at;
            var i = off;
            val end = off + len
            while (i < end) {
                dst[i] = LE.VH.I32.get(buf, p) as Int; p += 4; i++
            }
        }

        @JvmStatic
        fun getLongs(buf: ByteBuffer, at: Int, dst: LongArray, off: Int = 0, len: Int = dst.size - off) {
            require(off >= 0 && len >= 0 && off + len <= dst.size)
            require((at and 7) == 0) { "unaligned long read: at=$at" }
            rangeCheck(buf, at, len shl 3)
            var p = at;
            var i = off;
            val end = off + len
            while (i < end) {
                dst[i] = LE.VH.I64.get(buf, p) as Long; p += 8; i++
            }
        }
    }

    // ---------------- Basics ----------------
    /** Total capacity. */
    val capacity: Int get() = buf.capacity()

    /** Alias for capacity. */
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

    /** Intentionally unsafe: exposes raw ByteBuffer (LE invariants are not enforced). */
    @Deprecated("Exposes raw ByteBuffer; unsafe w.r.t. LE semantics. Use LE-safe methods instead.", ReplaceWith("this"))
    fun rawDuplicate(): ByteBuffer = buf.duplicate()

    // ---------------- LE-safe views (public) ----------------
    /**
     * Read-only duplicate as a LE-safe view.
     * Contract: the returned buffer's byte order is LITTLE_ENDIAN.
     */
    fun asReadOnlyDuplicate(): ByteBufferL =
        ByteBufferL(buf.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN))

    /** Duplicate with independent position/limit as a LE-safe view. */
    fun duplicate(): ByteBufferL =
        ByteBufferL(buf.duplicate().order(ByteOrder.LITTLE_ENDIAN))

    /** Slice of the remaining region as a LE-safe view. */
    fun slice(): ByteBufferL =
        ByteBufferL(buf.slice().order(ByteOrder.LITTLE_ENDIAN))

    /**
     * Absolute slice at [at] with [len] as a LE-safe view.
     * Throws if the requested range is out-of-bounds.
     */
    fun sliceAt(at: Int, len: Int): ByteBufferL {
        require(at >= 0) { "sliceAt: negative offset: $at" }
        require(len >= 0) { "sliceAt: negative length: $len" }
        val cap = buf.capacity()
        require(at + len <= cap) {
            "sliceAt OOB: at=$at, len=$len, cap=$cap (max index ${cap - 1})"
        }
        val d = buf.duplicate()
        d.position(at).limit(at + len)
        return ByteBufferL(d.slice().order(ByteOrder.LITTLE_ENDIAN))
    }

    /** Whether there are remaining bytes. */
    fun has(): Boolean = remaining > 0

    // ---------------- Intentional raw exposure (internal) ----------------
    internal fun rawBuffer(): ByteBuffer = buf
    internal fun rawSlice(): ByteBuffer = buf.slice()
    internal fun rawSliceAt(at: Int, len: Int): ByteBuffer {
        val d = buf.duplicate()
        d.position(at).limit(at + len)
        return d.slice()
    }

    // ---------------- Chaining helpers ----------------
    fun position(newPos: Int): ByteBufferL {
        position = newPos; return this
    }

    fun limit(newLim: Int): ByteBufferL {
        limit = newLim; return this
    }

    fun clear(): ByteBufferL {
        buf.clear(); return this
    }

    // ---------------- Relative primitives (fast path) ----------------
    /**
     * Fast relative get/put helpers that avoid constructing a per-access cursor.
     * They call LE.getX/putX at the current position and manually advance it.
     */
    @PublishedApi
    internal inline fun relGetU8(): Int {
        val p = buf.position()
        val v = LE.getU8(buf, p)
        buf.position(p + 1)
        return v
    }

    @PublishedApi
    internal inline fun relPutU8(v: Int) {
        val p = buf.position()
        LE.putU8(buf, p, v)
        buf.position(p + 1)
    }

    @PublishedApi
    internal inline fun relGetShort(): Short {
        val p = buf.position()
        val v = LE.getShort(buf, p)
        buf.position(p + 2)
        return v
    }

    @PublishedApi
    internal inline fun relPutShort(v: Short) {
        val p = buf.position()
        LE.putShort(buf, p, v)
        buf.position(p + 2)
    }

    @PublishedApi
    internal inline fun relGetInt(): Int {
        val p = buf.position()
        val v = LE.getInt(buf, p)
        buf.position(p + 4)
        return v
    }

    @PublishedApi
    internal inline fun relPutInt(v: Int) {
        val p = buf.position()
        LE.putInt(buf, p, v)
        buf.position(p + 4)
    }

    @PublishedApi
    internal inline fun relGetLong(): Long {
        val p = buf.position()
        val v = LE.getLong(buf, p)
        buf.position(p + 8)
        return v
    }

    @PublishedApi
    internal inline fun relPutLong(v: Long) {
        val p = buf.position()
        LE.putLong(buf, p, v)
        buf.position(p + 8)
    }

    @PublishedApi
    internal inline fun relGetFloat(): Float {
        val p = buf.position()
        val v = LE.getFloat(buf, p)
        buf.position(p + 4)
        return v
    }

    @PublishedApi
    internal inline fun relPutFloat(v: Float) {
        val p = buf.position()
        LE.putFloat(buf, p, v)
        buf.position(p + 4)
    }

    @PublishedApi
    internal inline fun relGetDouble(): Double {
        val p = buf.position()
        val v = LE.getDouble(buf, p)
        buf.position(p + 8)
        return v
    }

    @PublishedApi
    internal inline fun relPutDouble(v: Double) {
        val p = buf.position()
        LE.putDouble(buf, p, v)
        buf.position(p + 8)
    }

    // Relative properties (backed by fast helpers)
    var i8: Int
        get() = relGetU8()
        set(v) = relPutU8(v)

    var i16: Short
        get() = relGetShort()
        set(v) = relPutShort(v)

    var i32: Int
        get() = relGetInt()
        set(v) = relPutInt(v)

    var i64: Long
        get() = relGetLong()
        set(v) = relPutLong(v)

    var u32: U32
        get() = LE.cursor(buf).getU32()
        set(v) {
            LE.cursor(buf).putU32(v)
        }   // do NOT use toIntExact()

    var u64: U64
        get() = LE.cursor(buf).getU64()
        set(v) {
            LE.cursor(buf).putU64(v)
        }

    var f32: Float
        get() = relGetFloat()
        set(v) = relPutFloat(v)

    var f64: Double
        get() = relGetDouble()
        set(v) = relPutDouble(v)

    /** Relative bulk write of bytes. */
    fun putBytes(src: ByteArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        require(off in 0..src.size && len >= 0 && off + len <= src.size) {
            "putBytes OOB: off=$off, len=$len, src.size=${src.size}"
        }
        // Use ByteBuffer bulk put to benefit from native paths for direct buffers.
        val p = buf.position()
        val limit = p + len
        require(limit <= buf.limit()) { "putBytes OOB into dst: pos=$p, len=$len, limit=${buf.limit()}" }
        val ro = ByteBuffer.wrap(src, off, len) // heap buffer view (cheap)
        buf.put(ro)
        return this
    }

    /**
     * Relative bulk write from another ByteBufferL.
     * Handles self-copy overlap safely using a small chunked temp buffer for direct buffers.
     */
    fun put(src: ByteBufferL, len: Int = src.remaining): ByteBufferL {
        require(len in 0..src.remaining) { "len out of range: $len > ${src.remaining}" }

        val dstBB = this.buf
        val srcBB = src.buf

        val dstStart = dstBB.position()
        val srcStart = srcBB.position()
        val dstEnd = dstStart + len
        require(dstEnd <= dstBB.limit()) { "put: dst overflow pos=$dstStart len=$len limit=${dstBB.limit()}" }

        if (dstBB === srcBB) {
            // Potential overlap: copy via small temporary window (8–64 KiB chunk).
            val chunk = 64 * 1024
            val tmp = ByteArray(minOf(len, chunk))
            var remaining = len
            var s = srcStart
            var d = dstStart
            val sDup = srcBB.duplicate()
            val dDup = dstBB.duplicate()
            while (remaining > 0) {
                val step = minOf(remaining, tmp.size)
                sDup.limit(s + step).position(s)
                sDup.get(tmp, 0, step)
                dDup.limit(d + step).position(d)
                dDup.put(tmp, 0, step)
                s += step; d += step; remaining -= step
            }
            dstBB.position(dstEnd)
            srcBB.position(srcStart)
        } else {
            // Non-overlap or different buffers: use native put(ByteBuffer)
            val slice = srcBB.slice()
            slice.limit(len)
            dstBB.put(slice)
            dstBB.position(dstEnd)
            srcBB.position(srcStart) // non-destructive read
        }
        return this
    }


    /** Relative bulk write of Ints (aligned). */
    fun putInts(src: IntArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        require(off in 0..src.size && len >= 0 && off + len <= src.size) {
            "putInts OOB: off=$off, len=$len, size=${src.size}"
        }
        LE.putInts(buf, buf.position(), src, off, len)
        buf.position(buf.position() + (len shl 2))
        return this
    }

    /** Relative bulk write of Longs (aligned). */
    fun putLongs(src: LongArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        require(off in 0..src.size && len >= 0 && off + len <= src.size) {
            "putLongs OOB: off=$off, len=$len, size=${src.size}"
        }
        LE.putLongs(buf, buf.position(), src, off, len)
        buf.position(buf.position() + (len shl 3))
        return this
    }

    // ---------------- Absolute access via view ----------------
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

        fun putInts(src: IntArray, off: Int = 0, len: Int = src.size - off): At {
            LE.putInts(buf, base, src, off, len); return this
        }
        fun putLongs(src: LongArray, off: Int = 0, len: Int = src.size - off): At {
            LE.putLongs(buf, base, src, off, len); return this
        }
    }

    // ---------------- Fast absolute helpers (allocation-free) ----------------
    inline fun getI64At(off: Int): Long = LE.getLong(buf, off)
    inline fun putI64At(off: Int, v: Long) {
        LE.putLong(buf, off, v)
    }

    inline fun getU8At(off: Int): Int = LE.getU8(buf, off)
    inline fun putU8At(off: Int, v: Int) {
        LE.putU8(buf, off, v)
    }

    inline fun getI32At(off: Int): Int = LE.getInt(buf, off)
    inline fun putI32At(off: Int, v: Int) {
        LE.putInt(buf, off, v)
    }

    inline fun getI16At(off: Int): Short = LE.getShort(buf, off)
    inline fun putI16At(off: Int, v: Short) {
        LE.putShort(buf, off, v)
    }

    // ---------------- AkkaraDB header bridge (32B) ----------------
    fun putHeader32(at: Int, kLen: Int, vLen: U32, seq: U64, flags: Int, keyFP64: U64, miniKey: U64): ByteBufferL {
        AKHdr32.write(buf, at, kLen, vLen, seq, flags, keyFP64, miniKey); return this
    }
    fun putHeader32(kLen: Int, vLen: U32, seq: U64, flags: Int, keyFP64: U64, miniKey: U64): ByteBufferL {
        AKHdr32.writeRel(buf, kLen, vLen, seq, flags, keyFP64, miniKey); return this
    }
    fun putHeader32(at: Int, kLen: Int, vLen: Long, seq: Long, flags: Int, keyFP64: Long, miniKey: Long): ByteBufferL =
        putHeader32(at, kLen, U32.of(vLen), U64.fromSigned(seq), flags, U64.fromSigned(keyFP64), U64.fromSigned(miniKey))
    fun putHeader32(kLen: Int, vLen: Long, seq: Long, flags: Int, keyFP64: Long, miniKey: Long): ByteBufferL =
        putHeader32(kLen, U32.of(vLen), U64.fromSigned(seq), flags, U64.fromSigned(keyFP64), U64.fromSigned(miniKey))

    fun readHeader32(at: Int): AKHdr32.Header = AKHdr32.read(buf, at)
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

    /** CRC32C over [start, start+len). Validates range before computing. */
    fun crc32cRange(start: Int, len: Int): Int {
        require(start >= 0) { "crc32cRange: negative start: $start" }
        require(len >= 0) { "crc32cRange: negative length: $len" }
        val cap = buf.capacity()
        require(start + len <= cap) {
            "crc32cRange OOB: start=$start len=$len cap=$cap"
        }
        return LE.crc32c(buf, start, len)
    }

    // ---------------- Channels ----------------
    /** Write exactly [len] bytes from current position to [ch], advancing position. */
    fun writeFully(ch: WritableByteChannel, len: Int): Int =
        LE.writeFully(ch, buf, len)
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

    fun putRecord32(
        key: ByteArray,
        value: ByteArray,
        seq: Long,
        flags: Int,
        keyFP64: Long,
        miniKey: Long
    ): Int = putRecord32(key, value, U64.fromSigned(seq), flags, U64.fromSigned(keyFP64), U64.fromSigned(miniKey))

    /**
     * Read a 32-byte record header at absolute offset [recOff] and return
     * LE-safe slices for key and value without copying.
     *
     * Returns pair: (header, Pair(keySlice, valueSlice))
     *
     * Preconditions:
     * - [recOff] points to the start of a record header (32 bytes).
     * - The entire record (header + key + value) fits within the buffer.
     */
    fun readRecord32(recOff: Int): Pair<AKHdr32.Header, Pair<ByteBufferL, ByteBufferL>> {
        // Read header (absolute, does not mutate global position)
        val h = readHeader32(recOff)

        // Compute absolute offsets
        val keyOff = recOff + AKHdr32.SIZE
        val kLen = h.kLen
        val vLenI = h.vLen.toIntExact() // constrained by current chunk size

        // LE-safe zero-copy slices (with bounds checks inside sliceAt)
        val keySlice = sliceAt(keyOff, kLen)
        val valSlice = sliceAt(keyOff + kLen, vLenI)

        return h to (keySlice to valSlice)
    }
}

val ByteBufferL.isReadOnly: Boolean get() = buf.isReadOnly

fun ByteBufferL.hasRemaining(): Boolean = remaining > 0

fun ByteBufferL.copy(isDirect: Boolean = false): ByteBufferL {
    val dup = duplicate().position(0)
    val len = dup.remaining
    val dst = ByteBufferL.allocate(len, direct = isDirect)
    if (len > 0) dst.put(dup, len)
    dst.position = 0
    dst.limit = len
    return dst
}

fun ByteBufferL.putAscii(s: String): ByteBufferL {
    // We rely on the caller's isAsciiNoSep(), but still guard for robustness.
    for (ch in s) {
        val code = ch.code
        require(code <= 0x7F) { "Non-ASCII char: U+%04X".format(code) }
        this.i8 = code
    }
    return this
}

fun ByteBufferL.debugString(limit: Int = Int.MAX_VALUE / 4): String {
    val sb = StringBuilder()
    for (i in 0 until minOf(limit, remaining)) {
        sb.append(String.format("%02X ", i8))
    }
    if (remaining > 0) sb.append("...")
    return sb.toString()
}

fun ByteBufferL.toByteArray(): ByteArray {
    val n = remaining
    val dst = ByteArray(n)

    val dup = buf.duplicate()
    dup.limit(buf.limit()).position(buf.position())

    dup.get(dst, 0, n)

    return dst
}

val ByteBufferL.hex: String
    get() {
        val sb = StringBuilder(remaining * 2)
        val bb = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN)
        val p0 = bb.position()
        val p1 = bb.limit()
        var p = p0
        while (p < p1) {
            val b = LE.getU8(bb, p)
            sb.append(HEX[(b ushr 4) and 0xF])
            sb.append(HEX[b and 0xF])
            p++
        }
        return sb.toString()
    }

private val HEX = "0123456789ABCDEF".toCharArray()