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
 * Design:
 *  - Always Little-Endian semantics without relying on buffer.order().
 *  - Position/limit management is exposed as Kotlin properties.
 *  - Relative primitive I/O is property-based (i8/i16/i32/i64/f32/f64 + u32/u64).
 *  - Absolute primitive I/O uses property-based view via [at(index)].
 *  - 32B header read/write delegates to [AKHdr32] (vLen: U32, seq/keyFP/mini: U64).
 *  - No view buffers on hot paths; only duplicate/slice used for safe slicing.
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
            // Not required by LE, but makes intent explicit for incidental relative ops outside LE.
            bb.order(ByteOrder.LITTLE_ENDIAN)
            return ByteBufferL(bb)
        }
    }

    // ---------------- Basics as properties ----------------
    val capacity: Int get() = buf.capacity()
    val cap: Int get() = buf.capacity()

    var position: Int
        get() = buf.position()
        set(value) {
            buf.position(value)
        }

    var limit: Int
        get() = buf.limit()
        set(value) {
            buf.limit(value)
        }

    /** Remaining bytes (derived). */
    val remaining: Int get() = buf.remaining()

    /** Direct buffer flag. */
    val isDirect: Boolean get() = buf.isDirect

    /** Read-only duplicate; independent position/limit; LE order applied for convenience. */
    fun asReadOnlyDuplicate(): ByteBuffer = buf.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN)

    /** Duplicate; independent position/limit; LE order applied for convenience. */
    fun duplicate(): ByteBuffer = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN)

    /** Slice of remaining region; independent position/limit; LE order applied. */
    fun slice(): ByteBuffer = buf.slice().order(ByteOrder.LITTLE_ENDIAN)

    /** Absolute slice at [at] with [len]; independent position/limit; LE order applied. */
    fun sliceAt(at: Int, len: Int): ByteBuffer {
        require(at >= 0 && len >= 0 && at + len <= buf.capacity()) { "slice OOB" }
        val d = buf.duplicate()
        d.position(at).limit(at + len)
        return d.slice().order(ByteOrder.LITTLE_ENDIAN)
    }

    // ---------------- for method chaining ----------------
    fun position(newPos: Int): ByteBufferL {
        position = newPos; return this
    }

    fun limit(newLim: Int): ByteBufferL {
        limit = newLim; return this
    }

    fun clear(): ByteBufferL {
        buf.clear(); return this
    }

    // ---------------- Relative primitives via properties ----------------
    /**
     * Relative read/write properties:
     *  - get: reads a value at current position and advances position
     *  - set: writes a value at current position and advances position
     *
     * Naming:
     *  - i8 : unsigned 8-bit (Int 0..255)
     *  - i16: Short (LE)
     *  - i32: Int   (LE)
     *  - i64: Long  (LE)
     *  - u32: U32   (LE, unsigned 32-bit)
     *  - u64: U64   (LE, unsigned 64-bit)
     *  - f32: Float (LE)
     *  - f64: Double(LE)
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

    /** Relative bulk write (aligned) */
    fun putInts(src: IntArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        LE.cursor(buf).putInts(src, off, len); return this
    }

    fun putLongs(src: LongArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        LE.cursor(buf).putLongs(src, off, len); return this
    }

    // ---------------- Absolute primitives via view ----------------
    /**
     * Property-based absolute access view at a fixed [index].
     * Example:
     *   b.at(128).i32 = 42
     *   val x = b.at(136).i64
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
     * Read header32 at [recOff] and return slices for key/value.
     * Result pair: (header, Pair(keySlice, valueSlice))
     */
    fun readRecord32(recOff: Int): Pair<AKHdr32.Header, Pair<ByteBuffer, ByteBuffer>> {
        val h = readHeader32(recOff)
        val keyOff = recOff + AKHdr32.SIZE
        val valOff = keyOff + h.kLen
        val keySlice = sliceAt(keyOff, h.kLen)
        val vLenInt = h.vLen.toIntExact() // should fit; current chunk size <= block size
        val valSlice = sliceAt(valOff, vLenInt)
        return h to (keySlice to valSlice)
    }
}
