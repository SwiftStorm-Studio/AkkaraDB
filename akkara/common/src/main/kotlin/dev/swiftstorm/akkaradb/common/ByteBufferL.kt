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
 *  - Position/limit management lives here; all primitive I/O delegates to [LE] (VarHandle-based).
 *  - AkkaraDB-specific 32B header read/write delegates to [AKHdr32].
 *  - No view buffers on hot paths; only duplicate/slice used for safe slicing.
 *
 * Typical usage:
 *  - WAL/Stripe writer:
 *      val b = ByteBufferL.wrap(poolBuf)
 *      val fp = AKHdr32.sipHash24(keyBytes, seed64)
 *      val mk = AKHdr32.buildMiniKeyLE(keyBytes)
 *      b.putHeader32(kLen = keyBytes.size, vLen = valueLen.toLong(), seq, flags, fp, mk)
 *       .putBytes(keyBytes)
 *       .putBytes(valueBytes)
 *       .align(32 * 1024)
 *  - Reader:
 *      val h = b.readHeader32(recOff)
 *      val keyOff = recOff + AKHdr32.SIZE
 *      val valOff = keyOff + h.kLen
 *      val keySlice = b.sliceAt(keyOff, h.kLen)
 *      val valSlice = b.sliceAt(valOff, h.vLen)
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

    // ---------------- Basics (state) ----------------
    inline fun capacity(): Int = buf.capacity()
    inline fun position(): Int = buf.position()
    inline fun position(newPos: Int): ByteBufferL {
        buf.position(newPos); return this
    }

    inline fun limit(): Int = buf.limit()
    inline fun limit(newLimit: Int): ByteBufferL {
        buf.limit(newLimit); return this
    }

    inline fun remaining(): Int = buf.remaining()
    inline fun clear(): ByteBufferL {
        buf.clear(); return this
    }

    inline fun flip(): ByteBufferL {
        buf.flip(); return this
    }

    inline fun rewind(): ByteBufferL {
        buf.rewind(); return this
    }

    inline fun mark(): ByteBufferL {
        buf.mark(); return this
    }

    inline fun reset(): ByteBufferL {
        buf.reset(); return this
    }

    inline fun isDirect(): Boolean = buf.isDirect

    /** Expose underlying buffer (position/limit are shared). */
    fun unwrap(): ByteBuffer = buf

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

    // ---------------- Relative primitives (cursor-like; LE delegated) ----------------
    inline fun getU8(): Int = LE.cursor(buf).getU8()
    inline fun putU8(v: Int): ByteBufferL {
        LE.cursor(buf).putU8(v); return this
    }

    inline fun getShort(): Short = LE.cursor(buf).getShort()
    inline fun putShort(v: Short): ByteBufferL {
        LE.cursor(buf).putShort(v); return this
    }

    inline fun getInt(): Int = LE.cursor(buf).getInt()
    inline fun putInt(v: Int): ByteBufferL {
        LE.cursor(buf).putInt(v); return this
    }

    inline fun getLong(): Long = LE.cursor(buf).getLong()
    inline fun putLong(v: Long): ByteBufferL {
        LE.cursor(buf).putLong(v); return this
    }

    inline fun getFloat(): Float = LE.cursor(buf).getFloat()
    inline fun putFloat(v: Float): ByteBufferL {
        LE.cursor(buf).putFloat(v); return this
    }

    inline fun getDouble(): Double = LE.cursor(buf).getDouble()
    inline fun putDouble(v: Double): ByteBufferL {
        LE.cursor(buf).putDouble(v); return this
    }

    fun putBytes(src: ByteArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        LE.cursor(buf).putBytes(src, off, len); return this
    }

    // ---------------- Absolute primitives (LE delegated) ----------------
    inline fun getU8(at: Int): Int = LE.getU8(buf, at)
    inline fun putU8(at: Int, v: Int): ByteBufferL {
        LE.putU8(buf, at, v); return this
    }

    inline fun getShort(at: Int): Short = LE.getShort(buf, at)
    inline fun putShort(at: Int, v: Short): ByteBufferL {
        LE.putShort(buf, at, v); return this
    }

    inline fun getInt(at: Int): Int = LE.getInt(buf, at)
    inline fun putInt(at: Int, v: Int): ByteBufferL {
        LE.putInt(buf, at, v); return this
    }

    inline fun getLong(at: Int): Long = LE.getLong(buf, at)
    inline fun putLong(at: Int, v: Long): ByteBufferL {
        LE.putLong(buf, at, v); return this
    }

    inline fun getFloat(at: Int): Float = LE.getFloat(buf, at)
    inline fun putFloat(at: Int, v: Float): ByteBufferL {
        LE.putFloat(buf, at, v); return this
    }

    inline fun getDouble(at: Int): Double = LE.getDouble(buf, at)
    inline fun putDouble(at: Int, v: Double): ByteBufferL {
        LE.putDouble(buf, at, v); return this
    }

    // ---------------- AkkaraDB header bridge (32B) ----------------
    /** Absolute write of 32B header. vLen is Long but must be within u32. */
    fun putHeader32(at: Int, kLen: Int, vLen: Long, seq: Long, flags: Int, keyFP64: Long, miniKey: Long): ByteBufferL {
        AKHdr32.write(buf, at, kLen, vLen, seq, flags, keyFP64, miniKey); return this
    }

    /** Relative write of 32B header at current position; advances by 32. */
    fun putHeader32(kLen: Int, vLen: Long, seq: Long, flags: Int, keyFP64: Long, miniKey: Long): ByteBufferL {
        AKHdr32.writeRel(buf, kLen, vLen, seq, flags, keyFP64, miniKey); return this
    }

    /** Absolute read of 32B header. */
    fun readHeader32(at: Int): AKHdr32.Header = AKHdr32.read(buf, at)

    /** Relative read of 32B header at current position; advances by 32. */
    fun readHeader32(): AKHdr32.Header = AKHdr32.readRel(buf)

    // ---------------- Bulk (aligned) ----------------
    fun putInts(at: Int, src: IntArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        LE.putInts(buf, at, src, off, len); return this
    }

    fun putLongs(at: Int, src: LongArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        LE.putLongs(buf, at, src, off, len); return this
    }

    fun putInts(src: IntArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        LE.cursor(buf).putInts(src, off, len); return this
    }

    fun putLongs(src: LongArray, off: Int = 0, len: Int = src.size - off): ByteBufferL {
        LE.cursor(buf).putLongs(src, off, len); return this
    }

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
    fun writeFully(ch: WritableByteChannel, len: Int = remaining()): Int = LE.writeFully(ch, buf, len)

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
        seq: Long,
        flags: Int,
        keyFP64: Long,
        miniKey: Long
    ): Int {
        val hdrPos = position()
        putHeader32(key.size, value.size.toLong(), seq, flags, keyFP64, miniKey)
        putBytes(key)
        putBytes(value)
        return hdrPos
    }

    /**
     * Read header32 at [recOff] and return slices for key/value.
     * Result pair: (header, Pair(keySlice, valueSlice))
     */
    fun readRecord32(
        recOff: Int
    ): Pair<AKHdr32.Header, Pair<ByteBuffer, ByteBuffer>> {
        val h = readHeader32(recOff)
        val keyOff = recOff + AKHdr32.SIZE
        val valOff = keyOff + h.kLen
        val keySlice = sliceAt(keyOff, h.kLen)
        val valSlice = sliceAt(valOff, h.vLen)
        return h to (keySlice to valSlice)
    }
}
