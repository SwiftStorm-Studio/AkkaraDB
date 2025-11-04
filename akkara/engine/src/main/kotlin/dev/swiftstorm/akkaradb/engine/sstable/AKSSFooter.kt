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

package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.nio.channels.SeekableByteChannel
import java.util.zip.CRC32C

/**
 * AKSS footer (32 bytes, Little-Endian), unified definition for reader & writer.
 *
 * Layout:
 *  0..3   : magic 'AKSS' (u32)
 *  4      : ver (u8) = 1
 *  5..7   : pad (u24) = 0
 *  8..15  : indexOff (u64)
 *  16..23 : bloomOff (u64) -- 0 if none
 *  24..27 : entries  (u32)
 *  28..31 : crc32c   (u32) -- CRC32C over [0 .. fileSize-4), 0 = omitted
 */
object AKSSFooter {
    const val SIZE: Int = 32
    const val MAGIC: Int = 0x414B5353 // 'A''K''S''S'
    const val VER: Int = 1

    data class Footer(
        val indexOff: Long,
        val bloomOff: Long,
        val entries: Int,
        val ver: Int = VER
    )

    /** Writes a footer into [dst] at its current position and advances by 32 bytes. */
    fun writeTo(
        dst: ByteBufferL,
        indexOff: Long,
        bloomOff: Long,
        entries: Int,
        crc32c: Int = 0
    ): ByteBufferL {
        val start = dst.position
        dst.at(start + 0).i32 = MAGIC
        dst.at(start + 4).i8 = VER
        dst.at(start + 5).i8 = 0
        dst.at(start + 6).i8 = 0
        dst.at(start + 7).i8 = 0
        dst.at(start + 8).i64 = indexOff
        dst.at(start + 16).i64 = bloomOff
        dst.at(start + 24).i32 = entries
        dst.at(start + 28).i32 = crc32c
        dst.position(start + SIZE)
        return dst
    }

    /**
     * Parses the footer from [buf] (LE) and optionally verifies file-level CRC32C.
     *
     * If [verifyCrc] is true and the stored CRC is non-zero, computes CRC32C over bytes
     * [0 .. fileSize-4) and compares. The channel position is preserved.
     */
    fun readFrom(
        buf: ByteBufferL,
        verifyCrc: Boolean = false,
        ch: SeekableByteChannel? = null,
        fileSize: Long = -1L
    ): Footer {
        require(buf.remaining >= SIZE) { "footer too small" }

        val magic = buf.at(0).i32
        require(magic == MAGIC) { "bad footer magic: 0x${magic.toString(16)}" }

        val ver = buf.at(4).i8
        require(ver == VER) { "unsupported footer ver=$ver" }

        val indexOff = buf.at(8).i64
        val bloomOff = buf.at(16).i64
        val entries = buf.at(24).i32
        val storedCrc = buf.at(28).i32 // 0 means “not set”

        require(indexOff >= 0) { "indexOff must be >= 0" }

        if (fileSize >= SIZE) {
            require(indexOff < fileSize - SIZE) { "indexOff out of bounds: $indexOff (fileSize=$fileSize)" }
            if (bloomOff > 0) {
                require(bloomOff > indexOff) { "bloomOff must be after indexOff" }
                require(bloomOff < fileSize - SIZE) { "bloomOff out of bounds: $bloomOff (fileSize=$fileSize)" }
            }
        }

        if (verifyCrc && storedCrc != 0) {
            require(ch != null && fileSize > 0) { "CRC verification requires channel and fileSize" }
            verifyWholeFileCrc32C(ch, fileSize, storedCrc)
        }

        return Footer(indexOff, bloomOff, entries, ver)
    }

    /**
     * Streams the file via a ByteBufferL scratch and verifies CRC32C over [0 .. size-4).
     * Uses only ByteBufferL in user-facing code; internal CRC updates use the wrapped buffer.
     */
    private fun verifyWholeFileCrc32C(ch: SeekableByteChannel, size: Long, want: Int) {
        val oldPos = ch.position()
        try {
            val limit = size - 4
            val scratch = ByteBufferL.allocate(1 shl 20) // 1 MiB
            val crc = CRC32C()

            ch.position(0L)
            var readTotal = 0L
            while (readTotal < limit) {
                val toRead = kotlin.math.min(scratch.capacity.toLong(), limit - readTotal).toInt()
                scratch.clear()
                scratch.limit(toRead)
                // fill scratch completely
                scratch.readFully(ch, toRead)

                val bb = scratch.sliceAt(0, toRead) // internal ByteBuffer; no public exposure
                bb.position(0).limit(toRead)
                crc.update(bb.rawDuplicate())

                readTotal += toRead
            }

            val have = crc.value.toInt()
            require(have == want) { "footer CRC32C mismatch (have=$have, want=$want)" }
        } finally {
            ch.position(oldPos)
        }
    }

}
