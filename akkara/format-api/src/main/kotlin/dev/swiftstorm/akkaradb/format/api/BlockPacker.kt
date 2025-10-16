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

package dev.swiftstorm.akkaradb.format.api

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.types.U64
import java.io.Closeable

/**
 * Packs TLV records directly into a 32 KiB block using the fixed 32-byte header (AKHdr32).
 *
 * Block layout:
 *  [0..3]     : payloadLen (u32 LE)
 *  [4..4+N)   : payload = repeated records:
 *               header32 (32B) + key + value
 *  [4+N..-5]  : zero padding
 *  [-4..-1]   : CRC32C over bytes [0 .. BLOCK_SIZE-4)
 *
 * Header32 fields (see AKHdr32):
 *   kLen:u16, vLen:u32, seq:u64, flags:u8, pad0:u8, keyFP64:u64, miniKey:u64
 */
interface BlockPacker : Closeable {
    /** Start a new 32 KiB work buffer. If a non-empty block is open, it will be sealed first. */
    fun beginBlock()

    /**
     * Append one record (header32 + key + value) into the current block.
     * Returns false if it wouldn't fit; the caller should endBlock()+beginBlock() and retry.
     *
     * key/value are read in full (copied) from their current positions; callers should not mutate them
     * concurrently. seq/flags/keyFP64/miniKey are copied into the 32-byte header.
     */
    fun tryAppend(
        key: ByteBufferL,
        value: ByteBufferL,
        seq: U64,
        flags: Int,
        keyFP64: U64,
        miniKey: U64
    ): Boolean

    /** Seal and emit the current block (write payloadLen/zero-pad/CRC32C). No-op if payload empty. */
    fun endBlock()

    /** Seal & emit if a non-empty block is open; otherwise no-op. */
    fun flush()
}
