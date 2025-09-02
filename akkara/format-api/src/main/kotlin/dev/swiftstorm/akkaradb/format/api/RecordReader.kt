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
import dev.swiftstorm.akkaradb.common.Record

/**
 * Decoder interface for reading a [Record] from a binary buffer.
 *
 * A [RecordReader] interprets the fixed-length TLV-encoded layout used
 * throughout AkkaraDB:
 *
 * ```
 * [kLen: u16][vLen: u32][seq: u64 LE][flags: u8][key][value]
 * ```
 *
 * - All multi-byte fields are little-endian.
 * - The buffer's position must point to the start of a record.
 * - After decoding, the buffer's position will advance by the number of bytes consumed.
 */
interface RecordReader {

    /**
     * Reads a single [Record] from the given buffer.
     *
     * @param buf a [ByteBufferL] whose position points to the start of a record
     * @return a decoded [Record] instance
     * @throws IllegalArgumentException if the buffer does not contain a valid record
     * @throws java.nio.BufferUnderflowException if insufficient bytes remain
     */
    fun read(buf: ByteBufferL): Record
}
