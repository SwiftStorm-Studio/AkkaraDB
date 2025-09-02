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
 * Serialises a [Record] into a binary representation understood by the
 * Block-/Stripe-layer. Implementations **must not** perform any I/O;
 * they only fill the supplied buffer or return a `ByteArray`.
 */
interface RecordWriter {

    /**
     * Encodes [record] into the provided [ByteBufferL].
     *
     * @param record The logical record to encode.
     * @param dest   Destination buffer positioned at the write offset.
     * @return Number of bytes written.
     * @throws java.nio.BufferOverflowException if the buffer is too small.
     */
    fun write(record: Record, dest: ByteBufferL): Int

    /**
     * Calculates an upper bound on the encoded size of [record].
     * Implementations should be fast (<50 ns) and side-effect free.
     */
    fun computeMaxSize(record: Record): Int
}
