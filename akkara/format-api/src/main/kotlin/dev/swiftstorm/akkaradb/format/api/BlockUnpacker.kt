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

/**
 * Unpacks a 32 KiB block built with AKHdr32 records.
 * StripeReader is responsible for CRC verification/repair; the Unpacker only trusts and parses.
 */
interface BlockUnpacker : AutoCloseable {
    /** Forward-only cursor over records inside the payload. */
    fun cursor(block: ByteBufferL): RecordCursor

    /** Materialize zero-copy RecordView slices into [out]. Clears [out] first. */
    fun unpackInto(block: ByteBufferL, out: MutableList<RecordView>)

    override fun close() {}
}