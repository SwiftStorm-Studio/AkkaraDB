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
 * Parses a fixed-size block (32 KiB) and validates the CRC32.
 * Returns a view (`ByteBuffer.slice()`) of the payload region.
 *
 * Layout  : `[4B len][payload][padding][4B CRC]`
 *
 * Position: block.position() must be 0.
 */
interface BlockUnpacker {

    /** @throws dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException if CRC mismatch or len invalid. */
    fun unpackInto(block: ByteBufferL, out: MutableList<ByteBufferL>): Int
}