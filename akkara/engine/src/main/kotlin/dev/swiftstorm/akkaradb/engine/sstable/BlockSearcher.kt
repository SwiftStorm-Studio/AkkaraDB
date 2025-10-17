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

/**
 * BlockSearcher
 *
 * Strategy interface for searching inside a single 32 KiB data block:
 *  - [find]: point lookup, returns value slice or null
 *  - [iter]: range iteration starting at >= startKey within the block
 *
 * Implementations may parse/build a mini-index on the fly.
 */
interface BlockSearcher {
    fun find(blockBuf32k: ByteBufferL, key: ByteBufferL): ByteBufferL?
    fun iter(blockBuf32k: ByteBufferL, startKey: ByteBufferL): Sequence<Pair<ByteBufferL, ByteBufferL>>
}