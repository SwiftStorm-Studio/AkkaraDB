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

package dev.swiftstorm.akkaradb.engine.sstable.bs

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.engine.sstable.BlockSearcher

/* =======================================================================
 * UnpackerBlockSearcher
 * -----------------------------------------------------------------------
 * Thin adapter that delegates to the existing AkkBlockUnpacker-based cursor.
 * Keep this around if you prefer to route through your Unpacker.
 * ======================================================================= */
object UnpackerBlockSearcher : BlockSearcher {
    override fun find(blockBuf32k: ByteBufferL, key: ByteBufferL): ByteBufferL? {
        // Build a forward cursor and lower_bound by manual binary search using StandardBlockSearcher
        // to avoid depending on Cursor having random access methods.
        return StandardBlockSearcher.find(blockBuf32k, key)
    }

    override fun iter(blockBuf32k: ByteBufferL, startKey: ByteBufferL): Sequence<Pair<ByteBufferL, ByteBufferL>> =
        StandardBlockSearcher.iter(blockBuf32k, startKey)
}