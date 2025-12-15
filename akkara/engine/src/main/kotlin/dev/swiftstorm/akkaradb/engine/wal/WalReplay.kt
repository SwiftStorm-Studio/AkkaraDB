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

package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.AKHdr32
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.RecordFlags
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/** WAL replay for v3 framing. Safe to run on truncated tail. */
object WalReplay {
    data class Result(val applied: Long)

    fun replay(path: Path, mem: MemTable): Result {
        if (!Files.exists(path)) return Result(0)

        val fileSize = Files.size(path)

        if (fileSize == 0L) Result(0)

        var applied = 0L
        FileChannel.open(path, StandardOpenOption.READ).use { ch ->
            val map = ch.map(FileChannel.MapMode.READ_ONLY, 0, fileSize)
            while (true) {
                val payload = WalFraming.readOne(map) ?: break // stop at partial/tail
                // Interpret payload as AKHdr32 + key + value
                val bb = ByteBufferL.wrap(payload)
                val hdr = AKHdr32.readRel(bb.rawDuplicate())
                val keySlice = bb.sliceAt(bb.position, hdr.kLen)
                bb.position += hdr.kLen
                val vLen = hdr.vLen.toIntExact()
                val valSlice = if (vLen > 0) bb.sliceAt(bb.position, vLen) else ByteBufferL.allocate(0, direct = false)
                if ((hdr.flags and RecordFlags.TOMBSTONE.toInt()) != 0) {
                    mem.delete(keySlice, hdr.seq.raw)
                } else {
                    mem.put(keySlice, valSlice, hdr.seq.raw)
                }
                applied++
            }
        }
        return Result(applied)
    }
}