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

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.vh.LE
import java.nio.ByteBuffer
import java.util.zip.CRC32C


/**
 * v3 WAL framing
 *
 * Entry = [len:u32][payload:len bytes][crc32c:u32] (all Little‑Endian)
 * - len covers *payload* only (not header/trailer)
 * - crc32c is over payload only
 * - A *partial* tail (short read) is safely ignored by the reader (v3要件: WAL_TRUNCATED)
 */
object WalFraming {
    private val CRC32C_TL = ThreadLocal.withInitial { CRC32C() }


    /** Allocate a framed buffer for [payload] and return a view positioned at 0, limit=full frame. */
    fun frame(payload: ByteBufferL): ByteBufferL {
        val ro = payload.asReadOnlyDuplicate()
        val len = ro.remaining
        val out = Pools.io().get(4 + len + 4)
        out.i32 = len // len:u32
        // payload
        out.put(ro)
        // crc32c(payload)
        val crc = CRC32C_TL.get().apply {
            reset()
            update(
                ro.duplicate()
                    .limit(ro.position + len)
                    .rawDuplicate()
            )
        }.value.toInt()
        out.i32 = crc
        out.position = 0
        out.limit = 4 + len + 4
        return out
    }


    /** Read one framed entry from [mapped] at current position. Returns payload slice or null if partial tail. */
    fun readOne(mapped: ByteBuffer): ByteBuffer? {
        if (mapped.remaining() < 8) return null // need at least len + crc
        val p0 = mapped.position()
        val len = LE.getU32(mapped, p0).toIntExact()
        val need = 4 + len + 4
        if (mapped.remaining() < need) return null // partial tail → caller stops
        val pPayload = p0 + 4
        val pCrc = pPayload + len
        val payload = mapped.duplicate().apply {
            position(pPayload); limit(pPayload + len)
        }.slice()
        val stored = LE.getU32(mapped, pCrc).raw
        val actual = CRC32C_TL.get().apply { reset(); update(payload.duplicate()) }.value.toInt()
        require(actual == stored) { "WAL CRC32C mismatch: expected=$stored actual=$actual" }
        mapped.position(p0 + need)
        return payload
    }
}