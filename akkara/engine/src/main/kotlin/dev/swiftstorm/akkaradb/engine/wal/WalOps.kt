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
import dev.swiftstorm.akkaradb.common.types.U32
import dev.swiftstorm.akkaradb.common.types.U64


/**
 * WAL operation payloads (encoded inside the v3 framing).
 *
 * For simplicity and perf, we reuse the on‑disk AKHdr32 + key + value format for ADD/DEL.
 * - ADD: AKHdr32{ kLen, vLen, seq, flags, keyFP64, miniKey } + key + value
 * - DEL: same header with TOMBSTONE flag and value length = 0 (no value bytes)
 */
sealed interface WalOp {
    fun encodeInto(buf: ByteBufferL)


    data class Add(
        val key: ByteArray,
        val value: ByteArray,
        val seq: U64,
        val flags: Int = 0,
        val keyFP64: U64 = U64.ZERO,
        val miniKey: U64 = AKHdr32.buildMiniKeyLE(key)
    ) : WalOp {
        override fun encodeInto(buf: ByteBufferL) {
            buf.putHeader32(key.size, U32.of(value.size.toLong()), seq, flags, keyFP64, miniKey)
            buf.putBytes(key)
            buf.putBytes(value)
        }
    }


    data class Delete(
        val key: ByteArray,
        val seq: U64,
        val tombstoneFlag: Int = 0x01,
        val keyFP64: U64 = U64.ZERO,
        val miniKey: U64 = AKHdr32.buildMiniKeyLE(key)
    ) : WalOp {
        override fun encodeInto(buf: ByteBufferL) {
            buf.putHeader32(key.size, U32.ZERO, seq, tombstoneFlag, keyFP64, miniKey)
            buf.putBytes(key)
// no value bytes
        }
    }
}