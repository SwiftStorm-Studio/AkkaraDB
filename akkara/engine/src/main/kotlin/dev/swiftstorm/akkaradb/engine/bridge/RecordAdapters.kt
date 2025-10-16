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

package dev.swiftstorm.akkaradb.engine.bridge

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.MemRecord
import dev.swiftstorm.akkaradb.common.types.U64
import dev.swiftstorm.akkaradb.format.api.BlockPacker
import kotlin.math.min

/**
 * Append a MemRecord via the *format* API by adapting JVM-native types (Long/Byte)
 * to unsigned types (U64) and computing header-derived fields (keyFP64, miniKey).
 *
 * Keeps unsigned types out of MemRecord and confines them to the format boundary.
 */
fun BlockPacker.tryAppendMem(mem: MemRecord): Boolean {
    // Normalize flags to u8 range
    val flagsU8: Int = mem.flags.toInt() and 0xFF

    // Convert JVM signed long -> U64 (same bits, no reinterpretation)
    val seqU64 = U64.fromSigned(mem.seq)

    // Derive header helpers from key slice
    val keyFp64 = U64(fnv1a64(mem.key))
    val miniKey = U64(miniKeyLE8(mem.key))

    val key = mem.key.duplicate()
    val value =
        if (mem.tombstone) EMPTY_L
        else mem.value.duplicate()

    return this.tryAppend(key, value, seqU64, flagsU8, keyFp64, miniKey)
}

/** Read-only zero-length buffer for tombstones (LE-safe wrapper). */
private val EMPTY_L: ByteBufferL = ByteBufferL.allocate(0, direct = false)

/**
 * 64-bit FNV-1a over ByteBufferL (fast, non-cryptographic).
 * Suitable as a lightweight key fingerprint for AKHdr32.keyFP64.
 */
private fun fnv1a64(buf: ByteBufferL): Long {
    var h = 0xcbf29ce484222325UL // FNV offset basis (64-bit)
    val bb = buf.duplicate()
    while (bb.has()) {
        val b = bb.i8.toUByte()
        h = (h xor b.toULong()) * 0x00000100000001B3UL // FNV prime 64
    }
    return h.toLong()
}

/**
 * Mini-key: first ≤8 bytes of key as little-endian unsigned 64.
 * Short keys are zero-padded toward the high bytes.
 */
private fun miniKeyLE8(buf: ByteBufferL): Long {
    val bb = buf.duplicate()
    val n = min(8, bb.remaining)
    var acc = 0UL
    var i = 0
    while (i < n) {
        acc = acc or ((bb.i8.toUByte().toULong()) shl (8 * i))
        i++
    }
    return acc.toLong()
}
