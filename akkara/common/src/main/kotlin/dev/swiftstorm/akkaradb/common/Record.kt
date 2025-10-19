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
@file:Suppress("NOTHING_TO_INLINE")

package dev.swiftstorm.akkaradb.common

import kotlin.experimental.inv

/**
 * In-memory KV entry for MemTable and read-side indexes.
 * Decoupled from on-disk TLV encoding (see format.api.RecordView).
 *
 * Keep this tiny & hot: it sits on the put/get hot path.
 */
data class MemRecord(
    val key: ByteBufferL,       // usually a read-only slice or a pooled copy
    val value: ByteBufferL,     // empty when tombstone
    val seq: Long,              // global, monotonically increasing
    val flags: Byte,            // see RecordFlags
    val keyHash: Int,           // cached 32-bit hash (map bucketing / tries)
    val approxSizeBytes: Int    // memory accounting for flush thresholds
) {
    inline val tombstone: Boolean get() = flags.hasFlag(RecordFlags.TOMBSTONE)

    /** Copy with a new value, re-accounting size. */
    fun withValue(newValue: ByteBufferL): MemRecord =
        copy(value = newValue, approxSizeBytes = estimateMemFootprint(key, newValue))

    /** Convert to a tombstone (empty value), keeping key/seq. */
    fun asTombstone(): MemRecord =
        copy(
            value = EMPTY,
            flags = flags.withFlag(RecordFlags.TOMBSTONE),
            approxSizeBytes = estimateMemFootprint(key, EMPTY)
        )

    companion object {
        /** Zero-length heap buffer as an LE-safe wrapper. */
        val EMPTY: ByteBufferL = ByteBufferL.allocate(0, direct = false)
    }
}

/** Compact flag bits for TLV & in-memory records. */
object RecordFlags {
    const val TOMBSTONE: Byte = 0x01
    // Future: const val SECONDARY: Byte = 0x02, const val BLOB_CHUNK: Byte = 0x04, etc.
}

/** Bit helpers. */
inline fun Byte.hasFlag(bit: Byte): Boolean = (this.toInt() and bit.toInt()) != 0
inline fun Byte.withFlag(bit: Byte): Byte = (this.toInt() or bit.toInt()).toByte()
inline fun Byte.withoutFlag(bit: Byte): Byte = (this.toInt() and bit.inv().toInt()).toByte()

/**
 * Factory that computes hash & size consistently.
 */
fun memRecordOf(key: ByteBufferL, value: ByteBufferL, seq: Long, flags: Byte = 0): MemRecord =
    MemRecord(
        key = key,
        value = value,
        seq = seq,
        flags = flags,
        keyHash = fnv1a32(key),
        approxSizeBytes = estimateMemFootprint(key, value)
    )

/**
 * Decide if [candidate] should replace [existing] in the MemTable.
 *
 * Rule set mirrors the requirements:
 *  - Higher sequence numbers always replace lower ones.
 *  - Equal sequence numbers never allow a value to resurrect a tombstone.
 *  - Tombstones with the same sequence do not churn the entry (idempotent delete).
 */
fun shouldReplace(existing: MemRecord?, candidate: MemRecord): Boolean {
    existing ?: return true

    val seqCmp = candidate.seq.compareTo(existing.seq)
    if (seqCmp > 0) return true
    if (seqCmp < 0) return false

    val candidateTombstone = candidate.tombstone
    val existingTombstone = existing.tombstone

    if (candidateTombstone && !existingTombstone) return true
    if (!candidateTombstone && existingTombstone) return false

    // Equal sequence & same tombstone state → keep existing to avoid churn.
    return false
}

/**
 * Byte-wise lexicographic compare (dictionary order), independent of ByteOrder.
 * Uses LE-safe duplicates and property-based relative reads.
 */
fun lexCompare(a: ByteBufferL, b: ByteBufferL): Int {
    val aa = a.duplicate()
    val bb = b.duplicate()
    val n = minOf(aa.remaining, bb.remaining)
    var i = 0
    while (i < n) {
        val da = aa.i8          // 0..255 and advances
        val db = bb.i8
        if (da != db) return da - db
        i++
    }
    return aa.remaining - bb.remaining
}

/** Constant-time style equality (byte-wise). */
fun bytesEqual(a: ByteBufferL, b: ByteBufferL): Boolean {
    val aa = a.duplicate()
    val bb = b.duplicate()
    if (aa.remaining != bb.remaining) return false
    var r = aa.remaining
    while (r > 0) {
        if (aa.i8 != bb.i8) return false
        r--
    }
    return true
}

/** Simple & fast 32-bit FNV-1a for hashing ByteBufferL contents. */
fun fnv1a32(buf: ByteBufferL): Int {
    var h = 0x811C9DC5u
    val bb = buf.duplicate()
    var r = bb.remaining
    while (r > 0) {
        h = (h xor bb.i8.toUInt()) * 0x01000193u
        r--
    }
    return h.toInt()
}

/** Coarse but stable sizing for memory budgeting. */
fun estimateMemFootprint(key: ByteBufferL, value: ByteBufferL): Int {
    val base = 48 // struct & refs overhead (rule-of-thumb)
    return base + key.remaining + value.remaining
}
