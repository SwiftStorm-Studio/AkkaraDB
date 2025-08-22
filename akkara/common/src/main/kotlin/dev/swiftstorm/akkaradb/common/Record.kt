@file:OptIn(ExperimentalStdlibApi::class)
package dev.swiftstorm.akkaradb.common

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import kotlin.experimental.or

/**
 * Represents a record with a key, value, sequence number, key hash, and flags.
 * This class is immutable and provides utility methods for working with records.
 *
 * @property key The read-only key of the record (position=0).
 * @property value The read-only value of the record (position=0).
 * @property seqNo The sequence number of the record.
 * @property keyHash The pre-computed hash of the key.
 * @property flags Packed flags for the record (e.g., bit0=tombstone).
 */
@ConsistentCopyVisibility
data class Record private constructor(
    val key: ByteBuffer,     // read-only, position=0
    val value: ByteBuffer,   // read-only, position=0
    val seqNo: Long,
    val keyHash: Int,        // pre-computed hash of key
    val flags: Byte = 0,     // packed flags (bit0=tombstone)
) {

    /* ────────────── public ctors ────────────── */

    constructor(rawKey: ByteBuffer, rawValue: ByteBuffer, seqNo: Long, flags: Byte = 0) : this(
        rawKey.clone().readOnly0(),
        rawValue.clone().readOnly0(),
        seqNo,
        rawKey.readOnly0().contentHash(),
        flags
    )

    constructor(key: String, value: String, seqNo: Long, flags: Byte = 0) :
            this(StandardCharsets.UTF_8.encode(key), StandardCharsets.UTF_8.encode(value), seqNo, flags)

    /* ────────────── flags helpers ────────────── */

    val isTombstone: Boolean
        get() = (flags.toInt() and FLAG_TOMBSTONE.toInt()) != 0

    fun withFlags(newFlags: Byte): Record = copy(flags = newFlags)

    fun asTombstone(): Record = copy(flags = (flags or FLAG_TOMBSTONE))

    /* ────────────── equality / hash ────────────── */

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Record) return false
        if (seqNo != other.seqNo || keyHash != other.keyHash) return false
        return key.contentEquals(other.key)
    }

    override fun hashCode(): Int =
        31 * ((seqNo xor (seqNo ushr 32)).toInt()) + keyHash

    /* ────────────── debug ────────────── */

    override fun toString(): String {
        val tomb = if (isTombstone) ", tombstone" else ""
        return "Record(seq=$seqNo, key=${key.previewUtf8()}, value=${value.limit()}B$tomb)"
    }

    companion object {
        /** bit0 = tombstone */
        const val FLAG_TOMBSTONE: Byte = 0x01
    }
}

/* ════════════ private helpers ════════════ */
private fun ByteBuffer.readOnly0(): ByteBuffer =
    asReadOnlyBuffer().apply { rewind() }

private fun ByteBuffer.clone(): ByteBuffer {
    val src = this.duplicate()
    src.clear()
    src.position(this.position())
    src.limit(this.limit())

    val dst = Pools.io().get(src.remaining())
    dst.put(src)
    dst.flip()
    return dst
}

private fun ByteBuffer.contentEquals(other: ByteBuffer): Boolean =
    limit() == other.limit() && mismatch(other) == -1

private fun ByteBuffer.contentHash(): Int =
    duplicate().apply { rewind() }.hashCode()

private fun ByteBuffer.previewUtf8(max: Int = 16): String =
    if (limit() <= max)
        StandardCharsets.UTF_8.decode(duplicate().apply { rewind() }).toString()
    else "${limit()}B"