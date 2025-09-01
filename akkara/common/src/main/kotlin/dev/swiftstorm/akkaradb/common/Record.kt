@file:OptIn(ExperimentalStdlibApi::class)
package dev.swiftstorm.akkaradb.common

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
    val key: ByteBufferL,     // read-only, pos=0
    val value: ByteBufferL,   // read-only, pos=0
    val seqNo: Long,
    val keyHash: Int,         // pre-computed hash of key
    val flags: Byte = 0,      // packed flags (bit0=tombstone)
) {

    /* ────────────── public ctors ────────────── */
    constructor(key: ByteBufferL, value: ByteBufferL, seqNo: Long, flags: Byte = 0) : this(
        key = key.normalizeForRecord(),
        value = value.normalizeForRecord(),
        seqNo = seqNo,
        keyHash = key.hashOfRemaining(),
        flags = flags
    )

//    constructor(key: String, value: String, seqNo: Long, flags: Byte = 0) : this(
//        encodeUtf8ToPooledLE(key).readOnly0().rewind(),
//        encodeUtf8ToPooledLE(value).readOnly0().rewind(),
//        seqNo,
//        encodeUtf8ToPooledLE(key).readOnly0().contentHashL(),
//        flags
//    )

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
        return key.contentEqualsL(other.key)
    }

    override fun hashCode(): Int =
        31 * ((seqNo xor (seqNo ushr 32)).toInt()) + keyHash

    /* ────────────── debug ────────────── */

    override fun toString(): String {
        val tomb = if (isTombstone) ", tombstone" else ""
        return "Record(seq=$seqNo, key=${key.previewUtf8L()}, value=${value.limit}B$tomb)"
    }

    companion object {
        /** bit0 = tombstone */
        const val FLAG_TOMBSTONE: Byte = 0x01
    }
}

/* ════════════ private helpers (ByteBuffer/ByteBufferL) ════════════ */

private fun ByteBufferL.normalizeForRecord(): ByteBufferL =
    this.slice().asReadOnly()

fun ByteBufferL.hashOfRemaining(): Int {
    val bb = this.asReadOnlyByteBuffer().slice()
    val len = bb.remaining()
    if (len == 0) return 0

    var h = len

    h = 31 * h + (bb.get(0).toInt() and 0xFF)
    h = 31 * h + (bb.get(len / 2).toInt() and 0xFF)
    h = 31 * h + (bb.get(len - 1).toInt() and 0xFF)

    var i = 8
    while (i < len && i < 128) {
        h = 31 * h + (bb.get(i).toInt() and 0xFF)
        i *= 2
    }

    return h
}

private fun ByteBufferL.contentEqualsL(other: ByteBufferL): Boolean {
    val a = this.asReadOnlyByteBuffer().duplicate().apply { rewind() }
    val b = other.asReadOnlyByteBuffer().duplicate().apply { rewind() }
    if (a.limit() != b.limit()) return false
    return a.mismatch(b) == -1
}

private fun ByteBufferL.previewUtf8L(max: Int = 16): String {
    return if (limit <= max) {
        val tmp = this.asReadOnlyByteBuffer().duplicate().apply { rewind() }
        StandardCharsets.UTF_8.decode(tmp).toString()
    } else "${limit}B"
}