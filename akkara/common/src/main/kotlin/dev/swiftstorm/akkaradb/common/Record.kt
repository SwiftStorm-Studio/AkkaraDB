@file:OptIn(ExperimentalStdlibApi::class)
package dev.swiftstorm.akkaradb.common

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * Immutable key–value pair inside AkkaraDB.
 *
 *  * Identity = (seqNo, key)
 *  * value は equals/hashCode 非対象
 */
@ConsistentCopyVisibility
data class Record private constructor(
    val key: ByteBuffer,   // read-only, position=0
    val value: ByteBuffer,   // read-only, position=0
    val seqNo: Long,
    val keyHash: Int,        // pre-computed hash of key
) {

    /* ────────────── public ctors ────────────── */

    constructor(rawKey: ByteBuffer, rawValue: ByteBuffer, seqNo: Long) : this(
        rawKey.readOnly0(),
        rawValue.readOnly0(),
        seqNo,
        rawKey.readOnly0().contentHash()
    )

    constructor(key: String, value: String, seqNo: Long) :
            this(StandardCharsets.UTF_8.encode(key), StandardCharsets.UTF_8.encode(value), seqNo)

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

    override fun toString(): String =
        "Record(seq=$seqNo, key=${key.previewUtf8()}, value=${value.limit()}B)"
}

/* ════════════ private helpers ════════════ */
private fun ByteBuffer.readOnly0(): ByteBuffer =
    asReadOnlyBuffer().apply { rewind() }

private fun ByteBuffer.contentEquals(other: ByteBuffer): Boolean =
    limit() == other.limit() && mismatch(other) == -1

private fun ByteBuffer.contentHash(): Int =
    duplicate().apply { rewind() }.hashCode()

private fun ByteBuffer.previewUtf8(max: Int = 16): String =
    if (limit() <= max)
        StandardCharsets.UTF_8.decode(duplicate().apply { rewind() }).toString()
    else "${limit()}B"
