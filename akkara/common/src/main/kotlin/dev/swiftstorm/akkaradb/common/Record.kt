@file:OptIn(ExperimentalStdlibApi::class)
package dev.swiftstorm.akkaradb.common

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * Immutable key–value pair used inside AkkaraDB.
 *
 * ### Identity
 * A record is uniquely identified by the tuple **(seqNo, key)**.
 * The `value` field is *not* considered in `equals` / `hashCode`
 * because the same key at the same `seqNo` is guaranteed to have
 * a single authoritative value.
 *
 * ### Fields
 * @property key        Raw UTF-8 key bytes (read-only buffer, `position == 0`).
 * @property value      Raw UTF-8 value bytes (read-only buffer, `position == 0`).
 * @property seqNo      Monotonically increasing sequence number
 *                      assigned by the storage layer.
 * @property keyHash    Cached hash of `key` (independent of position)
 *                      to accelerate look-ups.
 *
 * ### Usage notes
 * * Prefer the secondary constructor when you have plain `String`s.
 * * If `key` or `value` may contain non-UTF-8 data, convert them
 *   externally and pass the resulting byte arrays here.
 *
 * @author  SwiftStorm - RiriFa
 * @since   0.1.0
 */
@ConsistentCopyVisibility
data class Record private constructor(
    val key: ByteBuffer,
    val value: ByteBuffer,
    val seqNo: Long,
    val keyHash: Int,
) {

    /* ───────────────── public constructors ───────────────── */

    constructor(rawKey: ByteBuffer, rawValue: ByteBuffer, seqNo: Long) : this(
        rawKey.asReadOnlyBuffer().rewound(),
        rawValue.asReadOnlyBuffer().rewound(),
        seqNo,
        rawKey.asReadOnlyBuffer().rewound().absHash(),
    )

    constructor(key: String, value: String, seqNo: Long) :
            this(StandardCharsets.UTF_8.encode(key), StandardCharsets.UTF_8.encode(value), seqNo)

    /* ───────────────── equality & hashing ───────────────── */

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Record) return false
        if (seqNo != other.seqNo) return false
        if (keyHash != other.keyHash) return false
        return key.absEquals(other.key)
    }

    override fun hashCode(): Int = 31 * seqNo.hashCode() + keyHash

    /* ───────────────── debug output ───────────────── */

    override fun toString(): String =
        "Record(seq=$seqNo, key=${key.preview()}, value=${value.limit()}B)"
}

/* ════════════════ helpers ════════════════ */

/** Rewind in-place and return this buffer for fluent usage. */
private fun ByteBuffer.rewound(): ByteBuffer = apply { rewind() }

/** Compare buffers byte-wise from absolute index 0 (position-independent). */
private fun ByteBuffer.absEquals(other: ByteBuffer): Boolean {
    val len = limit()
    if (len != other.limit()) return false
    for (i in 0 until len) {
        if (get(i) != other.get(i)) return false
    }
    return true
}

/** Compute a 31-based hash from absolute index 0 (position-independent). */
private fun ByteBuffer.absHash(): Int {
    var h = 1
    for (i in 0 until limit()) {
        h = 31 * h + (get(i).toInt() and 0xFF)
    }
    return h
}

/** Short preview for log output: UTF-8 string if small, otherwise “{n}B”. */
private fun ByteBuffer.preview(max: Int = 16): String =
    if (limit() <= max)
        StandardCharsets.UTF_8.decode(duplicate().rewound()).toString()
    else
        "${limit()}B"
