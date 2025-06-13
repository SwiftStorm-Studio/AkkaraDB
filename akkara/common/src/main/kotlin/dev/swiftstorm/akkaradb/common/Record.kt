package dev.swiftstorm.akkaradb.common

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
 * @property key        Raw UTF-8 key bytes.
 * @property value      Raw UTF-8 value bytes.
 * @property seqNo      Monotonically increasing sequence number
 *                      assigned by the storage layer.
 * @property keyHash    Cached hash of `key` to speed up look-ups
 *                      in skip-lists, ARTs, or hash tables.
 *
 * @author  SwiftStorm - RiriFa
 * @since 0.1.0
 *
 *
 * ### Usage notes
 * * Use the secondary constructor when you have plain `String`s.
 * * If the `key` or `value` may contain non-UTF-8 data, convert
 *   them externally and pass the resulting byte arrays here.
 */
data class Record(
    val key: ByteArray,
    val value: ByteArray,
    val seqNo: Long,
    val keyHash: Int = key.contentHashCode()
) {

    /**
     * Convenience constructor for callers that work with UTF-8
     * strings instead of raw byte arrays.
     *
     * @param key   Key string; encoded as UTF-8 internally.
     * @param value Value string; encoded as UTF-8 internally.
     * @param seqNo Sequence number for this record.
     */
    constructor(key: String, value: String, seqNo: Long) :
            this(
                key.toByteArray(StandardCharsets.UTF_8),
                value.toByteArray(StandardCharsets.UTF_8),
                seqNo
            )

    /** Two records are equal when both `seqNo` and `key` match. */
    override fun equals(other: Any?): Boolean =
        other is Record &&
                seqNo == other.seqNo &&
                key.contentEquals(other.key)

    /** Hash based on sequence number and cached key hash. */
    override fun hashCode(): Int =
        31 * seqNo.hashCode() + keyHash
}
