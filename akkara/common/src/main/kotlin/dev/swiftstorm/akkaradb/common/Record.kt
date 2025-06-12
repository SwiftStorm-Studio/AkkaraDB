package dev.swiftstorm.akkaradb.common

import java.nio.charset.StandardCharsets

/**
 * Immutable key‑value record with a monotonic sequence number.
 * Key and value are kept as raw byte arrays to avoid redundant copying.
 */
data class Record(
    val key: ByteArray,
    val value: ByteArray,
    val seqNo: Long
) {
    /** Lazily cached UTF‑8 key string. */
    val sKey: String by lazy { key.toString(StandardCharsets.UTF_8) }

    /** Lazily cached UTF‑8 value string. */
    val sValue: String by lazy { value.toString(StandardCharsets.UTF_8) }

    /**
     * Fast equality: sequence number + key content only.
     * This is sufficient because (seqNo,key) is globally unique by design.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Record) return false
        if (seqNo != other.seqNo) return false
        return key.contentEquals(other.key)
    }

    override fun hashCode(): Int = 31 * seqNo.hashCode() + key.contentHashCode()

    companion object {
        fun of(key: String, value: String, seqNo: Long): Record =
            Record(key.toByteArray(StandardCharsets.UTF_8), value.toByteArray(StandardCharsets.UTF_8), seqNo)
    }
}