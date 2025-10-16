package dev.swiftstorm.akkaradb.format.api

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.types.U32
import dev.swiftstorm.akkaradb.common.types.U64

/**
 * Read-only, zero-copy view over a TLV inside a block payload.
 * TLV (LE): [u16 kLen][u32 vLen][u64 seq][u8 flags][key][value]
 *
 * NOTE: key/value reference the original 32 KiB block buffer. Do NOT release() them;
 * only the original block buffer is managed by the buffer pool.
 */
data class RecordView(
    val key: ByteBufferL,
    val value: ByteBufferL,
    val seq: U64,
    val flags: Int,
    val kLen: Int,
    val vLen: U32,
    val totalLen: Int
)

/** Forward-only iterator over TLVs inside the payload. */
interface RecordCursor {
    fun hasNext(): Boolean
    fun next(): RecordView                    // throws on malformed/end
    fun tryNext(): RecordView?                // null on malformed/end
}
