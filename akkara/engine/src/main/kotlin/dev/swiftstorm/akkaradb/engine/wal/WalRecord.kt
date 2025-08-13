package dev.swiftstorm.akkaradb.engine.wal

import java.nio.ByteBuffer
import java.nio.ByteOrder

sealed interface WalRecord {
    fun writeTo(buf: ByteBuffer)

    companion object Codec {
        private const val TAG_ADD: Byte = 1
        private const val TAG_SEAL: Byte = 2
        private const val TAG_CHECKPOINT: Byte = 3

        /**
         * Fixed-length binary codec (little-endian):
         *  - ADD:        [tag:u8=1][len:u32][payload]
         *  - SEAL:       [tag:u8=2]
         *  - CHECKPOINT: [tag:u8=3][stripeIdx:u64][seqNo:u64]
         */
        fun readFrom(buf: ByteBuffer): WalRecord {
            val tag = buf.get()
            buf.order(ByteOrder.LITTLE_ENDIAN)
            return when (tag) {
                TAG_ADD -> {
                    val len = buf.int
                    require(len >= 0 && len <= buf.remaining()) { "invalid ADD len=$len" }
                    val payload = buf.slice().apply { limit(len) }
                    buf.position(buf.position() + len)
                    Add(payload.asReadOnlyBuffer())
                }

                TAG_SEAL -> Seal
                TAG_CHECKPOINT -> CheckPoint(
                    buf.long,
                    buf.long
                )

                else -> error("Unknown WAL tag=$tag @pos=${buf.position() - 1}")
            }
        }
    }

    /* ------------- concrete types ------------- */

    /** MemTable entry (encoded AkkRecord payload). */
    data class Add(val payload: ByteBuffer) : WalRecord {
        override fun writeTo(buf: ByteBuffer) {
            buf.put(TAG_ADD)
            buf.order(ByteOrder.LITTLE_ENDIAN)
            buf.putInt(payload.remaining())
            buf.put(payload.duplicate())
        }
    }

    /** Segment boundary (all previous records are durable). */
    data object Seal : WalRecord {
        override fun writeTo(buf: ByteBuffer) {
            buf.put(TAG_SEAL)
        }
    }

    /** Check-point: {stripeIdx, lastSeqNo}. */
    data class CheckPoint(val stripeIdx: Long, val seqNo: Long) : WalRecord {
        override fun writeTo(buf: ByteBuffer) {
            buf.put(TAG_CHECKPOINT)
            buf.order(ByteOrder.LITTLE_ENDIAN)
            buf.putLong(stripeIdx)
            buf.putLong(seqNo)
        }
    }
}
