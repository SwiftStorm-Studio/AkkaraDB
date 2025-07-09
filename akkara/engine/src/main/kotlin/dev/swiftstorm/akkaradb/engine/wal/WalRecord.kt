package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.codec.VarIntCodec
import java.nio.ByteBuffer

sealed interface WalRecord {
    fun writeTo(buf: ByteBuffer)

    /* --------------- codec --------------- */

    companion object Codec {
        private const val TAG_ADD = 1
        private const val TAG_SEAL = 2
        private const val TAG_CHECKPOINT = 3

        fun readFrom(buf: ByteBuffer): WalRecord {
            val tag = VarIntCodec.readInt(buf)
            return when (tag) {
                TAG_ADD -> {
                    val len = VarIntCodec.readInt(buf)
                    val payload = buf.slice().apply { limit(len) }
                    buf.position(buf.position() + len)
                    Add(payload.asReadOnlyBuffer())
                }

                TAG_SEAL -> Seal
                TAG_CHECKPOINT -> CheckPoint(
                    VarIntCodec.readLong(buf),
                    VarIntCodec.readLong(buf)
                )

                else -> error("Unknown WAL tag=$tag")
            }
        }
    }

    /* ------------- concrete types ------------- */

    /** MemTable entry (encoded AkkRecord payload). */
    data class Add(val payload: ByteBuffer) : WalRecord {
        override fun writeTo(buf: ByteBuffer) {
            VarIntCodec.writeInt(buf, TAG_ADD)
            VarIntCodec.writeInt(buf, payload.remaining())
            buf.put(payload.duplicate())
        }
    }

    /** Segment boundary (all previous records are durable). */
    data object Seal : WalRecord {
        override fun writeTo(buf: ByteBuffer) {
            VarIntCodec.writeInt(buf, TAG_SEAL)        // ← tag のみ
        }
    }

    /** Check-point: {stripeIdx, lastSeqNo}. */
    data class CheckPoint(val stripeIdx: Long, val seqNo: Long) : WalRecord {
        override fun writeTo(buf: ByteBuffer) {
            VarIntCodec.writeInt(buf, TAG_CHECKPOINT)
            VarIntCodec.writeLong(buf, stripeIdx)
            VarIntCodec.writeLong(buf, seqNo)
        }
    }
}
