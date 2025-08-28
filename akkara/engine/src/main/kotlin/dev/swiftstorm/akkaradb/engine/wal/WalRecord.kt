package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.nio.ByteBuffer
import java.util.zip.CRC32C

/**
 * WAL records with fixed-length binary encoding (little-endian).
 *
 * Layout:
 *  - ADD:        [tag:u8=1][len:u32][payload][crc:u32]   // CRC32C over payload only
 *  - SEAL:       [tag:u8=2]
 *  - CHECKPOINT: [tag:u8=3][stripeIdx:u64][seqNo:u64]
 *
 * All multi-byte integers are LITTLE_ENDIAN (ByteBufferL guarantees this).
 */
sealed interface WalRecord {
    fun writeTo(buf: ByteBufferL)

    /** MemTable entry (encoded AkkRecord payload). */
    data class Add(val payload: ByteBufferL) : WalRecord {
        override fun writeTo(buf: ByteBufferL) {
            val view: ByteBuffer = payload.asReadOnlyByteBuffer().slice()
            val len = view.remaining()

            buf.put(TAG_ADD)
            buf.putInt(len)

            val crc = CRC32C_TL.get().apply {
                reset()
                update(view.duplicate())
            }.value.toInt()

            buf.put(view)

            buf.putInt(crc)
        }
    }

    /** Segment boundary (all previous records are durable). */
    data object Seal : WalRecord {
        override fun writeTo(buf: ByteBufferL) {
            buf.put(TAG_SEAL)
        }
    }

    /** Check-point: {stripeIdx, lastSeqNo}. */
    data class CheckPoint(val stripeIdx: Long, val seqNo: Long) : WalRecord {
        override fun writeTo(buf: ByteBufferL) {
            buf.put(TAG_CHECKPOINT)
            buf.putLong(stripeIdx)
            buf.putLong(seqNo)
        }
    }

    companion object Codec {
        private const val TAG_ADD: Byte = 1
        private const val TAG_SEAL: Byte = 2
        private const val TAG_CHECKPOINT: Byte = 3

        private val CRC32C_TL: ThreadLocal<CRC32C> = ThreadLocal.withInitial { CRC32C() }

        /**
         * Decode a single WAL record from `buf` at its current position.
         * Advances `buf.position()` past the record.
         */
        fun readFrom(buf: ByteBufferL): WalRecord {
            val tag = buf.get()
            return when (tag) {
                TAG_ADD -> {
                    buf.requireRemaining(4)
                    val len = buf.int
                    require(len >= 0) { "negative ADD len=$len" }
                    buf.requireRemaining(len + 4)      // payload + crc

                    val payloadView = buf.slice().apply { limit(len) }
                    val payloadRO = payloadView.asReadOnly()

                    buf.advance(len)

                    val storedCrc = buf.int

                    val actualCrc = CRC32C_TL.get().apply {
                        reset()
                        update(payloadRO.asReadOnlyByteBuffer().duplicate())
                    }.value.toInt()

                    if (actualCrc != storedCrc) {
                        error("WAL ADD CRC mismatch: expected=$storedCrc actual=$actualCrc")
                    }

                    Add(payloadRO)
                }

                TAG_SEAL -> Seal

                TAG_CHECKPOINT -> {
                    buf.requireRemaining(16)
                    val stripeIdx = buf.long
                    val seqNo = buf.long
                    CheckPoint(stripeIdx, seqNo)
                }

                else -> error("Unknown WAL tag=$tag @pos=${buf.position - 1}")
            }
        }
    }
}
