package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.nio.ByteBuffer
import java.util.zip.CRC32C

/**
 * WAL records (little-endian) with mandatory LSN header.
 *
 * Layout:
 *  - ADD:        [tag:u8=1][lsn:u64][len:u32][payload][crc:u32]   // CRC32C over payload only
 *  - SEAL:       [tag:u8=2][lsn:u64]
 *  - CHECKPOINT: [tag:u8=3][lsn:u64][stripeIdx:u64][seqNo:u64]
 */
sealed interface WalRecord {
    val lsn: Long

    /** Write this record into `buf` at its current position. */
    fun writeTo(buf: ByteBufferL)

    /** Encoded byte size of this record. */
    fun encodedSize(): Int

    /** MemTable entry (encoded AkkRecord payload). */
    data class Add(
        override val lsn: Long,
        val payload: ByteBufferL
    ) : WalRecord {
        override fun encodedSize(): Int {
            val len = payload.asReadOnlyByteBuffer().remaining()
            return 1 /*tag*/ + 8 /*lsn*/ + 4 /*len*/ + len + 4 /*crc*/
        }

        override fun writeTo(buf: ByteBufferL) {
            val view: ByteBuffer = payload.asReadOnlyByteBuffer().slice()
            val len = view.remaining()

            buf.put(TAG_ADD)
            buf.putLong(lsn)
            buf.putInt(len)

            val crc = CRC32C_TL.get().apply {
                reset()
                update(view.duplicate())
            }.value.toInt()

            buf.put(view)          // payload
            buf.putInt(crc)        // crc32c(payload)
        }
    }

    /** Segment boundary (all previous ADDs are persisted). */
    data class Seal(
        override val lsn: Long
    ) : WalRecord {
        override fun encodedSize(): Int = 1 + 8
        override fun writeTo(buf: ByteBufferL) {
            buf.put(TAG_SEAL)
            buf.putLong(lsn)
        }
    }

    /** Check-point: {stripeIdx, lastSeqNo}. */
    data class CheckPoint(
        override val lsn: Long,
        val stripeIdx: Long,
        val seqNo: Long
    ) : WalRecord {
        override fun encodedSize(): Int = 1 + 8 + 8 + 8
        override fun writeTo(buf: ByteBufferL) {
            buf.put(TAG_CHECKPOINT)
            buf.putLong(lsn)
            buf.putLong(stripeIdx)
            buf.putLong(seqNo)
        }
    }

    companion object Codec {
        private const val TAG_ADD: Byte = 1
        private const val TAG_SEAL: Byte = 2
        private const val TAG_CHECKPOINT: Byte = 3

        private val CRC32C_TL = ThreadLocal.withInitial { CRC32C() }

        /** Decode a record from `buf` (advances position). */
        fun readFrom(buf: ByteBufferL): WalRecord {
            val tag = buf.get()
            return when (tag) {
                TAG_ADD -> {
                    buf.requireRemaining(8 + 4) // lsn + len
                    val lsn = buf.long
                    val len = buf.int
                    require(len >= 0) { "negative ADD len=$len" }
                    buf.requireRemaining(len + 4) // payload + crc

                    val payloadView = buf.slice().apply { limit(len) }
                    val payloadRO = payloadView.asReadOnly()
                    buf.advance(len)

                    val storedCrc = buf.int
                    val actualCrc = CRC32C_TL.get().apply {
                        reset()
                        update(payloadRO.asReadOnlyByteBuffer().duplicate())
                    }.value.toInt()
                    check(actualCrc == storedCrc) {
                        "WAL ADD CRC mismatch: expected=$storedCrc actual=$actualCrc"
                    }
                    Add(lsn, payloadRO)
                }

                TAG_SEAL -> {
                    buf.requireRemaining(8)
                    val lsn = buf.long
                    Seal(lsn)
                }

                TAG_CHECKPOINT -> {
                    buf.requireRemaining(8 + 8 + 8)
                    val lsn = buf.long
                    val stripeIdx = buf.long
                    val seqNo = buf.long
                    CheckPoint(lsn, stripeIdx, seqNo)
                }

                else -> error("Unknown WAL tag=$tag @pos=${buf.position - 1}")
            }
        }
    }
}
