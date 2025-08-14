package dev.swiftstorm.akkaradb.engine.wal

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32C

/**
 * WAL records with fixed-length binary encoding (little-endian).
 *
 * Layout:
 *  - ADD:        [tag:u8=1][len:u32][payload][crc:u32]
 *                 * crc32c is computed over `payload` bytes only.
 *  - SEAL:       [tag:u8=2]
 *  - CHECKPOINT: [tag:u8=3][stripeIdx:u64][seqNo:u64]
 *
 * Notes:
 *  - All multi-byte integers are LITTLE_ENDIAN.
 *  - The decoder validates payload length bounds and CRC (for ADD).
 */
sealed interface WalRecord {
    fun writeTo(buf: ByteBuffer)

    /* ------------- concrete types ------------- */

    /** MemTable entry (encoded AkkRecord payload). */
    data class Add(val payload: ByteBuffer) : WalRecord {
        override fun writeTo(buf: ByteBuffer) {
            buf.put(TAG_ADD)
            buf.order(ByteOrder.LITTLE_ENDIAN)
            buf.putInt(payload.remaining())
            val slice = payload.duplicate()
            buf.put(slice)
            // CRC32C over payload only
            val c = CRC32C_TL.get().apply { reset(); update(slice.duplicate()) }.value.toInt()
            buf.putInt(c)
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

    companion object Codec {
        private const val TAG_ADD: Byte = 1
        private const val TAG_SEAL: Byte = 2
        private const val TAG_CHECKPOINT: Byte = 3

        private val CRC32C_TL: ThreadLocal<CRC32C> = ThreadLocal.withInitial { CRC32C() }

        /**
         * Decode a single WAL record from `buf` at its current position.
         * Advances `buf.position()` past the record.
         */
        fun readFrom(buf: ByteBuffer): WalRecord {
            val tag = buf.get()
            buf.order(ByteOrder.LITTLE_ENDIAN)
            return when (tag) {
                TAG_ADD -> {
                    require(buf.remaining() >= 4) { "truncated ADD header" }
                    val len = buf.int
                    require(len >= 0) { "negative ADD len=$len" }
                    // require space for payload + crc
                    require(buf.remaining() >= len + 4) { "invalid ADD len=$len > remaining=${buf.remaining() - 4}" }

                    val payload = buf.slice().apply { limit(len) }
                    buf.position(buf.position() + len)

                    val storedCrc = buf.int
                    val c = CRC32C_TL.get().apply { reset(); update(payload.duplicate()) }.value.toInt()
                    if (c != storedCrc) error("WAL ADD CRC mismatch: expected=$storedCrc actual=$c")

                    Add(payload.asReadOnlyBuffer())
                }

                TAG_SEAL -> Seal

                TAG_CHECKPOINT -> {
                    require(buf.remaining() >= 16) { "truncated CHECKPOINT" }
                    val stripeIdx = buf.long
                    val seqNo = buf.long
                    CheckPoint(stripeIdx, seqNo)
                }

                else -> error("Unknown WAL tag=$tag @pos=${buf.position() - 1}")
            }
        }
    }
}
