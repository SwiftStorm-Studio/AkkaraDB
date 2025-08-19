package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32C

/**
 * Split a 32 KiB block produced by [AkkBlockPackerDirect] into
 * individual record-payload slices (read-only views).
 *
 * Portable on-disk layout (endianness fixed to BIG_ENDIAN):
 *   [0..3]     payloadLen: Int
 *   [4..4+N)   payload bytes (sequence of records: [u32 len][bytes])
 *   [..]       zero padding up to PAYLOAD_LIMIT (ignored on read)
 *   [end-4..]  crc32c over bytes [0 .. 4+payloadLen)
 *
 * Ownership: the caller retains ownership of the backing block buffer and is
 * responsible for releasing it back to the [BufferPool] after consumers stop
 * using the returned slices. This unpacker only releases the block on error.
 */
class AkkBlockUnpacker(
    private val pool: BufferPool = Pools.io()
) {
    private val crc = CRC32C()

    fun unpack(block: ByteBuffer): List<ByteBuffer> {
        // Never mutate the caller's buffer directly
        val base = block.duplicate().order(ByteOrder.BIG_ENDIAN)

        /* -------- 1) basic structure checks -------- */
        if (base.capacity() != BLOCK_SIZE) {
            pool.release(block)
            throw CorruptedBlockException("block capacity != $BLOCK_SIZE (was ${base.capacity()})")
        }

        val payloadLen = base.getInt(0)
        if (payloadLen < 0 || payloadLen > PAYLOAD_LIMIT) {
            pool.release(block)
            throw CorruptedBlockException("payloadLen=$payloadLen out of bounds [0,$PAYLOAD_LIMIT]")
        }

        /* -------- 2) CRC32C over [0 .. 4+payloadLen) -------- */
        crc.reset()
        val crcRegion = base.duplicate().apply {
            position(0); limit(4 + payloadLen)
        }
        crc.update(crcRegion)

        val storedCrc = base.getInt(PAYLOAD_LIMIT + 4)
        if (crc.value.toInt() != storedCrc) {
            pool.release(block)
            throw CorruptedBlockException("CRC mismatch (calc=${crc.value} stored=$storedCrc)")
        }

        /* -------- 3) slice payload area (read-only) -------- */
        val payloadView = base.duplicate().apply {
            position(4); limit(4 + payloadLen)
        }.slice().order(ByteOrder.BIG_ENDIAN).asReadOnlyBuffer()

        /* -------- 4) split into length-prefixed records -------- */
        val records = ArrayList<ByteBuffer>(16)
        var pos = 0
        while (pos < payloadLen) {
            if (pos + 4 > payloadLen) {
                pool.release(block)
                throw CorruptedBlockException("truncated record header at pos=$pos")
            }
            val recLen = payloadView.getInt(pos)
            if (recLen <= 0 || pos + 4 + recLen > payloadLen) {
                pool.release(block)
                throw CorruptedBlockException("record length out of range (recLen=$recLen pos=$pos)")
            }

            val rec = payloadView.duplicate().apply {
                position(pos + 4); limit(pos + 4 + recLen)
            }.slice().asReadOnlyBuffer()

            records += rec
            pos += 4 + recLen
        }

        return records
    }
}
