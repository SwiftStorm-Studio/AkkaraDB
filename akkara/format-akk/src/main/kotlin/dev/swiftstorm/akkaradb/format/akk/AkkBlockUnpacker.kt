package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException
import java.nio.ByteBuffer
import java.util.zip.CRC32C

/**
 * Split a 32 KiB *block* produced by [AkkBlockPackerDirect] into
 * its individual record-payload slices.
 *
 * *Input layout*
 * ```
 * [4B len] [payload bytes ……………] [0-padding …] [4B CRC32C]
 *  ^                               ^
 *  |                               +-- PAYLOAD_LIMIT + 4
 *  +-- BLOCK header
 * ```
 *
 * *Output*
 *  – Each record is returned as a **read-only slice** that shares the same
 *    backing memory with the original block. The caller remains responsible
 *    for releasing the original `block` back to the [BufferPool] *after*
 *    it is sure the upper layers no longer access those slices.
 *
 * Memory-safety: **The block is released immediately if a corruption
 * is detected (length/CRC mismatch); otherwise ownership stays with the
 * caller.**
 */
class AkkBlockUnpacker(
    private val pool: BufferPool = Pools.io()
) {

    private val crc = CRC32C()

    fun unpack(block: ByteBuffer): List<ByteBuffer> {
        try {
            /* -------- 1. header -------- */
            if (block.remaining() != BLOCK_SIZE)
                throw CorruptedBlockException("block size != 32 KiB (was ${block.remaining()})")

            val payloadLen = block.getInt(0)
            if (payloadLen < 0 || payloadLen > PAYLOAD_LIMIT)
                throw CorruptedBlockException("payloadLen=$payloadLen out of bounds")

            /* -------- 2. CRC check -------- */
            crc.reset()
            val dup = block.duplicate()
            dup.limit(4 + payloadLen).position(0)   // [len][payload]
            crc.update(dup)

            val storedCrc = block.getInt(PAYLOAD_LIMIT + 4)
            if (crc.value.toInt() != storedCrc)
                throw CorruptedBlockException("CRC mismatch (calc=${crc.value} stored=$storedCrc)")

            /* -------- 3. slice payload area -------- */
            val payloadArea = block.duplicate()
                .position(4)                 // skip len
                .limit(4 + payloadLen)
                .slice()
                .asReadOnlyBuffer()

            /* -------- 4. split into records -------- */
            val records = ArrayList<ByteBuffer>(16)
            var pos = 0
            while (pos < payloadLen) {
                val recLen = payloadArea.getInt(pos)
                if (recLen <= 0 || pos + 4 + recLen > payloadLen)
                    throw CorruptedBlockException("record length out of range (recLen=$recLen pos=$pos)")

                val rec = payloadArea.duplicate()
                    .position(pos + 4)
                    .limit(pos + 4 + recLen)
                    .slice()
                    .asReadOnlyBuffer()

                records += rec
                pos += 4 + recLen
            }

            return records
        } catch (ex: Exception) {
            /* On any corruption, immediately recycle the buffer */
            pool.release(block)
            throw ex
        }
    }
}
