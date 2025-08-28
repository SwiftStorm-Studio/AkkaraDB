package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.BlockUnpacker
import dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException
import java.nio.ByteOrder
import java.util.zip.CRC32C

/**
 * Split a 32 KiB block produced by [AkkBlockPackerDirect] into
 * individual record-payload slices (read-only ByteBufferL views).
 *
 * On-disk layout (ALL LITTLE_ENDIAN):
 *   [0..3]       payloadLen: u32 LE
 *   [4..4+N)     payload bytes (sequence of [u32 LE len][bytes])
 *   [..]         zero padding up to PAYLOAD_LIMIT (ignored on read)
 *   [end-4..]    crc32c (u32 LE) over bytes [0 .. 4+payloadLen)
 *
 * Ownership: the caller retains ownership of the backing block buffer and is
 * responsible for releasing it back to the [BufferPool] after consumers stop
 * using the returned slices. This unpacker only releases the block on error.
 */
class AkkBlockUnpacker(
    private val pool: BufferPool = Pools.io()
) : BlockUnpacker {
    private val crc = CRC32C()

    /** Unpack one fixed-size block into record payload slices (RO/LE, pos=0..limit). */
    override fun unpack(block: ByteBufferL): List<ByteBufferL> {
        // Never mutate the caller's buffer directly
        val base = block.asReadOnlyByteBuffer() // LE is guaranteed by ByteBufferL

        /* -------- 1) basic structure checks -------- */
        if (block.capacity != BLOCK_SIZE) {
            pool.release(block)
            throw CorruptedBlockException("block capacity != $BLOCK_SIZE (was ${block.capacity})")
        }

        val payloadLen = try {
            base.getInt(0) // absolute read, LE
        } catch (t: Throwable) {
            pool.release(block)
            throw CorruptedBlockException("failed to read payloadLen")
        }

        if (payloadLen < 0 || payloadLen > PAYLOAD_LIMIT) {
            pool.release(block)
            throw CorruptedBlockException("payloadLen=$payloadLen out of bounds [0,$PAYLOAD_LIMIT]")
        }

        /* -------- 2) CRC32C over [0 .. 4+payloadLen) -------- */
        val calcCrc = try {
            crc.reset()
            val crcRegion = base.duplicate().apply {
                position(0); limit(4 + payloadLen)
            }
            crc.update(crcRegion)
            crc.value.toInt()
        } catch (t: Throwable) {
            pool.release(block)
            throw CorruptedBlockException("failed to compute CRC")
        }

        val storedCrc = try {
            base.getInt(PAYLOAD_LIMIT + 4) // at block end, LE
        } catch (t: Throwable) {
            pool.release(block)
            throw CorruptedBlockException("failed to read stored CRC")
        }

        if (calcCrc != storedCrc) {
            pool.release(block)
            throw CorruptedBlockException("CRC mismatch (calc=$calcCrc stored=$storedCrc)")
        }

        /* -------- 3) slice payload area (read-only, LE) -------- */
        val payloadView = base.duplicate().apply {
            position(4); limit(4 + payloadLen)
        }.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN)

        /* -------- 4) split into length-prefixed records -------- */
        val out = ArrayList<ByteBufferL>(16)
        var pos = 0
        while (pos < payloadLen) {
            if (pos + 4 > payloadLen) {
                pool.release(block)
                throw CorruptedBlockException("truncated record header at pos=$pos")
            }
            val recLen = payloadView.getInt(pos) // LE
            if (recLen <= 0 || pos + 4 + recLen > payloadLen) {
                pool.release(block)
                throw CorruptedBlockException("record length out of range (recLen=$recLen pos=$pos)")
            }

            // Make a 0..recLen RO/LE view and wrap it as ByteBufferL
            val recBB = payloadView.duplicate().apply {
                position(pos + 4); limit(pos + 4 + recLen)
            }.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN)

            out += ByteBufferL.wrap(recBB).asReadOnly()
            pos += 4 + recLen
        }

        return out
    }
}
