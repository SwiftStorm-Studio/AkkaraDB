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
 * 32KiB固定ブロック（AkkBlockPackerDirect生成物）を
 * 長さプレフィックス付きの payload 切り身に展開する。
 *
 * On-disk (LE):
 *  [0..3]     payloadLen (u32 LE)
 *  [4..4+N)   payload bytes ([u32 len][bytes]…)
 *  [..]       zero padding up to PAYLOAD_LIMIT
 *  [end-4..]  crc32c(u32 LE) over [0 .. 4+payloadLen)
 *
 * 所有権: 正常時は **呼び出し側が block を保持**（Stripeがclose時にpoolへ返却）。
 * エラー時のみ当クラスが block を pool へ返却する。
 */
class AkkBlockUnpacker(
    private val pool: BufferPool = Pools.io()
) : BlockUnpacker {

    private val crc = CRC32C()

    override fun unpackInto(block: ByteBufferL, out: MutableList<ByteBufferL>): Int {
        val base = block.asReadOnlyByteBuffer()

        if (block.capacity != BLOCK_SIZE) {
            pool.release(block)
            throw CorruptedBlockException("block capacity != $BLOCK_SIZE (was ${block.capacity})")
        }

        val payloadLen = try {
            base.getInt(0) // absolute, LE
        } catch (t: Throwable) {
            pool.release(block)
            throw CorruptedBlockException("failed to read payloadLen")
        }

        if (payloadLen == 0) {
            return 0
        }
        if (payloadLen < 0 || payloadLen > PAYLOAD_LIMIT) {
            pool.release(block)
            throw CorruptedBlockException("payloadLen=$payloadLen out of bounds [1,$PAYLOAD_LIMIT]")
        }

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
            base.getInt(PAYLOAD_LIMIT + 4) // end-4（block末尾）に格納
        } catch (t: Throwable) {
            pool.release(block)
            throw CorruptedBlockException("failed to read stored CRC")
        }

        if (calcCrc != storedCrc) {
            pool.release(block)
            throw CorruptedBlockException("CRC mismatch (calc=$calcCrc stored=$storedCrc)")
        }

        val payloadView = base.duplicate().apply {
            position(4); limit(4 + payloadLen)
        }.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN)

        val startSize = out.size
        var pos = 0
        while (pos < payloadLen) {
            if (pos + 4 > payloadLen) {
                pool.release(block)
                throw CorruptedBlockException("truncated record header at pos=$pos")
            }
            val recLen = payloadView.getInt(pos) // LE
            val end = pos + 4 + recLen
            if (recLen <= 0 || end > payloadLen) {
                pool.release(block)
                throw CorruptedBlockException("record length out of range (recLen=$recLen pos=$pos)")
            }

            val recBB = payloadView.duplicate().apply {
                position(pos + 4); limit(end)
            }.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN)

            out.add(ByteBufferL.wrap(recBB).asReadOnly())
            pos = end
        }

        return out.size - startSize
    }

    private companion object {
        private val TL_OUT = ThreadLocal.withInitial { ArrayList<ByteBufferL>(32) }
    }
}
