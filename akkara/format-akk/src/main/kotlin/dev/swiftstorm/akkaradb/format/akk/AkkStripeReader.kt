package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.Pools           // ★ 追加
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.StripeReader
import dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ

class AkkStripeReader(
    baseDir: Path,
    private val k: Int,
    private val parityCoder: ParityCoder? = null
) : StripeReader {

    private val dataCh = (0 until k).map { Files.newByteChannel(baseDir.resolve("data_$it.akd"), READ) }
    private val parityCh = (0 until (parityCoder?.parityCount ?: 0))
        .map { Files.newByteChannel(baseDir.resolve("parity_$it.akp"), READ) }

    private val unpacker = AkkBlockUnpacker()
    private val pool = Pools.io()

    override fun readStripe(): List<ByteBuffer>? {
        /* 1 ─ read k data lanes */
        val laneBlocks = ArrayList<ByteBuffer?>(k)
        dataCh.forEach { ch ->
            val blk = pool.get()
            val n   = ch.read(blk)
            val buf: ByteBuffer? = if (n == BLOCK_SIZE) {
                blk.flip(); blk                       // full block
            } else {                                  // EOF or short
                pool.release(blk); null
            }
            laneBlocks += buf
        }
        if (laneBlocks.all { it == null }) return null

        /* 2 ─ parity recovery */
        if (laneBlocks.any { it == null } && parityCoder != null) {
            val parityBufs = parityCh.map { ch ->
                val buf = pool.get()
                val n   = ch.read(buf)
                if (n == BLOCK_SIZE) {
                    buf.flip(); buf
                } else {
                    pool.release(buf); null
                }
            }
            val miss = laneBlocks.indexOfFirst { it == null }
            laneBlocks[miss] = parityCoder.decode(miss, laneBlocks, parityBufs)
            parityBufs.filterNotNull().forEach(pool::release)
        }

        /* 3 ─ CRC check & payload slice */
        val payloads = laneBlocks.map { blk ->
            blk ?: throw CorruptedBlockException("Unrecoverable lane in stripe")
        }.map(unpacker::unpack)                       // read-only slice

        /* 4 ─ release lane buffers back to pool */
        laneBlocks.filterNotNull().forEach(pool::release)

        return payloads
    }

    override fun close() {
        (dataCh + parityCh).forEach { it.close() }
    }
}
