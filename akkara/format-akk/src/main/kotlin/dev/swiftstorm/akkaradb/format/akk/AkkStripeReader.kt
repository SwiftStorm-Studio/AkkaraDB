package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.Pools           // ★ 追加
import dev.swiftstorm.akkaradb.common.Record
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
        /* 1. read data lanes */
        val laneBlocks = MutableList<ByteBuffer?>(k) { null }
        for ((idx, ch) in dataCh.withIndex()) {
            val blk = pool.get()
            val n   = ch.read(blk)
            if (n == BLOCK_SIZE) {
                blk.flip(); laneBlocks[idx] = blk
            } else {
                pool.release(blk) // EOF
            }
        }
        if (laneBlocks.all { it == null }) return null

        /* 2. parity recovery (optional) */
        if (laneBlocks.any { it == null } && parityCoder != null) {
            val parityBufs = parityCh.map { ch ->
                val buf = pool.get();
                val n = ch.read(buf)
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

        /* 3. CRC & unpack */
        val payloads = laneBlocks.map { blk ->
            blk ?: throw CorruptedBlockException("Unrecoverable lane in stripe")
        }.map(unpacker::unpack)   // read‑only slice (shares backing array)

        return payloads
    }

    fun searchLatestStripe(key: ByteBuffer): Record? {
        val rr = AkkRecordReader
        var stripePayloads: List<ByteBuffer>? = readStripe()
        while (stripePayloads != null) {
            var found: Record? = null
            for (payload in stripePayloads) {
                val dup = payload.duplicate()
                while (dup.hasRemaining()) {
                    val rec = rr.read(dup)
                    if (rec.key == key) {
                        found = rec; break
                    }
                }
                if (found != null) break
            }
            // 🔋 release buffers now that we no longer need them
            stripePayloads.forEach(pool::release)

            if (found != null) return found
            stripePayloads = readStripe()
        }
        return null
    }

    override fun close() {
        (dataCh + parityCh).forEach { it.close() }
    }
}
