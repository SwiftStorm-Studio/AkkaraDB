package dev.swiftstorm.akkaradb.format

import dev.swiftstorm.akkaradb.format.akk.BlockConst
import dev.swiftstorm.akkaradb.format.akk.BlockUnpacker
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*

class StripeReader(
    dir: Path,
    private val dataCount: Int,
    private val parityCoder: ParityCoder
) {
    private val dataCh = (0 until dataCount).map { idx ->
        FileChannel.open(dir.resolve("data_$idx.akd"), READ)
    }
    private val parityCh = (0 until parityCoder.parityCount).map { idx ->
        FileChannel.open(dir.resolve("parity_$idx.akp"), READ)
    }

    fun read(stripeOffset: Long): List<ByteArray> {
        val blockSize = BlockConst.BLOCK_SIZE.toLong()
        val data = MutableList<ByteArray?>(dataCount) { null }
        val parity = MutableList<ByteArray?>(parityCoder.parityCount) { null }

        data.indices.forEach { i ->
            dataCh[i].readFully(blockSize * stripeOffset, data, i)
        }
        parity.indices.forEach { i ->
            parityCh[i].readFully(blockSize * stripeOffset, parity, i)
        }

        var lost = -1
        data.forEachIndexed { idx, blk ->
            if (blk == null || !verify(blk)) lost = idx
        }
        parity.forEachIndexed { idx, blk ->
            if (blk == null || !verify(blk)) lost = dataCount + idx
        }

        if (lost >= 0) {
            val recovered = parityCoder.decode(lost, data, parity)
            if (lost < dataCount) data[lost] = recovered else parity[lost - dataCount] = recovered
        }

        return data.requireNoNulls()
    }

    private fun verify(block: ByteArray): Boolean = try {
        BlockUnpacker.unpack(block); true
    } catch (e: Exception) { false }

    private fun FileChannel.readFully(
        pos: Long, target: MutableList<ByteArray?>, idx: Int
    ) {
        val buf = ByteBuffer.allocate(BlockConst.BLOCK_SIZE)
        if (read(buf, pos) == BlockConst.BLOCK_SIZE) {
            target[idx] = buf.array()
        }
    }
}
