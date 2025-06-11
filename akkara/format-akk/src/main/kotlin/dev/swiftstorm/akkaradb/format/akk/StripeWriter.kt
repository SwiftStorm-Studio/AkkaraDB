package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ArrayBlockingQueue

class StripeWriter(
    private val dir: Path,
    private val dataCount: Int,
    private val parityCoder: ParityCoder
) : AutoCloseable {

    private val ring = ArrayBlockingQueue<ByteArray>(dataCount)
    private val dataChannels: List<FileChannel>
    private val parityChannels: List<FileChannel>

    init {
        Files.createDirectories(dir)
        dataChannels = (0 until dataCount).map { idx ->
            FileChannel.open(dir.resolve("data_$idx.akd"),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE)
        }
        parityChannels = (0 until parityCoder.parityCount).map { idx ->
            FileChannel.open(dir.resolve("parity_$idx.akp"),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE)
        }
    }

    fun append(block: ByteArray) {
        require(block.size == BlockConst.BLOCK_SIZE)
        ring.put(block)
        if (ring.size == dataCount) flushStripe()
    }

    private fun flushStripe() {
        val dataBlocks = mutableListOf<ByteArray>()
        repeat(dataCount) { dataBlocks += ring.take() }

        dataBlocks.forEachIndexed { idx, blk ->
            dataChannels[idx].write(ByteBuffer.wrap(blk))
        }

        val parity = parityCoder.encode(dataBlocks)
        parity.forEachIndexed { idx, blk ->
            parityChannels[idx].write(ByteBuffer.wrap(blk))
        }

        (dataChannels + parityChannels).forEach { it.force(true) }
    }

    override fun close() {
        // 残ブロック pad
        while (ring.isNotEmpty()) append(ByteArray(BlockConst.BLOCK_SIZE))
        (dataChannels + parityChannels).forEach(FileChannel::close)
    }
}