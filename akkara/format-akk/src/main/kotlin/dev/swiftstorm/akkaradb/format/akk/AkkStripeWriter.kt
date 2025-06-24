package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.logger
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.StripeWriter
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*

/**
 * Append-only stripe writer for the “akk” on-disk format.
 *
 * * k data lanes (`data_i.akd`)
 * * m = parityCoder?.parityCount ?: 0  parity lanes (`parity_i.akp`)
 *
 * Durability
 * ----------
 * * Lanes are opened with `DSYNC`; `flush()` issues `force(true)`.
 * * `addBlock()` buffers until k blocks are queued, then writes
 *   the entire stripe in a gather-write loop.
 */
class AkkStripeWriter(
    val baseDir: Path,
    override val k: Int = 4,
    val parityCoder: ParityCoder? = null,
    val autoFlush: Boolean = false
) : StripeWriter {

    override val m: Int = parityCoder?.parityCount ?: 0

    init {
        require(k > 0) { "k must be ≥ 1" }
        require(m >= 0) { "parityCount must be ≥ 0" }
    }

    /* ---------- lane channels ---------- */

    private val dataLanes: List<FileChannel> =
        (0 until k).map { lane ->
            FileChannel.open(
                baseDir.resolve("data_$lane.akd"),
                CREATE, WRITE, APPEND, DSYNC
            )
        }

    private val parityLanes: List<FileChannel> =
        (0 until m).map { lane ->
            FileChannel.open(
                baseDir.resolve("parity_$lane.akp"),
                CREATE, WRITE, APPEND, DSYNC
            )
        }

    /* ---------- state ---------- */

    private val queue = ArrayList<ByteBuffer>(k)
    override var stripesWritten: Long = 0
        private set

    /* ---------- public API ---------- */

    override fun addBlock(block: ByteBuffer) {
        if (queue.size >= k) {
            when (autoFlush) {
                true -> run {
                    flush()
                    addBlock(block)  // retry with the new empty queue
                    logger.debug("Stripe flushed, block added to new queue")
                }

                false -> error("Stripe already full; call flush()")
            }
        }
        queue += block.asReadOnlyBuffer()          // keep caller untouched
        if (queue.size == k) writeStripe()
    }

    override fun flush(): Long {
        if (queue.isNotEmpty()) writeStripe()
        (dataLanes + parityLanes).forEach { it.force(true) }
        return stripesWritten
    }

    override fun seek(stripeIndex: Long) {
        require(queue.isEmpty()) { "cannot seek while a stripe is buffered" }
        require(stripeIndex >= 0) { "negative stripe index" }

        val blockSize = BLOCK_SIZE.toLong()
        val expectedBytes = stripeIndex * blockSize
        require(dataLanes.all { it.size() == expectedBytes }) {
            "lane length mismatch: manifest=$stripeIndex, file=${
                dataLanes.first().size() / blockSize
            }"
        }

        stripesWritten = stripeIndex
    }

    override fun close() {
        flush()
        (dataLanes + parityLanes).forEach(FileChannel::close)
    }

    /* ---------- internal ---------- */

    private fun writeStripe() {
        val dataDup = queue.map { it.duplicate() }
        val parityBlocks = parityCoder?.encode(dataDup) ?: emptyList()

        for ((idx, ch) in dataLanes.withIndex()) ch.write(queue[idx].duplicate())
        for ((idx, ch) in parityLanes.withIndex()) ch.write(parityBlocks[idx].duplicate())

        queue.clear()
        stripesWritten++
    }

}
