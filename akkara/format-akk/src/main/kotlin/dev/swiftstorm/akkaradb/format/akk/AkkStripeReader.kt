package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.compareTo
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.StripeReader
import dev.swiftstorm.akkaradb.format.api.StripeReader.Stripe
import dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ

/**
 * Reader for a *stripe* in the append‑only segment.
 *
 * Ownership & Lifetime:
 *  * The returned [Stripe] holds both payload slices and the backing lane blocks.
 *  * Caller **must** call [Stripe.close] after use to release buffers back to the pool.
 */
class AkkStripeReader(
    baseDir: Path,
    private val k: Int,
    private val parityCoder: ParityCoder? = null,
) : StripeReader, AutoCloseable {

    /* ───────── file handles ───────── */
    private val dataCh = (0 until k).map { Files.newByteChannel(baseDir.resolve("data_$it.akd"), READ) }
    private val parityCh = (0 until (parityCoder?.parityCount ?: 0))
        .map { Files.newByteChannel(baseDir.resolve("parity_$it.akp"), READ) }

    /* ───────── helpers ───────── */
    private val unpacker = AkkBlockUnpacker()
    private val pool = Pools.io()

    /**
     * Reads one stripe and returns a [Stripe] with payload slices and lane blocks.
     * Returns `null` on EOF.
     */
    override fun readStripe(): Stripe? {
        /* 1. read data lanes (must fill 32KiB each) */
        val laneBlocks = MutableList<ByteBufferL?>(k) { null }
        for ((idx, ch) in dataCh.withIndex()) {
            val blk = pool.get(BLOCK_SIZE)
            if (readFullBlock(ch, blk)) {
                laneBlocks[idx] = blk
            } else {
                pool.release(blk) // EOF / partial read → discard
            }
        }
        if (laneBlocks.all { it == null }) return null // true EOF

        /* 2. optional parity recovery */
        val missingCnt = laneBlocks.count { it == null }
        val pCount = parityCoder?.parityCount ?: 0
        require(missingCnt <= pCount) {
            "Unrecoverable stripe – $missingCnt lanes missing but only $pCount parity lanes configured"
        }

        val parityBufs: List<ByteBufferL?> =
            if (missingCnt > 0 && parityCoder != null) {
                // read parity lanes fully (may yield null on partial/EOF)
                val tmp = parityCh.map { ch ->
                    val buf = pool.get(BLOCK_SIZE)
                    if (readFullBlock(ch, buf)) buf else run { pool.release(buf); null }
                }

                // recover each missing data lane using available parity
                for (missIdx in laneBlocks.indices) {
                    if (laneBlocks[missIdx] == null) {
                        laneBlocks[missIdx] = parityCoder.decode(missIdx, laneBlocks, tmp)
                    }
                }
                tmp
            } else {
                emptyList()
            }

        // parity bufs are no longer needed after recovery
        parityBufs.forEach { it?.let(pool::release) }

        /* 3. unpack payloads (all blocks must be present here) */
        val nonNullBlocks = laneBlocks.map { it ?: throw CorruptedBlockException("Corrupted stripe: unrecoverable lane") }
        val payloads = nonNullBlocks.flatMap(unpacker::unpack)

        return Stripe(payloads, nonNullBlocks, pool)
    }

    /** Read exactly BLOCK_SIZE bytes into [blk]. Returns true on success (and flips), false otherwise. */
    private fun readFullBlock(ch: java.nio.channels.ReadableByteChannel, blk: ByteBufferL): Boolean {
        val bb = blk.toMutableByteBuffer()
        bb.clear() // position=0, limit=capacity
        var read = 0
        while (read < BLOCK_SIZE) {
            val n = ch.read(bb)
            if (n < 0) break
            if (n == 0) continue
            read += n
        }
        return if (read == BLOCK_SIZE) {
            blk.flip(); true
        } else false
    }

    override fun close() {
        (dataCh + parityCh).forEach { it.close() }
    }

    data class StripeHit(
        val record: Record,
        val stripeId: Long,
        val blocks: List<Record>,
        val stripe: Stripe
    ) : AutoCloseable {
        override fun close() = stripe.close()
    }

    fun searchLatestStripe(key: ByteBufferL, untilStripe: Long): StripeHit? {
        var idx = 0L
        val rr = AkkRecordReader
        while (true) {
            val stripe = readStripe() ?: break
            val stripeId = untilStripe - idx; idx++
            val recs = ArrayList<Record>()
            var hit: Record? = null
            for (p in stripe.payloads) {
                val r = rr.read(p.duplicate())
                recs.add(r)
                if (hit == null && r.key.compareTo(key) == 0) hit = r
            }
            if (hit != null) return StripeHit(hit, stripeId, recs, stripe)
            stripe.close()
        }
        return null
    }

}
