package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.StripeReader
import dev.swiftstorm.akkaradb.format.api.StripeReader.Stripe
import dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException
import java.nio.ByteBuffer
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
        /* 1. read data lanes */
        val laneBlocks = MutableList<ByteBuffer?>(k) { null }
        for ((idx, ch) in dataCh.withIndex()) {
            val blk = pool.get()
            val n = ch.read(blk)
            if (n == BLOCK_SIZE) {
                blk.flip(); laneBlocks[idx] = blk
            } else {
                pool.release(blk) // EOF or partial → discard
            }
        }
        if (laneBlocks.all { it == null }) return null // true EOF

        /* 2. optional parity recovery */
        val missingCnt = laneBlocks.count { it == null }
        require(missingCnt <= (parityCoder?.parityCount ?: 0)) {
            "Unrecoverable stripe – $missingCnt lanes missing but only " +
                    "${parityCoder?.parityCount ?: 0} parity lanes configured"
        }

        val parityBufs: List<ByteBuffer?> = if (missingCnt > 0 && parityCoder != null) {
            val tmp = parityCh.map { ch ->
                val buf = pool.get()
                val n = ch.read(buf)
                if (n == BLOCK_SIZE) {
                    buf.flip(); buf
                } else {
                    pool.release(buf); null
                }
            }

            for (missIdx in laneBlocks.indices) {
                if (laneBlocks[missIdx] == null) {
                    laneBlocks[missIdx] = parityCoder.decode(missIdx, laneBlocks, tmp)
                }
            }

            tmp
        } else {
            emptyList()
        }

        // release parity buffers immediately
        parityBufs.forEach { it?.let(pool::release) }

        /* 3. unpack payloads */
        val nonNullBlocks = laneBlocks.map { it ?: throw CorruptedBlockException("Corrupted stripe: unrecoverable lane") }
        val payloads = nonNullBlocks.flatMap(unpacker::unpack)

        return StripeReader.Stripe(payloads, nonNullBlocks, pool)
    }

    override fun close() {
        (dataCh + parityCh).forEach { it.close() }
    }

    // 型を拡張
    data class StripeHit(
        val record: Record,
        val stripeId: Long,
        val blocks: List<Record>,
        val stripe: Stripe
    ) : AutoCloseable {
        override fun close() = stripe.close()
    }

    fun searchLatestStripe(key: ByteBuffer, untilStripe: Long): StripeHit? {
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
