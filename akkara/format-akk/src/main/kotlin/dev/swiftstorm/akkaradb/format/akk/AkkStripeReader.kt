package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.StripeReader
import dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ

/**
 * Reader for a *stripe* in the append‑only segment.
 *
 * <h3>Ownership &amp; Lifetime</h3>
 *  * The returned payload slices and subsequently decoded [Record]s **share the
 *    same backing memory** as the original 32&nbsp;KiB lane blocks. Therefore the
 *    caller is responsible for releasing those lane blocks <b>after</b> it has
 *    parsed everything it needs from the payloads. In typical usage this means:
 *      1. Call [readStripe] → iterate payloads → parse records
 *      2. Immediately `pool.release(block)` every element of the list returned
 *         by `laneBlocks` (not exposed here – track them yourself if needed)
 *
 * The reader itself only releases <em>parity</em> buffers internally after decode.
 */
class AkkStripeReader(
    baseDir: Path,
    private val k: Int,
    private val parityCoder: ParityCoder? = null,
) : StripeReader {

    /* ───────── file handles ───────── */
    private val dataCh = (0 until k).map { Files.newByteChannel(baseDir.resolve("data_$it.akd"), READ) }
    private val parityCh = (0 until (parityCoder?.parityCount ?: 0))
        .map { Files.newByteChannel(baseDir.resolve("parity_$it.akp"), READ) }

    /* ───────── helpers ───────── */
    private val unpacker = AkkBlockUnpacker()
    private val pool = Pools.io()

    /**
     * Reads <b>one</b> stripe and returns a list of record‑payload slices
     * (may be empty if no records in stripe). Returns `null` on EOF.
     */
    override fun readStripe(): List<ByteBuffer>? {
        /* 1. read data lanes */
        val laneBlocks = MutableList<ByteBuffer?>(k) { null }
        for ((idx, ch) in dataCh.withIndex()) {
            val blk = pool.get()
            val n = ch.read(blk)
            if (n == BLOCK_SIZE) {
                blk.flip(); laneBlocks[idx] = blk  // full 32 KiB block
            } else {
                pool.release(blk)                 // EOF or partial → discard
            }
        }
        if (laneBlocks.all { it == null }) return null   // true EOF

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

        // release parity buffers immediately – their data has been copied/consumed
        parityBufs.forEach { it?.let(pool::release) }

        /* 3. split each 32 KiB block into their record‑payload slices */
        val nonNullBlocks = laneBlocks.map { it ?: throw CorruptedBlockException("Corrupted stripe: unrecoverable lane") }
        val payloads = nonNullBlocks.flatMap(unpacker::unpack)

        return payloads
    }

    /**
     * Scans stripes <em>newest‑first</em> until the given [key] is found or we hit
     * EOF. Returns a [StripeHit] with decoded records for caching.
     */
    fun searchLatestStripe(key: ByteBuffer, untilStripe: Long): StripeHit? {
        var idx = 0L
        val rr = AkkRecordReader

        while (true) {
            val payloads = readStripe() ?: break
            val stripeId = untilStripe - idx  // newest‑first numbering
            idx++

            val recs = ArrayList<Record>()
            for (p in payloads) {
                val r = rr.read(p.duplicate())
                recs.add(r)
                if (r.key.compareTo(key) == 0) {
                    return StripeHit(r, stripeId, recs)
                }
            }
            // NOTE: caller did not find the key in this stripe → it will be GC‑collected;
            // lane blocks are still in pool for potential reuse.
        }
        return null
    }

    override fun close() {
        (dataCh + parityCh).forEach { it.close() }
    }

    data class StripeHit(val record: Record, val stripeId: Long, val blocks: List<Record>)
}
