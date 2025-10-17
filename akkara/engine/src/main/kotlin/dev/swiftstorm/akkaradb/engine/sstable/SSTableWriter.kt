@file:Suppress("NOTHING_TO_INLINE", "unused")

package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.MemRecord
import dev.swiftstorm.akkaradb.engine.IndexBlock
import dev.swiftstorm.akkaradb.engine.bloom.BloomFilter
import dev.swiftstorm.akkaradb.engine.bridge.tryAppendMem
import dev.swiftstorm.akkaradb.format.akk.AkkBlockPacker
import java.io.Closeable
import java.nio.channels.SeekableByteChannel

/**
 * SSTableWriter (v3)
 *
 * Layout (Little-Endian):
 *   [ data blocks (32 KiB each, packed by AkkBlockPacker) ]
 *   [ IndexBlock ('AKIX') ]
 *   [ BloomFilter ('AKBL') (optional if no entries) ]
 *   [ Footer ('AKSS', 32 bytes) ]
 *
 * Responsibilities:
 *  - Stream MemRecords in ascending key order into fixed 32 KiB blocks.
 *  - On each sealed block, write it to the channel and record (firstKey32, blockOff) into IndexBlock.Builder.
 *  - Add every key into BloomFilter.Builder.
 *  - Append, in order: IndexBlock → BloomFilter → Footer.
 */
class SSTableWriter(
    private val ch: SeekableByteChannel,
    expectedEntries: Long,
    private val bloomFpRate: Double = 0.01
) : Closeable {

    private val indexBuilder = IndexBlock.Builder(initialCapacity = 256)
    private val bloomBuilder = BloomFilter(
        expectedInsertions = expectedEntries,
        fpRate = bloomFpRate
    )

    private var totalEntries: Long = 0
    private var pendingFirstKey32: ByteArray? = null

    private val packer = AkkBlockPacker(onBlockReady = { fullBlock ->
        // File offset BEFORE writing this block.
        val blockOff = ch.position()

        // Write the 32 KiB block in one go.
        fullBlock.position(0)
        fullBlock.writeFully(ch, fullBlock.remaining)

        // Index entry for this block using the first key captured earlier.
        val fk = checkNotNull(pendingFirstKey32) {
            "missing firstKey for block at off=$blockOff"
        }
        indexBuilder.addKey32(fk, blockOff)
        pendingFirstKey32 = null
    })

    /**
     * Write all records. Keys must be provided in ascending order.
     */
    fun writeAll(records: Sequence<MemRecord>) {
        packer.beginBlock()

        records.forEach { rec ->
            // Try to append into the current block; if full, seal and retry.
            if (!packer.tryAppendMem(rec)) {
                packer.endBlock()
                packer.beginBlock()
                check(packer.tryAppendMem(rec)) {
                    "record larger than a block (keyLen=${rec.key.remaining}, valueLen=${rec.value.remaining})"
                }
            }

            // Capture the first key of the current block for IndexBlock.
            if (pendingFirstKey32 == null) {
                pendingFirstKey32 = IndexBlock.normalize32(rec.key)
            }

            // Bloom accumulation (key → fp64 path matches the reader side).
            bloomBuilder.addKey(rec.key)

            totalEntries++
        }

        // Seal any open block.
        packer.flush()
    }

    /**
     * Finalize the SST by appending Index → Bloom → Footer.
     * Returns offsets and total entry count.
     */
    fun seal(): SealResult {
        // (1) IndexBlock
        val indexOff = ch.position()
        run {
            val idx = indexBuilder.buildBuffer()
            idx.position(0)
            idx.writeFully(ch, idx.remaining)
        }

        // (2) BloomFilter (optional)
        val bloomOff = if (totalEntries > 0) {
            val off = ch.position()
            val bloom = bloomBuilder.build()
            bloom.writeTo(ch)
            off
        } else 0L

        // (3) Footer (fixed 32 bytes)
        val footerBuf = ByteBufferL.allocate(FOOTER_SIZE)
        Footer.writeTo(
            dst = footerBuf,
            indexOff = indexOff,
            bloomOff = bloomOff,
            entries = totalEntries.toInt(),
            crc32c = 0 // advisory (reader may ignore)
        )
        footerBuf.position(0)
        footerBuf.writeFully(ch, FOOTER_SIZE)

        return SealResult(indexOff, bloomOff, totalEntries)
    }

    override fun close() {
        // packer.close() does internal cleanup; channel ownership is left to the caller.
        packer.close()
    }

    data class SealResult(val indexOff: Long, val bloomOff: Long, val entries: Long)

    /* ---------------- Footer (AKSS, 32 bytes) ---------------- */

    companion object Footer {
        private const val FOOTER_MAGIC = 0x414B5353 // 'A''K''S''S'
        private const val FOOTER_VER = 1
        private const val FOOTER_SIZE = 32

        /**
         * Write AKSS footer at current position of [dst].
         * Layout (LE, 32 bytes total):
         *   0  .. 3  : magic 'AKSS' (u32)
         *   4        : ver (u8) = 1
         *   5  .. 7  : pad (u24) = 0
         *   8  .. 15 : indexOff (u64)
         *   16 .. 23 : bloomOff (u64) -- 0 if none
         *   24 .. 27 : entries (u32)  -- total records (tombstones included)
         *   28 .. 31 : crc32c (u32)   -- optional, currently advisory
         */
        fun writeTo(
            dst: ByteBufferL,
            indexOff: Long,
            bloomOff: Long,
            entries: Int,
            crc32c: Int = 0
        ): ByteBufferL {
            val start = dst.position
            dst.at(start + 0).i32 = FOOTER_MAGIC
            dst.at(start + 4).i8 = FOOTER_VER
            dst.at(start + 5).i8 = 0
            dst.at(start + 6).i8 = 0
            dst.at(start + 7).i8 = 0
            dst.at(start + 8).i64 = indexOff
            dst.at(start + 16).i64 = bloomOff
            dst.at(start + 24).i32 = entries
            dst.at(start + 28).i32 = crc32c
            dst.position(start + FOOTER_SIZE)
            return dst
        }
    }
}
