/*
 * AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

@file:Suppress("NOTHING_TO_INLINE", "unused")

package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.MemRecord
import dev.swiftstorm.akkaradb.engine.bloom.BloomFilter
import dev.swiftstorm.akkaradb.engine.bridge.tryAppendMem
import dev.swiftstorm.akkaradb.engine.util.IndexBlock
import dev.swiftstorm.akkaradb.format.akk.AkkBlockPacker
import java.io.Closeable
import java.nio.channels.SeekableByteChannel
import java.util.zip.CRC32C

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
        val footerBuf = ByteBufferL.allocate(AKSSFooter.SIZE)
        AKSSFooter.writeTo(
            dst = footerBuf,
            indexOff = indexOff,
            bloomOff = bloomOff,
            entries = totalEntries.toInt(),
            crc32c = 0
        )
        footerBuf.position(0)
        footerBuf.writeFully(ch, AKSSFooter.SIZE)

        val size = ch.size()
        val limit = size - 4
        require(limit >= 0) { "illegal file size while sealing: $size" }

        val oldPos = ch.position()
        try {
            val scratch = ByteBufferL.allocate(1 shl 20) // 1 MiB
            val crc = CRC32C()

            ch.position(0L)
            var total = 0L
            while (total < limit) {
                val toRead = kotlin.math.min(scratch.capacity.toLong(), limit - total).toInt()
                scratch.clear()
                scratch.limit(toRead)
                // fill scratch completely
                scratch.readFully(ch, toRead)
                // update CRC from backing ByteBuffer
                val bb = scratch.sliceAt(0, toRead)
                bb.position(0).limit(toRead)
                crc.update(bb.rawDuplicate())
                total += toRead
            }

            val have = crc.value.toInt()
            val tail = ByteBufferL.allocate(4)
            tail.at(0).i32 = have
            tail.position(0)
            ch.position(size - 4)
            tail.writeFully(ch, 4)
        } finally {
            ch.position(oldPos)
        }

        return SealResult(indexOff, bloomOff, totalEntries)
    }

    override fun close() {
        // packer.close() does internal cleanup; channel ownership is left to the caller.
        packer.close()
    }

    data class SealResult(val indexOff: Long, val bloomOff: Long, val entries: Long)
}
