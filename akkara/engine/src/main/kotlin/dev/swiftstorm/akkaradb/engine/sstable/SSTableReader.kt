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

@file:Suppress("NOTHING_TO_INLINE")

package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.AKHdr32
import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.lexCompare
import dev.swiftstorm.akkaradb.engine.bloom.BloomFilter
import dev.swiftstorm.akkaradb.engine.sstable.bs.BlockSearcher
import dev.swiftstorm.akkaradb.engine.sstable.bs.impl.StandardBlockSearcher
import dev.swiftstorm.akkaradb.engine.util.IndexBlock
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.SeekableByteChannel

/**
 * SSTableReader (v3)
 *
 * Responsibilities:
 *  - Parse footer (AKSS), then load IndexBlock and BloomFilter (direct, LE-safe).
 *  - Point lookups: Index lower_bound → Bloom reject → read+verify block → in-block search (delegates to Unpacker).
 *  - Range scans: start block from Index lower_bound, then advance block-by-block.
 *  - CRC policy: verify blocks on first load; file-level CRC (footer) can be verified on open.
 */
class SSTableReader(
    private val ch: SeekableByteChannel,
    private val footer: AKSSFooter.Footer,
    private val index: IndexBlock,
    private val bloom: BloomFilter?,
    private val cache: BlockCache = BlockCache(512),
    private val searcher: BlockSearcher = StandardBlockSearcher
) : Closeable {

    /** Close underlying channel (caller may choose to not hand ownership; this closes it). */
    override fun close() {
        ch.close()
    }

    /** Point lookup. Returns a LE-safe value slice (zero-copy) or null. */
    fun get(key: ByteBufferL): ByteBufferL? {
        // Fast Bloom reject (if present)
        if (bloom != null && !bloom.mightContain(key)) return null

        // Locate candidate block via external index
        val blockOff = index.lookup(key)
        if (blockOff < 0) return null

        // Load block (with CRC verification on first load)
        val block = loadBlock(blockOff)

        // Delegate in-block search to pluggable strategy (uses your Unpacker internally)
        return searcher.find(block.buf, key)
    }

    /** Range iterator [startKey, endKey). endKey == null means "to EOF". */
    fun range(
        startKey: ByteBufferL,
        endKey: ByteBufferL? = null
    ): Sequence<Triple<ByteBufferL, ByteBufferL, AKHdr32.Header>> = sequence {
        var idx = index.lowerBound32(IndexBlock.normalize32(startKey))
        if (idx == index.size()) return@sequence

        while (idx < index.size()) {
            val off = index.blockOffAt(idx)
            val block = loadBlock(off)

            val (start, end) = payloadBounds(block.buf)
            var pos = start

            while (pos < end) {
                val (hdr, pair) = block.buf.readRecord32(pos)
                val (k, v) = pair

                // Range check
                if (lexCompare(k, startKey) >= 0) {
                    if (endKey != null && lexCompare(k, endKey) >= 0) {
                        return@sequence
                    }
                    yield(Triple(k, v, hdr))
                }

                // Next record
                pos += AKHdr32.SIZE + hdr.kLen + hdr.vLen.toIntExact()
            }
            idx++
        }
    }

    /* -------------------------- internals -------------------------- */

    private fun loadBlock(off: Long): Block {
        cache[off]?.let { return it }
        val raw = readAt(off, BLOCK_SIZE)

        // Verify CRC32C: layout is [0..BLOCK_SIZE-5]=data, [BLOCK_SIZE-4..]=crc32c
        val dataCrc = ByteBufferL.wrap(raw.duplicate().order(ByteOrder.LITTLE_ENDIAN))
            .crc32cRange(0, BLOCK_SIZE - 4)
        val crcStored = readIntLE(raw, BLOCK_SIZE - 4)
        require(dataCrc == crcStored) { "CRC mismatch at block off=$off (have=$dataCrc, want=$crcStored)" }

        val bufL = ByteBufferL.wrap(raw.order(ByteOrder.LITTLE_ENDIAN)).position(0).limit(BLOCK_SIZE)
        val block = Block(off, bufL)
        cache.put(off, block)
        return block
    }

    private fun payloadBounds(block: ByteBufferL): Pair<Int, Int> {
        val payloadLen = block.at(0).i32
        require(payloadLen in 0..PAYLOAD_LIMIT)
        val start = 4
        val end = start + payloadLen
        require(end <= BLOCK_SIZE - 4)
        return start to end
    }

    /** Random-access read into a direct ByteBuffer (order LE set for consistency). */
    private fun readAt(off: Long, len: Int): ByteBuffer {
        val dst = ByteBuffer.allocateDirect(len).order(ByteOrder.LITTLE_ENDIAN)
        var pos = off
        while (dst.hasRemaining()) {
            val n = ch.position(pos).read(dst)
            require(n >= 0) { "EOF while reading at off=$pos" }
            pos += n
        }
        dst.flip()
        return dst
    }

    private fun readIntLE(bb: ByteBuffer, at: Int): Int {
        val oldPos = bb.position()
        bb.position(at)
        val v =
            (bb.get().toInt() and 0xFF) or
                    ((bb.get().toInt() and 0xFF) shl 8) or
                    ((bb.get().toInt() and 0xFF) shl 16) or
                    ((bb.get().toInt() and 0xFF) shl 24)
        bb.position(oldPos)
        return v
    }

    data class Block(val off: Long, val buf: ByteBufferL)

    /* -------------------------- static helpers -------------------------- */

    companion object {
        /**
         * Open an SSTableReader from a SeekableByteChannel (file).
         * It reads the footer, then loads IndexBlock and BloomFilter slices into memory.
         */
        fun open(ch: SeekableByteChannel, verifyFooterCrc: Boolean = false): SSTableReader {
            val size = ch.size()
            require(size >= AKSSFooter.SIZE) { "file too small for footer" }

            // Read footer
            val footerBuf = ByteBufferL.allocate(AKSSFooter.SIZE)
            ch.position(size - AKSSFooter.SIZE)
            footerBuf.readFully(ch, AKSSFooter.SIZE)
            footerBuf.position(0).limit(AKSSFooter.SIZE)

            // Parse footer (optionally verify file-level CRC32C if storedCrc != 0)
            val footer = AKSSFooter.readFrom(
                buf = footerBuf,
                verifyCrc = verifyFooterCrc,
                ch = ch,
                fileSize = size
            )

            // Read IndexBlock
            val indexEndBound = if (footer.bloomOff > 0) footer.bloomOff else (size - AKSSFooter.SIZE)
            val indexLen = indexEndBound - footer.indexOff
            require(indexLen in 16..(1L shl 31)) { "index length suspicious: $indexLen" }
            val indexBuf = ByteBufferL.allocate(indexLen.toInt())
            ch.position(footer.indexOff)
            indexBuf.readFully(ch, indexLen.toInt())
            indexBuf.position(0).limit(indexLen.toInt())
            val index = IndexBlock.readFrom(indexBuf)

// Read Bloom (optional)
            val bloom = if (footer.bloomOff > 0) {
                val bloomLen = (size - AKSSFooter.SIZE) - footer.bloomOff
                if (bloomLen >= BloomFilter.HEADER_SIZE) {
                    val bloomBuf = ByteBufferL.allocate(bloomLen.toInt())
                    ch.position(footer.bloomOff)
                    bloomBuf.readFully(ch, bloomLen.toInt())
                    bloomBuf.position(0).limit(bloomLen.toInt())
                    BloomFilter.readFrom(bloomBuf)
                } else {
                    // too small to be valid → skip
                    null
                }
            } else null

            return SSTableReader(ch, footer, index, bloom)
        }
    }
}

/* =======================================================================
 * BlockCache: tiny LRU keyed by block file offset
 * ======================================================================= */
class BlockCache(private val maxBlocks: Int) {
    private val map = object : LinkedHashMap<Long, SSTableReader.Block>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Long, SSTableReader.Block>?): Boolean =
            size > maxBlocks
    }

    operator fun get(off: Long): SSTableReader.Block? = synchronized(map) { map[off] }
    fun put(off: Long, b: SSTableReader.Block) = synchronized(map) { map[off] = b }
}
