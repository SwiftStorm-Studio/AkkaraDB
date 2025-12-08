@file:Suppress("SameParameterValue", "unused")

package dev.swiftstorm.akkaradb.debugger.core.inspectors

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.hasRemaining
import dev.swiftstorm.akkaradb.debugger.core.*
import dev.swiftstorm.akkaradb.engine.bloom.BloomFilter
import dev.swiftstorm.akkaradb.engine.sstable.AKSSFooter
import dev.swiftstorm.akkaradb.engine.util.IndexBlock
import dev.swiftstorm.akkaradb.format.akk.AkkBlockUnpacker
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.zip.CRC32C
import kotlin.io.path.fileSize
import kotlin.io.path.pathString

/**
 * SSTable inspector for AkkaraDB v3 format
 */
class SSTInspector(private val path: Path) {

    fun inspect(includeRecordData: Boolean = false): SSTReport {
        val errors = mutableListOf<String>()

        FileChannel.open(path, StandardOpenOption.READ).use { ch ->
            val fileSize = path.fileSize()

            if (fileSize < AKSSFooter.SIZE) {
                return SSTReport(
                    filePath = path.pathString,
                    fileSize = fileSize,
                    footer = FooterInfo("INVALID", 0, 0, 0, 0, 0),
                    index = IndexInfo(0, 0, emptyList()),
                    bloom = null,
                    blocks = emptyList(),
                    errors = listOf("File too small: $fileSize < ${AKSSFooter.SIZE}")
                )
            }

            // Read footer
            val (footerInfo, fileCrc) = try {
                readFooter(ch, fileSize)
            } catch (e: Exception) {
                errors.add("Footer parse error: ${e.message}")
                return SSTReport(
                    filePath = path.pathString,
                    fileSize = fileSize,
                    footer = FooterInfo("INVALID", 0, 0, 0, 0, 0),
                    index = IndexInfo(0, 0, emptyList()),
                    bloom = null,
                    blocks = emptyList(),
                    errors = errors
                )
            }

            // Read index
            val indexInfo = try {
                readIndex(ch, footerInfo, fileSize)
            } catch (e: Exception) {
                errors.add("Index parse error: ${e.message}")
                IndexInfo(0, 0, emptyList())
            }

            // Read bloom
            val bloomInfo = try {
                if (footerInfo.bloomOffset > 0) {
                    readBloom(ch, footerInfo, fileSize)
                } else null
            } catch (e: Exception) {
                errors.add("Bloom parse error: ${e.message}")
                null
            }

            // Read blocks
            val blocks = mutableListOf<BlockInfo>()
            for (entry in indexInfo.entries) {
                try {
                    val blockInfo = readBlock(ch, entry.blockOffset, includeRecordData)
                    blocks.add(blockInfo)
                } catch (e: Exception) {
                    errors.add("Block at offset ${entry.blockOffset} error: ${e.message}")
                }
            }

            return SSTReport(
                filePath = path.pathString,
                fileSize = fileSize,
                footer = footerInfo.copy(crc32c = fileCrc),
                index = indexInfo,
                bloom = bloomInfo,
                blocks = blocks,
                errors = errors
            )
        }
    }

    private fun readFooter(ch: FileChannel, fileSize: Long): Pair<FooterInfo, Int> {
        val footerBuf = ByteBufferL.allocate(AKSSFooter.SIZE)
        ch.position(fileSize - AKSSFooter.SIZE)
        footerBuf.readFully(ch, AKSSFooter.SIZE)
        footerBuf.position(0).limit(AKSSFooter.SIZE)

        // Parse footer
        val footer = AKSSFooter.readFrom(footerBuf, verifyCrc = false, ch = ch, fileSize = fileSize)

        // Extract raw magic for display (i8 returns Int 0-255)
        footerBuf.position(0)
        val magicBytes = ByteArray(4)
        repeat(4) { magicBytes[it] = footerBuf.i8.toByte() }
        val magic = String(magicBytes, Charsets.UTF_8)

        footerBuf.position(4)
        val version = footerBuf.i8 // Int

        // Read stored CRC from footer position 28
        footerBuf.position(28)
        val storedCrc = footerBuf.i32

        val info = FooterInfo(
            magic = magic,
            version = version,
            indexOffset = footer.indexOff,
            bloomOffset = footer.bloomOff,
            entryCount = footer.entries,
            crc32c = storedCrc,
        )

        return info to storedCrc
    }

    private fun readIndex(ch: FileChannel, footer: FooterInfo, fileSize: Long): IndexInfo {
        val indexEndBound = if (footer.bloomOffset > 0) footer.bloomOffset else (fileSize - AKSSFooter.SIZE)
        val indexLen = indexEndBound - footer.indexOffset

        require(indexLen in 16..(1L shl 31)) { "index length suspicious: $indexLen" }

        val indexBuf = ByteBufferL.allocate(indexLen.toInt())
        ch.position(footer.indexOffset)
        indexBuf.readFully(ch, indexLen.toInt())
        indexBuf.position(0).limit(indexLen.toInt())

        val index = IndexBlock.readFrom(indexBuf)

        val entries = (0 until index.size()).map { i ->
            // Extract 32-byte key manually from entries slice
            val key32 = ByteArray(32)
            val base = i * 40 // 32 bytes key + 8 bytes offset
            repeat(32) { j ->
                key32[j] = indexBuf.at(12 + base + j).i8.toByte() // HEADER_SIZE=12 + entry offset
            }
            val blockOff = index.blockOffAt(i)

            IndexEntry(
                firstKey32 = key32.toHexString(),
                blockOffset = blockOff
            )
        }

        return IndexInfo(
            entryCount = index.size(),
            keySize = 32,
            entries = entries
        )
    }

    private fun readBloom(ch: FileChannel, footer: FooterInfo, fileSize: Long): BloomInfo? {
        val bloomLen = (fileSize - AKSSFooter.SIZE) - footer.bloomOffset
        if (bloomLen < 20) return null // HEADER_SIZE is internal, use hardcoded 20

        val bloomBuf = ByteBufferL.allocate(bloomLen.toInt())
        ch.position(footer.bloomOffset)
        bloomBuf.readFully(ch, bloomLen.toInt())
        bloomBuf.position(0).limit(bloomLen.toInt())

        val bloom = BloomFilter.readFrom(bloomBuf)

        // Extract k from header (offset 5)
        bloomBuf.position(5)
        val k = bloomBuf.i8

        // Extract mBits from header (offset 8)
        bloomBuf.position(8)
        val mBits = bloomBuf.i32

        return BloomInfo(
            byteSize = bloom.byteSize,
            hashCount = k,
            expectedInsertions = estimateExpectedInsertions(mBits, k)
        )
    }

    // Rough estimate of expected insertions from mBits and k
    private fun estimateExpectedInsertions(mBits: Int, k: Int): Long {
        if (k == 0) return 0L
        // Approximate: n ≈ (m * ln(2)) / k
        return ((mBits * 0.693) / k).toLong()
    }

    private fun readBlock(ch: FileChannel, offset: Long, includeRecordData: Boolean): BlockInfo {
        // Read raw block
        val raw = ByteBuffer.allocateDirect(BLOCK_SIZE).order(ByteOrder.LITTLE_ENDIAN)
        ch.position(offset)
        var totalRead = 0
        while (totalRead < BLOCK_SIZE) {
            val n = ch.read(raw)
            if (n < 0) break
            totalRead += n
        }
        raw.flip()

        // Verify CRC
        val dataCrc = computeCRC32C(raw.duplicate(), BLOCK_SIZE - 4)
        val crcStored = readIntLE(raw, BLOCK_SIZE - 4)
        val crcValid = dataCrc == crcStored

        val bufL = ByteBufferL.wrap(raw).position(0).limit(BLOCK_SIZE)

        // Read payload length
        val payloadLen = bufL.at(0).i32

        // Parse records using AkkBlockUnpacker
        val unpacker = AkkBlockUnpacker()
        val records = mutableListOf<RecordSummary>()

        try {
            val cursor = unpacker.cursor(bufL)
            while (cursor.hasNext()) {
                try {
                    val recView = cursor.tryNext() ?: break

                    val keyPreview = recView.key.toHexPreview(32)
                    val valuePreview = if ((recView.flags and 0x01) != 0) null else recView.value.toHexPreview(32)

                    // Full data extraction if requested
                    val keyData = if (includeRecordData) recView.key.readAllBytes() else null
                    val valueData = if (includeRecordData && (recView.flags and 0x01) == 0) {
                        recView.value.readAllBytes()
                    } else null

                    records.add(
                        RecordSummary(
                            seq = recView.seq.raw,
                            keyLen = recView.kLen,
                            valueLen = recView.vLen.raw.toInt(),
                            flags = recView.flags,
                            keyPreview = keyPreview,
                            valuePreview = valuePreview,
                            isTombstone = (recView.flags and 0x01) != 0,
                            keyData = keyData,
                            valueData = valueData
                        )
                    )
                } catch (e: Exception) {
                    break
                }
            }
        } catch (e: Exception) {
            // CRC or structural error already caught
        }

        return BlockInfo(
            offset = offset,
            payloadLen = payloadLen,
            recordCount = records.size,
            records = records,
            crcValid = crcValid,
            crcStored = crcStored,
            crcComputed = dataCrc
        )
    }

    private fun computeCRC32C(buf: ByteBuffer, length: Int): Int {
        val crc = CRC32C()
        val slice = buf.duplicate()
        slice.position(0).limit(length)
        crc.update(slice)
        return crc.value.toInt()
    }

    private fun readIntLE(bb: ByteBuffer, at: Int): Int {
        val oldPos = bb.position()
        bb.position(at)
        val v = (bb.get().toInt() and 0xFF) or
                ((bb.get().toInt() and 0xFF) shl 8) or
                ((bb.get().toInt() and 0xFF) shl 16) or
                ((bb.get().toInt() and 0xFF) shl 24)
        bb.position(oldPos)
        return v
    }
}

// Extension functions for hex conversion
private fun ByteArray.toHexString(): String =
    joinToString("") { "%02x".format(it) }

private fun ByteBufferL.toHexPreview(maxBytes: Int): String {
    val dup = duplicate()
    val len = minOf(maxBytes, dup.remaining)
    val bytes = ByteArray(len)
    repeat(len) {
        bytes[it] = dup.i8.toByte() // i8 returns Int, convert to Byte
    }
    return bytes.toHexString() + if (dup.hasRemaining()) "..." else ""
}

private fun ByteBufferL.readAllBytes(): ByteArray {
    val dup = duplicate()
    val bytes = ByteArray(dup.remaining)
    repeat(bytes.size) {
        bytes[it] = dup.i8.toByte()
    }
    return bytes
}