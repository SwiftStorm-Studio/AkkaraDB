package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.*
import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.engine.IndexBlock
import dev.swiftstorm.akkaradb.engine.util.BloomFilter
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import dev.swiftstorm.akkaradb.format.akk.AkkRecordWriter
import java.io.Closeable
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.zip.CRC32C

/**
 * SSTable writer (ALL Little-Endian to match the reader):
 *  - Block payload = [MiniIndex][Concatenated Records]
 *  - MiniIndex     = [count:u16 LE][offset:u32 LE]×count
 *  - Block on disk = [len:u32 LE][ payload ][crc:u32 LE]   // CRC32C over payload only
 *  - Outer index   = repeat { [key:FIXED(32B)][blockOff:i64 LE] }
 *  - Footer        = ["AKSS":u32 LE=0x53534B41][indexOff:u64 LE][bloomOff:u64 LE]
 */
class SSTableWriter(
    private val path: Path,
    private val pool: BufferPool = Pools.io()
) : Closeable {

    private val ch = FileChannel.open(path, CREATE, WRITE, TRUNCATE_EXISTING, DSYNC)

    // 作業用ブロックバッファ（LE固定の ByteBufferL）
    private val blockBuf: ByteBufferL = pool.get(BLOCK_SIZE)

    private val crc32 = CRC32C()
    private val index = IndexBlock()

    /* ───────── public ───────── */

    fun write(records: Collection<Record>) {
        require(records.isNotEmpty()) { "records must not be empty" }

        // Bloom filter: build alongside writing
        val bloomBuilder = BloomFilter.Builder(records.size)

        var firstKeyInBlock: ByteBufferL? = null
        for (rec in records) {
            val encoded = encode(rec) // AkkRecordWriter: payload for a single record (LE)

            // ブロックに入らないならフラッシュ
            if (blockBuf.remaining < encoded.remaining) {
                blockBuf.flip()
                flushBlock(firstKeyInBlock!!)
                blockBuf.clear()
                firstKeyInBlock = null
            }
            if (firstKeyInBlock == null) firstKeyInBlock = rec.key

            // レコード連結
            blockBuf.put(encoded.asReadOnlyByteBuffer())
            bloomBuilder.add(rec.key)
            pool.release(encoded)
        }

        val bloom = bloomBuilder.build()

        // 末尾ブロック
        if (blockBuf.position > 0) {
            blockBuf.flip()
            flushBlock(firstKeyInBlock!!)
        }

        /* ---- append index + bloom + footer (ALL LE) ---- */
        val indexOff = ch.position()
        index.writeTo(ch) // IndexBlock 側で offset は i64 LE で書く想定

        val bloomOff = ch.position()
        // bloom bits はエンディアン非依存のビット列、直後に hashCount:u32 LE を書く
        bloom.writeTo(ch)
        pool.borrow(4) { b ->
            b.clear()
            b.putInt(bloom.hashCount) // LE
            b.flip()
            ch.write(b.toMutableByteBuffer())
        }

        writeFooter(indexOff, bloomOff)
        ch.force(true)
    }

    /* ───────── internal ───────── */

    private fun flushBlock(firstKey: ByteBufferL) {
        // MiniIndex を作り、[MiniIndex][Records] を payload にまとめる
        val miniIdx = buildMiniIndex(blockBuf.duplicate())
        val payloadSize = miniIdx.remaining + blockBuf.remaining
        val payload = pool.get(payloadSize)
        payload.put(miniIdx.asReadOnlyByteBuffer())
        payload.put(blockBuf.asReadOnlyByteBuffer())
        payload.flip()

        val offset = ch.position()

        // Write [len:u32 LE][payload][crc:u32 LE]
        pool.borrow(8) { hdrCrc ->
            hdrCrc.clear()
            hdrCrc.putInt(payload.remaining) // len (LE)

            crc32.reset()
            crc32.update(payload.asReadOnlyByteBuffer().slice()) // CRC over payload only
            hdrCrc.putInt(crc32.value.toInt()) // crc (LE)
            hdrCrc.flip()

            // 2つの write：先に len、次に payload、本体が終わったら crc
            //（gather-write の arrayWrite も可能だが、LE固定のため明示的に書く）
            val lenBB = hdrCrc.asReadOnlyByteBuffer().slice().apply { limit(4) }
            val crcBB = hdrCrc.asReadOnlyByteBuffer().slice().apply { position(4); limit(8) }

            ch.write(lenBB)
            ch.write(payload.toMutableByteBuffer())
            ch.write(crcBB)
        }

        // 外部インデックスに (先頭キー, ブロック先頭オフセット) を追加
        // IndexBlock の API が ByteBuffer を受けるなら ByteBuffer へ変換
        index.add(firstKey.asReadOnlyByteBuffer(), offset)

        pool.release(payload)
        blockBuf.clear()
    }

    /**
     * Build MiniIndex = [count:u16 LE][offset:u32 LE]×count for concatenated records in `data`.
     * Offsets are relative to the start of the records region (i.e., to be added to `dataStart = 2 + 4*count` by the reader).
     */
    private fun buildMiniIndex(data: ByteBufferL): ByteBufferL {
        val offsets = ArrayList<Int>(128)
        val tmp = data.duplicate()

        while (tmp.hasRemaining()) {
            val start = tmp.position
            offsets += start
            AkkRecordReader.read(tmp)
            if (tmp.position <= start) error("mini-index stalled at $start")
        }

        val count = offsets.size
        val out = pool.get(2 + 4 * count)
        out.putShort(count.toShort())          // u16 (LE)
        for (off in offsets) out.putInt(off)   // u32 (LE)
        out.flip()
        return out
    }

    private fun writeFooter(indexOff: Long, bloomOff: Long) {
        // Footer = ["AKSS":u32 LE=0x53534B41][indexOff:u64 LE][bloomOff:u64 LE]
        pool.borrow(20) { f ->
            f.clear()
            f.putInt(0x53534B41)   // "AKSS" as LE int
            f.putLong(indexOff)
            f.putLong(bloomOff)
            f.flip()
            ch.write(f.toMutableByteBuffer())
        }
    }

    private fun encode(rec: Record): ByteBufferL =
        pool.borrow(AkkRecordWriter.computeMaxSize(rec)) { buf ->
            AkkRecordWriter.write(rec, buf)
            buf.flip()
            buf.slice()
        }

    /* ───────── lifecycle ───────── */

    override fun close() {
        pool.release(blockBuf)
        ch.close()
    }
}
