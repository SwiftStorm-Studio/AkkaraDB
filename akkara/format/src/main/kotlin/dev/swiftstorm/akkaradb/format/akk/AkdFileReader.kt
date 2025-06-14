package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.format.ParityCoder
import dev.swiftstorm.akkaradb.format.akk.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ

class AkdFileReader(
    baseDir: Path,
    private val k: Int,
    private val parityCoder: ParityCoder? = null
) : Closeable {

    private val dataCh = (0 until k).map { Files.newByteChannel(baseDir.resolve("data_$it.akd"), READ) }
    private val parityCh = (0 until (parityCoder?.parityCount ?: 0))
        .map { Files.newByteChannel(baseDir.resolve("parity_$it.akp"), READ) }

    private val unpacker = AkkBlockUnpacker()

    /**
     * @return payloads for one stripe, or `null` when *all* lanes hit EOF.
     * @throws CorruptedBlockException if a lane is unreadable and
     *         parity recovery is unavailable or fails.
     */
    fun readStripe(): List<ByteBuffer>? {
        /* 1 — read k data lanes */
        val blocks = ArrayList<ByteBuffer?>(k)
        dataCh.forEach { ch ->
            val blk = ByteBuffer.allocateDirect(BLOCK_SIZE)
            val n   = ch.read(blk)

            val result: ByteBuffer? = when (n) {
                BLOCK_SIZE -> { blk.flip(); blk.asReadOnlyBuffer() }  // full block OK
                -1         -> null                                    // EOF
                else        -> null                                    // short read ⇒ recover via parity
            }
            blocks += result
        }

        if (blocks.all { it == null }) return null   // 完全 EOF

        /* 2 — parity recovery (optional) */
        if (blocks.any { it == null } && parityCoder != null) {
            val parityBufs: List<ByteBuffer?> = parityCh.map { ch ->
                val buf = ByteBuffer.allocateDirect(BLOCK_SIZE)
                val n   = ch.read(buf)
                if (n == BLOCK_SIZE) { buf.flip(); buf.asReadOnlyBuffer() } else null
            }

            val missingIdx = blocks.indexOfFirst { it == null }
            val recovered  = parityCoder.decode(missingIdx, blocks, parityBufs)
            blocks[missingIdx] = recovered
        }

        /* 3 — CRC check + slice payload */
        return blocks.map { blk ->
            blk ?: throw CorruptedBlockException("Unrecoverable lane $k in stripe")
        }.map(unpacker::unpack)   // returns payload slice
    }

    override fun close() {
        (dataCh + parityCh).forEach { it.close() }
    }
}
