package dev.swiftstorm.akkaradb.test

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.toByteArray
import dev.swiftstorm.akkaradb.engine.sstable.AKSSFooter
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

fun main() {
    val path = Path.of("C:/Users/main/IdeaProjects/AkkaraDB/akkara/test/test/perf-sst/sst/L0/L0_1110816966601700.sst")
    debugDumpSST(path)
}

fun debugDumpSST(path: Path) {
    Files.newByteChannel(path, StandardOpenOption.READ).use { ch ->
        val size = ch.size()

        // 1) Read footer
        ch.position(size - AKSSFooter.SIZE)
        val footerBuf = ByteBufferL.allocate(AKSSFooter.SIZE)
        footerBuf.readFully(ch, AKSSFooter.SIZE)
        footerBuf.position(0)
        val footer = AKSSFooter.readFrom(
            buf = footerBuf,
            verifyCrc = true,
            ch = ch,
            fileSize = size
        )

        println("footer = $footer")

        // 2) Iterate blocks one by one
        var off = 0L
        var blockIndex = 0

        while (off + BLOCK_SIZE <= footer.indexOff) {
            println("=== BLOCK $blockIndex at off=$off ===")

            val raw = ByteBuffer.allocateDirect(BLOCK_SIZE).order(ByteOrder.LITTLE_ENDIAN)
            ch.position(off)
            raw.clear()
            ch.read(raw)
            raw.flip()

            // CRC check
            val bufL = ByteBufferL.wrap(raw)
            val crcCalc = bufL.crc32cRange(0, BLOCK_SIZE - 4)
            val crcStored = bufL.at(BLOCK_SIZE - 4).i32

            println("CRC stored=$crcStored calc=$crcCalc")

            // payload
            val payload = bufL.at(0).i32
            println("payload = $payload")

            if (payload > BLOCK_SIZE - 4) {
                println("INVALID payload, STOP")
                break
            }

            var p = 4
            var recIndex = 0

            while (p < 4 + payload) {
                val h = bufL.readHeader32(p)
                val kLen = h.kLen
                val vLen = h.vLen.toIntExact()
                val total = 32 + kLen + vLen

                val k = bufL.sliceAt(p + 32, kLen)
                val v = bufL.sliceAt(p + 32 + kLen, vLen)

                println(
                    "rec[$recIndex]: kLen=$kLen vLen=$vLen key=" +
                            k.toByteArray().joinToString(",")
                )

                p += total
                recIndex++
            }

            off += BLOCK_SIZE
            blockIndex++
        }
    }
}
