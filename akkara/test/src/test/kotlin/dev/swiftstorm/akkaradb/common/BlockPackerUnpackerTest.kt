package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.types.U32
import dev.swiftstorm.akkaradb.common.types.U64
import dev.swiftstorm.akkaradb.format.akk.AkkBlockPacker
import dev.swiftstorm.akkaradb.format.akk.AkkBlockUnpacker
import dev.swiftstorm.akkaradb.format.api.BlockPacker
import dev.swiftstorm.akkaradb.format.api.RecordView
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.*

class BlockPackerUnpackerTest {

    // --- helpers ------------------------------------------------------------
    private fun bbOf(bytes: ByteArray, direct: Boolean = false): ByteBufferL {
        val b = ByteBufferL.allocate(bytes.size, direct)
        b.putBytes(bytes)
        b.limit = b.position
        b.position = 0
        return b
    }

    private fun bbOfUtf8(s: String): ByteBufferL = bbOf(s.toByteArray(Charsets.UTF_8))

    private fun bytes(n: Int, seed: Long = 1L): ByteArray {
        val rnd = Random(seed)
        return ByteArray(n) { rnd.nextInt(256).toByte() }
    }

    private fun viewBytes(b: ByteBufferL): ByteArray {
        val dup = b.duplicate()
        val out = ByteArray(dup.remaining)
        var i = 0
        while (dup.remaining > 0) {
            out[i++] = dup.i8.toByte()
        }
        return out
    }

    // --- tests --------------------------------------------------------------

    @Test
    fun singleRecord_roundTrip_ok() {
        val blocks = mutableListOf<ByteBufferL>()
        val packer = AkkaraBlockPackerForTest(onBlockReady = { blocks += it })

        // 1) pack one record
        val key = bbOfUtf8("user:42")
        val value = bbOfUtf8("Alice")
        val seq = U64.fromSigned(1L)
        val flags = 0
        val fp = AKHdr32.sipHash24(key, AKHdr32.DEFAULT_SIPHASH_SEED)
        val mini = AKHdr32.buildMiniKeyLE(key)

        packer.beginBlock()
        assertTrue(packer.tryAppend(key, value, seq, flags, fp, mini))
        packer.endBlock()

        // emitted exactly one block
        assertEquals(1, blocks.size)
        val block = blocks.single()
        assertEquals(BLOCK_SIZE, block.limit)

        // CRC32C matches header
        val calc = block.crc32cRange(0, BLOCK_SIZE - 4)
        val stored = block.at(BLOCK_SIZE - 4).i32
        assertEquals(calc, stored)

        // 2) unpack and verify contents
        val unpacker = AkkBlockUnpacker()
        val cur = unpacker.cursor(block)
        assertTrue(cur.hasNext())
        val rec = cur.next()

        assertEquals(seq, rec.seq)
        assertEquals(flags, rec.flags)
        assertEquals(key.remaining, rec.kLen)
        assertEquals(U32.of(value.remaining.toLong()), rec.vLen)

        assertArrayEquals(viewBytes(key), viewBytes(rec.key))
        assertArrayEquals(viewBytes(value), viewBytes(rec.value))

        assertFalse(cur.hasNext())
    }

    @Test
    fun appends_untilPayloadLimit_thenFalseOnOverflow() {
        val blocks = mutableListOf<ByteBufferL>()
        val packer = AkkaraBlockPackerForTest(onBlockReady = { blocks += it })
        packer.beginBlock()

        // medium-size records to fill near the limit
        val baseKey = bbOf(bytes(24, seed = 7))
        val baseVal = bbOf(bytes(128, seed = 9))
        val seq0 = U64.fromSigned(100L)
        val flags = 0

        var appended = 0
        while (true) {
            val key = baseKey.duplicate()
            val value = baseVal.duplicate()
            val fp = AKHdr32.sipHash24(key, AKHdr32.DEFAULT_SIPHASH_SEED)
            val mini = AKHdr32.buildMiniKeyLE(key)
            val ok = packer.tryAppend(key, value, seq0, flags, fp, mini)
            if (!ok) break
            appended++
        }
        // At least one record should have been appended
        assertTrue(appended > 0)

        // seal and emit
        packer.endBlock()
        assertEquals(1, blocks.size)

        // verify payloadLen boundary and record count via unpacker
        val block = blocks.first()
        val payloadLen = block.at(0).i32
        assertTrue(payloadLen in 0..PAYLOAD_LIMIT)
        val end = 4 + payloadLen
        assertTrue(end <= BLOCK_SIZE - 4)

        val unpacker = AkkBlockUnpacker()
        val out = mutableListOf<RecordView>()
        unpacker.unpackInto(block, out)
        assertEquals(appended, out.size)
    }

    @Test
    fun zeroPadding_isAllZeros() {
        val blocks = mutableListOf<ByteBufferL>()
        val packer = AkkaraBlockPackerForTest(onBlockReady = { blocks += it })

        packer.beginBlock()
        val key = bbOfUtf8("k")
        val value = bbOf(bytes(123))
        val seq = U64.fromSigned(2L)
        val fp = AKHdr32.sipHash24(key, AKHdr32.DEFAULT_SIPHASH_SEED)
        val mini = AKHdr32.buildMiniKeyLE(key)
        assertTrue(packer.tryAppend(key, value, seq, 0, fp, mini))
        packer.endBlock()

        val block = blocks.single()
        val payloadLen = block.at(0).i32
        val padStart = 4 + payloadLen
        val padEnd = BLOCK_SIZE - 4
        val dup = block.duplicate()
        dup.position = padStart
        dup.limit = padEnd
        while (dup.remaining > 0) {
            assertEquals(0, dup.i8)
        }
    }

    @Test
    fun multipleBlocks_emitted_and_roundTripAll() {
        val blocks = mutableListOf<ByteBufferL>()
        val packer = AkkaraBlockPackerForTest(onBlockReady = { blocks += it })

        var seq = U64.fromSigned(10L)
        var total = 0
        var rollovers = 0

        packer.beginBlock()
        repeat(2000) { i -> // 充分に多め
            val k = bbOfUtf8("k:$i")
            val v = bbOf(bytes(256 + (i % 512), seed = i.toLong()))
            val fp = AKHdr32.sipHash24(k, AKHdr32.DEFAULT_SIPHASH_SEED)
            val mini = AKHdr32.buildMiniKeyLE(k)

            if (!packer.tryAppend(k, v, seq, i and 0xFF, fp, mini)) {
                // ここに来た = PAYLOAD_LIMIT 超過なので必ずブロックが出る
                packer.endBlock(); rollovers++
                packer.beginBlock()
                check(packer.tryAppend(k, v, seq, i and 0xFF, fp, mini))
            }
            seq = U64.fromSigned(seq.raw + 1); total++
        }
        packer.endBlock()

        assertTrue(rollovers >= 1, "at least one rollover expected")
        assertTrue(blocks.size >= 2, "Expected >=2 blocks, got ${blocks.size}")

        // round-trip
        val unpacker = AkkBlockUnpacker()
        var seen = 0
        for (b in blocks) {
            val out = mutableListOf<RecordView>()
            unpacker.unpackInto(b, out)
            seen += out.size
        }
        assertEquals(total, seen)
    }

    @Test
    fun malformed_payloadLen_throws() {
        val blocks = mutableListOf<ByteBufferL>()
        val packer = AkkaraBlockPackerForTest(onBlockReady = { blocks += it })
        packer.beginBlock()
        val k = bbOfUtf8("a")
        val v = bbOfUtf8("b")
        val seq = U64.fromSigned(1L)
        val fp = AKHdr32.sipHash24(k, AKHdr32.DEFAULT_SIPHASH_SEED)
        val mini = AKHdr32.buildMiniKeyLE(k)
        assertTrue(packer.tryAppend(k, v, seq, 0, fp, mini))
        packer.endBlock()

        val block = blocks.single().duplicate()
        // Corrupt payloadLen to exceed limit
        block.at(0).i32 = PAYLOAD_LIMIT + 1

        val unpacker = AkkBlockUnpacker()
        assertThrows<IllegalArgumentException> {
            unpacker.cursor(block)
        }
    }
}

/**
 * Small wrapper to make the concrete class name clear in tests.
 * (Rename if your implementation class name differs.)
 */
private class AkkaraBlockPackerForTest(
    private val onBlockReady: (ByteBufferL) -> Unit
) : BlockPacker {
    private val delegate = AkkBlockPacker(onBlockReady)
    override fun beginBlock() = delegate.beginBlock()
    override fun tryAppend(key: ByteBufferL, value: ByteBufferL, seq: U64, flags: Int, keyFP64: U64, miniKey: U64): Boolean =
        delegate.tryAppend(key, value, seq, flags, keyFP64, miniKey)

    override fun endBlock() = delegate.endBlock()
    override fun flush() = delegate.flush()
    override fun close() = delegate.close()
}
