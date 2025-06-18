package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.MAX_RECORD
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.BlockPacker
import java.io.Closeable
import java.nio.ByteBuffer
import java.util.zip.CRC32

/**
 * Block packer backed by a global [dev.swiftstorm.akkaradb.common.BufferPool].
 */
object AkkBlockPackerDirect : BlockPacker, Closeable {

    override val blockSize: Int = BLOCK_SIZE

    private val pool = Pools.io()
    private val scratch: ByteBuffer = pool.get()
    private val crc = CRC32()

    /* ---------- public API ---------- */

    override fun addRecord(record: ByteBuffer, consumer: (ByteBuffer) -> Unit) {
        val len = record.remaining()
        require(len <= MAX_RECORD) { "Record of $len B exceeds $MAX_RECORD B" }

        if (scratch.position() + len > PAYLOAD_LIMIT) emitBlock(consumer)
        scratch.put(record.duplicate())
    }

    override fun flush(consumer: (ByteBuffer) -> Unit) {
        if (scratch.position() > 0) emitBlock(consumer)
    }

    override fun close() {
        pool.release(scratch)
    }

    /* ---------- internal ---------- */

    private fun emitBlock(consumer: (ByteBuffer) -> Unit) {
        val payloadLen = scratch.position()
        scratch.flip()

        val block = pool.get()
        block.putInt(payloadLen)
        block.put(scratch)

        while (block.position() < PAYLOAD_LIMIT + 4) block.put(0)

        crc.reset()
        block.duplicate().limit(4 + payloadLen).position(0).let { crc.update(it) }

        block.position(PAYLOAD_LIMIT + 4)
        block.putInt(crc.value.toInt())
        block.flip()

        consumer(block.asReadOnlyBuffer())                     // caller sees read-only view
        pool.release(block)

        scratch.clear()
    }
}
