package dev.swiftstorm.akkaradb.common

import java.nio.ByteBuffer

class FixedBufferPool(
    private val capacity: Int,
    private val bufSize: Int = BlockConst.BLOCK_SIZE
) : BufferPool {

    private val queue = ArrayDeque<ByteBuffer>(capacity)

    override fun get(size: Int): ByteBuffer =
        synchronized(queue) { queue.removeFirstOrNull() } ?: ByteBuffer.allocateDirect(bufSize)

    override fun release(buf: ByteBuffer) {
        buf.clear()
        synchronized(queue) {
            if (queue.size < capacity) queue.addLast(buf)
        }
    }

    override fun close() = queue.clear()
}