package dev.swiftstorm.akkaradb.common

import java.nio.ByteBuffer

/** Strategy for allocating a ByteBuffer. Should return a buffer with arbitrary order; order is enforced to LE by caller. */
fun interface ByteBufferAllocator {
    fun allocate(capacity: Int, direct: Boolean): ByteBuffer

    companion object {
        /** Default allocator using JDK heap/direct. */
        val Default: ByteBufferAllocator = ByteBufferAllocator { cap, direct ->
            if (direct) ByteBuffer.allocateDirect(cap) else ByteBuffer.allocate(cap)
        }
    }
}