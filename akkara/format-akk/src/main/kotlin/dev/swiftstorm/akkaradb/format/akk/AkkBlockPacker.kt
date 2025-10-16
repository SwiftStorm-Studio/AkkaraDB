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

package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.AKHdr32
import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.types.U32
import dev.swiftstorm.akkaradb.common.types.U64
import dev.swiftstorm.akkaradb.format.api.BlockPacker
import java.io.Closeable

/**
 * Direct-write 32 KiB block packer using ByteBufferL and AKHdr32.
 *
 * Block layout:
 *  [0..3]     : payloadLen (u32 LE)
 *  [4..4+N)   : payload = repeated records:
 *               AKHdr32 (32B) + key + value
 *  [4+N..-5]  : zero padding
 *  [-4..-1]   : CRC32C over bytes [0 .. BLOCK_SIZE-4)
 *
 * Responsibilities:
 *  - Write AKHdr32 at an absolute payload offset (no raw ByteBuffer usage)
 *  - Copy key and value once each via ByteBufferL.putBytes(...)
 *  - Seal with payloadLen + zero padding + CRC32C (crc32cRange)
 */
class AkkBlockPacker(
    private val onBlockReady: (ByteBufferL) -> Unit,
    private val pool: BufferPool = Pools.io()
) : BlockPacker, Closeable {

    private var buf: ByteBufferL = pool.get(BLOCK_SIZE)
    private var payloadPos: Int = 4
    private var openBlock = false
    private var closed = false

    override fun beginBlock() {
        ensureOpen()
        if (openBlock && payloadPos > 4) endBlock()
        buf.clear()
        // reserve [0..3] for payloadLen
        buf.position = 4
        payloadPos = 4
        openBlock = true
    }

    override fun tryAppend(
        key: ByteBufferL,
        value: ByteBufferL,
        seq: U64,
        flags: Int,
        keyFP64: U64,
        miniKey: U64
    ): Boolean {
        ensureOpen()
        if (!openBlock) beginBlock()

        val kLen = key.remaining
        val vLen = value.remaining
        require(kLen in 0..0xFFFF) { "key too long: $kLen" }
        require(flags in 0..0xFF) { "flags (u8) out of range: $flags" }

        val need = AKHdr32.SIZE + kLen + vLen
        if (payloadPos + need > PAYLOAD_LIMIT) return false

        // --- write AKHdr32 at absolute position (ByteBufferL API only) ---
        buf.putHeader32(
            at = payloadPos,
            kLen = kLen,
            vLen = U32.of(vLen.toLong()),
            seq = seq,
            flags = flags,
            keyFP64 = keyFP64,
            miniKey = miniKey
        )

        // --- copy key then value via ByteBufferL bulk copy (no raw access) ---
        buf.position = payloadPos + AKHdr32.SIZE
        if (kLen > 0) buf.put(key, kLen)
        if (vLen > 0) buf.put(value, vLen)

        payloadPos += need
        buf.position = payloadPos
        return true
    }

    override fun endBlock() {
        ensureOpen()
        if (!openBlock) return
        if (payloadPos <= 4) {
            // empty: drop current work buffer content and keep it for reuse
            openBlock = false
            buf.position = 0
            buf.limit = buf.capacity
            return
        }

        // (1) write payloadLen at head
        val payloadLen = payloadPos - 4
        buf.at(0).i32 = payloadLen

        // (2) zero-pad [payloadPos .. BLOCK_SIZE-4)
        val padBytes = (BLOCK_SIZE - 4) - payloadPos
        if (padBytes > 0) {
            buf.position = payloadPos
            buf.fillZero(padBytes) // uses ByteBufferL's internal zero chunk
        }

        // (3) stamp CRC32C over [0 .. BLOCK_SIZE-4)
        val crc = buf.crc32cRange(0, BLOCK_SIZE - 4)
        buf.at(BLOCK_SIZE - 4).i32 = crc

        // (4) emit & rotate a fresh buffer
        val full = buf
        buf = pool.get(BLOCK_SIZE)
        payloadPos = 4
        openBlock = false
        onBlockReady(full)
    }

    override fun flush() {
        ensureOpen()
        if (openBlock && payloadPos > 4) endBlock()
        else openBlock = false
    }

    override fun close() {
        if (closed) return
        try {
            flush()
        } finally {
            pool.release(buf)
            closed = true
        }
    }

    private fun ensureOpen() {
        check(!closed) { "packer already closed" }
    }
}
