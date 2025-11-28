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

package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import java.io.Closeable
import java.io.IOException
import java.nio.channels.ClosedChannelException
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport
import kotlin.concurrent.thread

/**
 * v3 WAL writer (single file, append‑only, group‑commit N or T, durable‑before‑ACK).
 *
 * Ack semantics:
 *  - append() blocks until the flusher fsyncs a batch that *includes* the caller's LSN.
 */
class WalWriter(
    private val path: Path,
    private val groupN: Int = 32,
    private val groupTmicros: Long = 500, // T=500µs default
    private val fastMode: Boolean = false,
) : Closeable {

    private val ch: FileChannel
    private val running = AtomicBoolean(true)
    private val nextLsn = AtomicLong(0)

    // append→queue framed buffers; flusher writes and fsyncs batches
    private data class Item(val lsn: Long, val frame: ByteBufferL, val waiter: Waiter)
    private class Waiter {
        @Volatile
        var done = false
        private val thread = Thread.currentThread()

        fun await(timeoutMicros: Long) {
            val deadline = System.nanoTime() + TimeUnit.MICROSECONDS.toNanos(timeoutMicros)
            while (!done && System.nanoTime() < deadline) {
                LockSupport.parkNanos(10_000)
            }
        }

        fun signal() {
            done = true
            LockSupport.unpark(thread)
        }
    }

    private val q = LinkedBlockingQueue<Item>(1024)
    private val io = Pools.io()
    private val flusher = thread(name = "wal-flusher", isDaemon = true) { flushLoop() }

    init {
        if (path.parent != null && !Files.exists(path.parent)) Files.createDirectories(path.parent)
        ch = FileChannel.open(path, CREATE, WRITE, APPEND)
    }

    /** Encode a WalOp and append; returns durable LSN (blocks until group commit). */
    fun append(op: WalOp): Long {
        check(running.get()) { "WAL is closed" }
        val lsn = nextLsn.incrementAndGet()

        // Encode payload (AKHdr32 + key/value for ADD/DEL)
        val payload = io.get(estimatePayloadSize(op))
        op.encodeInto(payload)
        payload.limit = payload.position
        payload.position = 0

        // Frame it: [len][payload][crc]
        val frame = WalFraming.frame(payload)
        io.release(payload)

        val waiter = Waiter()
        q.put(Item(lsn, frame, waiter))

        if (!fastMode) {
            waiter.await(timeoutMicros = groupTmicros * 10)
            if (!waiter.done) {
                throw IOException("WAL fsync timeout")
            }
        }

        return lsn
    }

    fun forceSync() {
        if (!running.get() || !ch.isOpen) return

        synchronized(ch) {
            runCatching {
                if (ch.isOpen) {
                    ch.force(false)
                }
            }.onFailure {}
        }
    }


    override fun close() {
        if (!running.getAndSet(false)) return

        q.offer(Item(-1, ByteBufferL.allocate(0), Waiter().apply { done = true }))

        flusher.join(2000)

        synchronized(ch) {
            runCatching { ch.force(false) }
            runCatching { ch.close() }
        }
    }

    // ─────────── internals ───────────

    private fun estimatePayloadSize(op: WalOp): Int = when (op) {
        is WalOp.Add -> 32 + op.key.size + op.value.size
        is WalOp.Delete -> 32 + op.key.size
    }

    private fun flushLoop() {
        val batch = ArrayList<Item>(groupN.coerceAtLeast(1))
        try {
            while (running.get() || q.isNotEmpty()) {
                val first = q.poll(groupTmicros, TimeUnit.MICROSECONDS) ?: continue
                if (first.lsn < 0) break

                batch.add(first)
                q.drainTo(batch, groupN - 1)

                synchronized(ch) {
                    for (it in batch) {
                        val bb = it.frame.rawDuplicate()
                        while (bb.hasRemaining()) ch.write(bb)
                        io.release(it.frame)
                    }
                    ch.force(false)
                }

                for (it in batch) {
                    it.waiter.done = true
                    it.waiter.signal()
                }
                batch.clear()
            }
        } catch (e: ClosedChannelException) {
            if (running.get()) throw e
        } finally {
            runCatching {
                synchronized(ch) { ch.force(false) }
            }
        }
    }

    /** Truncate WAL file to zero length. Intended to be called after a durable checkpoint. */
    fun truncate() {
        synchronized(ch) {
            ch.truncate(0)
            ch.force(true)
        }
    }
}