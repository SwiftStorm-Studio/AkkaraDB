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

package dev.swiftstorm.akkaradb.engine.memtable

import dev.swiftstorm.akkaradb.common.*
import java.io.Closeable
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * MemTable (v3):
 *  - Lock-sharded in-memory KV (byte-wise lexicographic key order)
 *  - Replacement rule mirrors v3 invariants (see shouldReplace)
 *  - Flush signaling via *seal & swap* (no long hold on hot path)
 *  - Single flusher thread to coalesce I/O (writer-friendly)
 *  - Optional range iterator across shards (merged on-demand)
 */
class MemTable(
    shardCount: Int = Runtime.getRuntime().availableProcessors().coerceAtLeast(2),
    /** Soft threshold per shard (bytes) to request a flush. */
    private val thresholdBytesPerShard: Long = (64L * 1024 * 1024) / shardCount,
    /** Callback invoked by the flusher with an immutable sealed batch. */
    private val onFlush: (List<MemRecord>) -> Unit,
    flusherThreadName: String = "akkaradb-mem-flusher"
) : Closeable {

    private val closed = AtomicBoolean(false)
    private val shards: Array<Shard>
    private val flusher: Flusher

    private val seqGen = AtomicLong(0)

    init {
        val q = MpscQueue<List<MemRecord>>()
        this.flusher = Flusher(
            queue = q,
            sink = onFlush,
            onFlushed = { batch ->
                shards.forEach { it.onFlushed(batch) } // 全shardに通知（バッチを保持していたshardだけが削除）
            },
            tname = flusherThreadName
        ).apply { start() }
        this.shards = Array(shardCount) { idx ->
            Shard(
                shardId = idx,
                queue = q,
                softThresholdBytes = thresholdBytesPerShard
            )
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        // Final seal of all shards and stop flusher (drain remaining)
        shards.forEach { it.forceSealAndEnqueueIfNotEmpty() }
        flusher.stopAndDrain()
    }

    // ─────────── public API (engine-facing) ───────────

    fun lastSeq(): Long = seqGen.get()
    fun nextSeq(): Long = seqGen.incrementAndGet()

    fun size(): Int = shards.sumOf { it.size() }

    /** Best-effort total memory accounting (bytes). */
    fun approxBytes(): Long = shards.sumOf { it.currentBytes() }

    /**
     * Get the newest record for key (null if absent).
     * Requires that callers pass read-only slices for keys.
     */
    fun get(key: ByteBufferL): MemRecord? {
        if (closed.get()) return null
        val h = fnv1a32(key)
        return shards[shardIdx(h)].get(key, h)
    }

    /** Put value. Returns the applied sequence (usually caller-provided). */
    fun put(key: ByteBufferL, value: ByteBufferL, seq: Long, flags: Byte = 0): Long {
        if (closed.get()) return seq
        val rec = memRecordOf(key, value, seq, flags)
        shards[shardIdx(rec.keyHash)].put(rec)
        return seq
    }

    /** Tombstone insert. */
    fun delete(key: ByteBufferL, seq: Long): Long {
        if (closed.get()) return seq
        val rec = memRecordOf(key, MemRecord.EMPTY, seq, RecordFlags.TOMBSTONE)
        shards[shardIdx(rec.keyHash)].put(rec)
        return seq
    }

    /** Compare-and-swap by sequence (monotonic). */
    fun compareAndSwap(key: ByteBufferL, expectedSeq: Long, newValue: ByteBufferL?): Boolean {
        if (closed.get()) return false
        val k0 = key.duplicate().position(0)
        val h = fnv1a32(k0)
        val nextSeq = lastSeq() + 1
        val cas = shards[shardIdx(h)].cas(k0, h, expectedSeq, nextSeq, newValue)
        if (cas) seqGen.set(nextSeq)
        return cas
    }

    /** Non-blocking flush hint for all shards. */
    fun flushHint() {
        shards.forEach { it.maybeSealAndEnqueue() }
    }

    /** Range iterator (merged across shards on demand). */
    fun iterator(range: KeyRange = KeyRange.ALL): Sequence<MemRecord> = sequence {
        val perShard = shards.map { it.snapshotRange(range) }
        val heap = PriorityQueue<Peekable>()
        perShard.filter { it.hasNext() }.forEach { heap.add(Peekable(it)) }

        var lastKey: ByteBufferL? = null
        var bestForKey: MemRecord? = null

        suspend fun SequenceScope<MemRecord>.emitBestIfAny() {
            bestForKey?.let { yield(it) }
            bestForKey = null
        }

        while (heap.isNotEmpty()) {
            val p = heap.poll()
            val rec = p.next()

            if (lastKey == null || lexCompare(rec.key, lastKey) != 0) {
                this.emitBestIfAny()
                lastKey = rec.key
                bestForKey = rec
            } else if (rec.seq > (bestForKey?.seq ?: Long.MIN_VALUE)) {
                bestForKey = rec
            }

            if (p.hasNext()) heap.add(p)
        }
        this.emitBestIfAny()
    }

    // ─────────── internals ───────────

    private fun shardIdx(hash: Int): Int = Math.floorMod(hash, shards.size)

    data class KeyRange(val start: ByteBufferL?, val endExclusive: ByteBufferL?) {
        companion object {
            val ALL = KeyRange(null, null)
        }

        fun contains(k: ByteBufferL): Boolean {
            if (start != null && lexCompare(k, start) < 0) return false
            if (endExclusive != null && lexCompare(k, endExclusive) >= 0) return false
            return true
        }
    }

    private class Shard(
        private val shardId: Int,
        private val queue: MpscQueue<List<MemRecord>>, // N:1 to single flusher
        private val softThresholdBytes: Long
    ) {
        private val lock = ReentrantReadWriteLock()

        // Access-ordered LRU-ish map to enable optional eviction hooks (not used by default)
        private var active: TreeMap<Key, MemRecord> = TreeMap()
        private val immutables = ArrayDeque<TreeMap<Key, MemRecord>>()

        private var bytes: Long = 0L
        private var flushPending = AtomicBoolean(false)
        private var nextFlushAt = softThresholdBytes

        fun size(): Int = lock.read {
            var total = active.size
            for (imm in immutables) total += imm.size
            total
        }

        fun currentBytes(): Long = lock.read { bytes }

        fun get(key: ByteBufferL, keyHash: Int): MemRecord? = lock.read {
            val k = Key(key, keyHash)
            active[k]?.let { return it }
            for (imm in immutables) {
                imm[k]?.let { return it }
            }
            return null
        }

        fun cas(
            key: ByteBufferL,
            keyHash: Int,
            expectedSeq: Long,
            newSeq: Long,
            newValue: ByteBufferL?
        ): Boolean {
            var ok = false
            lock.write {
                val k = Key(key, keyHash)

                // Search in active first
                var cur = active[k]

                // If not found in active, check immutables (read-only)
                if (cur == null) {
                    for (imm in immutables) {
                        val found = imm[k]
                        if (found != null) {
                            cur = found
                            break
                        }
                    }
                }

                if ((cur?.seq ?: -1L) == expectedSeq) {
                    val next = when {
                        newValue == null -> cur!!.asTombstone(newSeq)
                        else -> cur!!.withValueAndSeq(newValue, newSeq)
                    }
                    if (shouldReplace(cur, next)) {
                        active[k] = next  // always update active
                        bytes += sizeOf(next) - sizeOf(cur)
                        triggerIfNeeded()
                        ok = true
                    }
                }
            }
            if (ok) maybeSealOutsideLock()
            return ok
        }

        fun put(rec: MemRecord) {
            var needSeal = false
            lock.write {
                val k = Key(rec.key, rec.keyHash)
                val prev = active[k]
                if (prev == null || shouldReplace(prev, rec)) {
                    active[k] = rec
                    bytes += sizeOf(rec) - (prev?.let { sizeOf(it) } ?: 0)
                }
                if (triggerIfNeeded()) needSeal = true
            }
            if (needSeal) sealAndEnqueue()
        }

        fun snapshotRange(range: KeyRange): Iterator<MemRecord> = lock.read {
            val fromKey = range.start?.let { Key(it, fnv1a32(it)) }
            val toKey = range.endExclusive?.let { Key(it, fnv1a32(it)) }

            // active map view
            val activeView: NavigableMap<Key, MemRecord> = when {
                fromKey != null && toKey != null -> active.subMap(fromKey, true, toKey, false)
                fromKey != null -> active.tailMap(fromKey, true)
                toKey != null -> active.headMap(toKey, false)
                else -> active
            }

            val merged = ArrayList<MemRecord>(activeView.size)
            merged.addAll(activeView.values)

            // include immutables
            for (imm in immutables) {
                val immView: NavigableMap<Key, MemRecord> = when {
                    fromKey != null && toKey != null -> imm.subMap(fromKey, true, toKey, false)
                    fromKey != null -> imm.tailMap(fromKey, true)
                    toKey != null -> imm.headMap(toKey, false)
                    else -> imm
                }
                merged.addAll(immView.values)
            }

            merged.sortWith { a, b -> lexCompare(a.key, b.key) }
            merged.iterator()
        }

        fun maybeSealAndEnqueue() {
            if (flushPending.compareAndSet(false, true)) sealAndEnqueue()
        }

        fun forceSealAndEnqueueIfNotEmpty() {
            var has = false
            lock.read { has = active.isNotEmpty() || immutables.isNotEmpty() }
            if (has && flushPending.compareAndSet(false, true)) sealAndEnqueue()
        }

        fun onFlushed(batch: List<MemRecord>) {
            lock.write {
                immutables.removeIf { imm -> imm.values === batch }
            }
        }

        private fun sizeOf(r: MemRecord): Long = r.approxSizeBytes.toLong()

        private fun triggerIfNeeded(): Boolean {
            if (bytes >= nextFlushAt && flushPending.compareAndSet(false, true)) {
                nextFlushAt = (softThresholdBytes * 9 / 10)
                    .coerceAtLeast(softThresholdBytes / 2)
                return true
            }
            return false
        }

        private fun maybeSealOutsideLock() {
            if (flushPending.get()) sealAndEnqueue()
        }

        private fun sealAndEnqueue() {
            val sealed: TreeMap<Key, MemRecord>
            lock.write {
                if (active.isEmpty()) {
                    nextFlushAt = softThresholdBytes
                    flushPending.set(false)
                    return
                }
                sealed = active
                immutables.add(sealed)
                active = TreeMap()
                bytes = 0L
                nextFlushAt = softThresholdBytes
            }

            val batch = ArrayList<MemRecord>(sealed.size)
            for (v in sealed.values) batch.add(v)
            try {
                queue.offer(batch)
            } finally {
                flushPending.set(false)
            }
        }

        private data class Key(val key: ByteBufferL, val hash: Int) : Comparable<Key> {
            override fun hashCode(): Int = hash
            override fun equals(other: Any?): Boolean =
                other is Key && bytesEqual(this.key, other.key)
            override fun compareTo(other: Key): Int = lexCompare(this.key, other.key)
        }
    }

    private class Flusher(
        private val queue: MpscQueue<List<MemRecord>>,
        private val sink: (List<MemRecord>) -> Unit,
        private val onFlushed: (List<MemRecord>) -> Unit,
        tname: String
    ) {
        private val running = AtomicBoolean(true)
        private val th = Thread({ runLoop() }, tname).apply { isDaemon = true }

        fun start() = th.start()
        fun stopAndDrain() {
            running.set(false)
            th.join()
            while (true) {
                val b = queue.poll() ?: break
                sink(b)
                onFlushed(b)
            }
        }

        private fun runLoop() {
            while (running.get()) {
                val b = queue.poll()
                if (b != null) {
                    try {
                        sink(b)
                    } finally {
                        onFlushed(b)
                    }
                } else {
                    LockSupport.parkNanos(200_000)
                }
            }
        }
    }

    /** Simple unbounded MPSC backed by ConcurrentLinkedQueue. */
    private class MpscQueue<T> {
        private val q = ConcurrentLinkedQueue<T>()
        fun offer(v: T): Boolean = q.offer(v)
        fun poll(): T? = q.poll()
    }

    // K-way merge helper
    private class Peekable(private val it: Iterator<MemRecord>) : Comparable<Peekable> {
        private var head: MemRecord? = if (it.hasNext()) it.next() else null
        fun peek(): MemRecord? = head
        fun hasNext(): Boolean = head != null
        fun next(): MemRecord {
            val r = head ?: throw NoSuchElementException()
            head = if (it.hasNext()) it.next() else null
            return r
        }

        override fun compareTo(other: Peekable): Int =
            lexCompare(this.peek()!!.key, other.peek()!!.key)
    }
}
