// ─────────────────────────────────────────────────────────────────────────────
// File: dev/swiftstorm/akkaradb/engine/memtable/MemTable.kt
// ─────────────────────────────────────────────────────────────────────────────
package dev.swiftstorm.akkaradb.engine.memtable

import dev.swiftstorm.akkaradb.common.*
import java.io.Closeable
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
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
        this.flusher = Flusher(q, onFlush, flusherThreadName).apply { start() }
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
        val h = fnv1a32(key)
        return shards[shardIdx(h)].cas(key, h, expectedSeq, newValue)
    }

    /** Non-blocking flush hint for all shards. */
    fun flushHint() {
        shards.forEach { it.maybeSealAndEnqueue() }
    }

    /** Range iterator (merged across shards on demand). */
    fun iterator(range: KeyRange = KeyRange.ALL): Sequence<MemRecord> = sequence {
        val perShard = shards.map { it.snapshotRange(range) }
        // K-way merge by seq preference? For MemTable we only need lexicographic key order;
        // within the same key only the newest survives. We'll compress on merge.
        val heap = PriorityQueue<Peekable>(compareBy { it.peek()?.key?.getI32At(0) })
        perShard.filter { it.hasNext() }.forEach { heap.add(Peekable(it)) }
        var lastKey: ByteBufferL? = null
        var lastSeq = Long.MIN_VALUE
        while (heap.isNotEmpty()) {
            val p = heap.poll()
            val rec = p.next()
            // collapse duplicates with same key keeping highest seq
            if (lastKey == null || lexCompare(rec.key, lastKey) != 0) {
                // new key boundary
                yield(rec)
                lastKey = rec.key
                lastSeq = rec.seq
            } else if (rec.seq > lastSeq) {
                // replace with newer
                yield(rec)
                lastSeq = rec.seq
            }
            if (p.hasNext()) heap.add(p)
        }
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
        private var map: LinkedHashMap<Key, MemRecord> = newMap()

        private var bytes: Long = 0L
        private var flushPending = AtomicBoolean(false)
        private var nextFlushAt = softThresholdBytes

        fun size(): Int = lock.read { map.size }
        fun currentBytes(): Long = lock.read { bytes }

        fun get(key: ByteBufferL, keyHash: Int): MemRecord? = lock.read {
            map[Key(key, keyHash)]
        }

        fun cas(key: ByteBufferL, keyHash: Int, expectedSeq: Long, newValue: ByteBufferL?): Boolean {
            var ok = false
            lock.write {
                val k = Key(key, keyHash)
                val cur = map[k]
                if ((cur?.seq ?: -1L) == expectedSeq) {
                    val next = if (newValue == null) cur!!.asTombstone() else cur!!.withValue(newValue)
                    if (shouldReplace(cur, next)) {
                        map[k] = next
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
                val prev = map[k]
                if (prev == null) {
                    map[k] = rec
                    bytes += sizeOf(rec)
                } else {
                    if (shouldReplace(prev, rec)) {
                        map[k] = rec
                        bytes += sizeOf(rec) - sizeOf(prev)
                    }
                }
                if (triggerIfNeeded()) needSeal = true
            }
            if (needSeal) sealAndEnqueue()
        }

        fun snapshotRange(range: KeyRange): Iterator<MemRecord> = lock.read {
            val it = map.values.iterator()
            object : Iterator<MemRecord> {
                var next: MemRecord? = seek()
                override fun hasNext(): Boolean = next != null
                override fun next(): MemRecord {
                    val r = next ?: throw NoSuchElementException()
                    next = seek(); return r
                }

                private fun seek(): MemRecord? {
                    while (it.hasNext()) {
                        val r = it.next()
                        if (range.contains(r.key)) return r
                    }
                    return null
                }
            }
        }

        fun maybeSealAndEnqueue() {
            if (flushPending.compareAndSet(false, true)) sealAndEnqueue()
        }

        fun forceSealAndEnqueueIfNotEmpty() {
            var has = false
            lock.read { has = map.isNotEmpty() }
            if (has && flushPending.compareAndSet(false, true)) sealAndEnqueue()
        }

        private fun newMap(): LinkedHashMap<Key, MemRecord> = object : LinkedHashMap<Key, MemRecord>(256, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Key, MemRecord>?): Boolean = false
        }

        private fun sizeOf(r: MemRecord): Long = r.approxSizeBytes.toLong()

        private fun triggerIfNeeded(): Boolean {
            if (bytes >= nextFlushAt && flushPending.compareAndSet(false, true)) {
                // next time trigger earlier to avoid storms
                nextFlushAt = (softThresholdBytes * 0.8).toLong().coerceAtLeast(softThresholdBytes / 2)
                return true
            }
            return false
        }

        private fun maybeSealOutsideLock() {
            if (flushPending.get()) sealAndEnqueue()
        }

        private fun sealAndEnqueue() {
            // swap under write lock (O(1))
            val sealed: LinkedHashMap<Key, MemRecord>
            lock.write {
                if (map.isEmpty()) {
                    flushPending.set(false)
                    return
                }
                sealed = map
                map = newMap()
                bytes = 0L
            }
            // copy values out of lock
            val batch = ArrayList<MemRecord>(sealed.size)
            for (v in sealed.values) batch.add(v)
            if (!queue.offer(batch)) {
                // should not happen with unbounded queue; allow future retries
                flushPending.set(false)
            }
        }

        private data class Key(val key: ByteBufferL, val hash: Int) {
            override fun hashCode(): Int = hash
            override fun equals(other: Any?): Boolean =
                other is Key && bytesEqual(this.key, other.key)
        }
    }

    private class Flusher(
        private val queue: MpscQueue<List<MemRecord>>,
        private val sink: (List<MemRecord>) -> Unit,
        tname: String
    ) {
        private val running = AtomicBoolean(true)
        private val th = Thread({ runLoop() }, tname).apply { isDaemon = true }

        fun start() = th.start()

        fun stopAndDrain() {
            running.set(false)
            th.join()
            // Drain leftovers (best-effort)
            while (true) sink(queue.poll() ?: break)
        }

        private fun runLoop() {
            while (running.get()) {
                val b = queue.poll()
                if (b != null) {
                    try {
                        sink(b)
                    } catch (_: Throwable) {
                    }
                } else {
                    Thread.onSpinWait()
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
