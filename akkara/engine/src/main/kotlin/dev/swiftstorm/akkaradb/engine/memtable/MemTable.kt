package dev.swiftstorm.akkaradb.engine.memtable

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.hashOfRemaining
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Sharded MemTable with *seal & swap* and a single flusher thread.
 *
 * Goals:
 * - Flush work never blocks the hot path (put/get).
 * - O(1) seal: swap active map under write lock, then copy outside lock.
 * - I/O is serialized via a single flusher to improve write coalescing.
 * - Optional hysteresis to avoid flush storms.
 */
class MemTable(
    private val shardCount: Int = Runtime.getRuntime().availableProcessors().coerceAtLeast(2),
    /** target soft threshold per-shard in bytes */
    private val thresholdBytesPerShard: Long,
    /** called by flusher thread with a sealed batch */
    private val onFlush: (MutableList<Record>) -> Unit,
    threadName: String = "akkaradb-mem-flusher"
) {
    private val closed = AtomicBoolean(false)
    private val flusher: Flusher
    private val shards: Array<Shard>

    private val seqGen = AtomicLong()

    init {
        require(shardCount >= 1)
        val q = MpscQueue<MutableList<Record>>()
        this.flusher = Flusher(q, onFlush, threadName)
        this.shards = Array(shardCount) { idx ->
            Shard(
                shardId = idx,
                queue = q,
                softThresholdBytes = thresholdBytesPerShard
            )
        }
        flusher.start()
    }

    fun close() {
        if (closed.compareAndSet(false, true)) {
            // request final flush of all shards
            shards.forEach { it.forceSealAndEnqueueIfNotEmpty() }
            flusher.stopAndDrain()
        }
    }

    fun size(): Int = shards.sumOf { it.size() }

    fun getAll(): List<Record> =
        shards.flatMap { it.snapshotAll() }

    fun get(key: ByteBufferL, keyHash: Int = key.hashOfRemaining()): Record? {
        if (closed.get()) return null
        val idx = Math.floorMod(keyHash, shardCount)
        return shards[idx].get(key, keyHash)
    }

    fun put(record: Record): Boolean {
        if (closed.get()) return false
        val shardIdx = Math.floorMod(record.keyHash, shardCount)
        return shards[shardIdx].put(record)
    }

    fun lastSeq(): Long = seqGen.get()

    fun nextSeq(): Long = seqGen.incrementAndGet()

    /** Hint to flush; non-blocking. */
    fun flush() {
        shards.forEach { it.maybeSealAndEnqueue() }
    }

    fun evictOldest(n: Int = 1) {
        shards.forEach { it.evictOldest(n) }
    }

    // ---------------------------------------------------------------------
    // Shard
    // ---------------------------------------------------------------------
    private class Shard(
        private val shardId: Int,
        private val queue: MpscQueue<MutableList<Record>>, // N:1 to flusher
        private val softThresholdBytes: Long,
    ) {
        private val lock = ReentrantReadWriteLock()

        /** next flush trigger; updated with hysteresis after each flush. */
        private var nextFlushAt = softThresholdBytes

        /** Active map (LRU-ish by access order). */
        private var active: LinkedHashMap<Key, Record> = newLruMap()

        /**
         * Memory accounting for current active map; only mutated under write lock.
         * Keeping AtomicLong out of hot path.
         */
        private var currentBytes: Long = 0

        /** prevent duplicate enqueue while a sealed batch from this shard is pending. */
        private val flushPending = AtomicBoolean(false)

        fun size(): Int = lock.read { active.size }

        fun snapshotAll(): List<Record> = lock.read { ArrayList(active.values) }

        fun get(key: ByteBufferL, keyHash: Int): Record? = lock.read {
            active[Key(key, keyHash)]
        }

        fun put(r: Record): Boolean {
            var needSeal = false
            lock.write {
                val k = Key(r.key, r.keyHash)
                val prev = active.put(k, r)
                if (prev != null) {
                    currentBytes -= sizeOf(prev)
                }
                currentBytes += sizeOf(r)
                // soft trigger only; actual enqueue happens outside lock
                if (currentBytes >= nextFlushAt && flushPending.compareAndSet(false, true)) {
                    needSeal = true
                }
            }
            if (needSeal) sealAndEnqueue()
            return true
        }

        fun evictOldest(n: Int) {
            lock.write {
                repeat(n.coerceAtMost(active.size)) {
                    val it = active.entries.iterator()
                    if (!it.hasNext()) return
                    val e = it.next()
                    currentBytes -= sizeOf(e.value)
                    it.remove()
                }
            }
        }

        fun maybeSealAndEnqueue() {
            if (flushPending.compareAndSet(false, true)) {
                sealAndEnqueue()
            }
        }

        fun forceSealAndEnqueueIfNotEmpty() {
            var has = false
            lock.read { has = active.isNotEmpty() }
            if (has && flushPending.compareAndSet(false, true)) {
                sealAndEnqueue()
            }
        }

        private fun sealAndEnqueue() {
            // 1) swap map under write lock (O(1))
            val sealed: LinkedHashMap<Key, Record>
            lock.write {
                if (active.isEmpty()) {
                    flushPending.set(false)
                    return
                }
                sealed = active
                active = newLruMap()
                currentBytes = 0L
                // Hysteresis: next time trigger a bit earlier to coalesce bursts
                nextFlushAt = (softThresholdBytes * 0.8).toLong().coerceAtLeast(softThresholdBytes / 2)
            }
            // 2) Copy values to an array list OUTSIDE the lock
            val batch = ArrayList<Record>(sealed.size)
            for (e in sealed.values) batch.add(e)
            // 3) enqueue for the single flusher; if rejected, mark as not pending so future calls can retry
            if (!queue.offer(batch)) {
                // very unlikely with our unbounded MPSC, but keep safety
                flushPending.set(false)
            }
        }

        private fun sizeOf(r: Record): Long = r.approxSizeBytes.toLong()

        private fun newLruMap(): LinkedHashMap<Key, Record> =
            object : LinkedHashMap<Key, Record>(128, 0.75f, /*accessOrder*/ true) {
                override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Key, Record>?): Boolean {
                    return false // manual eviction
                }
            }

        private data class Key(val ro: ByteBufferL, val hash: Int) {
            override fun hashCode(): Int = hash
            override fun equals(other: Any?): Boolean =
                other is Key && contentEquals(ro, other.ro)

            private fun contentEquals(a: ByteBufferL, b: ByteBufferL): Boolean {
                val aa = a.asReadOnlyByteBuffer().slice()
                val bb = b.asReadOnlyByteBuffer().slice()
                if (aa.remaining() != bb.remaining()) return false
                return aa.mismatch(bb) == -1
            }
        }
    }

    // ---------------------------------------------------------------------
    // Flusher (single thread)
    // ---------------------------------------------------------------------
    private class Flusher(
        private val queue: MpscQueue<MutableList<Record>>,
        private val sink: (MutableList<Record>) -> Unit,
        threadName: String
    ) {
        private val running = AtomicBoolean(true)
        private val thread: Thread = Thread({ runLoop() }, threadName).apply { isDaemon = true }

        fun start() = thread.start()

        fun stopAndDrain() {
            running.set(false)
            thread.join()
            // drain any leftovers (best-effort, single-threaded now)
            while (true) {
                val b = queue.poll() ?: break
                try {
                    sink(b)
                } catch (_: Throwable) {
                }
            }
        }

        private fun runLoop() {
            while (running.get()) {
                val batch = queue.poll()
                if (batch != null) {
                    try {
                        sink(batch)
                    } catch (_: Throwable) {
                    }
                } else {
                    // small park to reduce CPU when idle
                    Thread.onSpinWait()
                }
            }
        }
    }

    // ---------------------------------------------------------------------
    // Minimal unbounded MPSC queue (backed by ConcurrentLinkedQueue)
    // If JCTools is available, replace with MpscArrayQueue for lower overhead.
    // ---------------------------------------------------------------------
    private class MpscQueue<T> {
        private val q = ConcurrentLinkedQueue<T>()
        fun offer(v: T): Boolean = q.offer(v)
        fun poll(): T? = q.poll()
    }
}
