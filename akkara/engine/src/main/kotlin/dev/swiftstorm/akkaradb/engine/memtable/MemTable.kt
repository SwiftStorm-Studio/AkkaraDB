package dev.swiftstorm.akkaradb.engine.memtable

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.hashOfRemaining
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.math.max

/**
 * Sharded MemTable for scalability.
 * - Keys are distributed across shards using (key.hashOfRemaining() % shardCount).
 * - Each shard maintains its own lock, LRU, and flush boundary.
 */
class MemTable(
    private val thresholdBytes: Long,
    private val onFlush: (MutableList<Record>) -> Unit,
    private val onEvict: ((List<Record>) -> Unit)? = null,
    private val maxEntries: Int? = null,
    private val shardCount: Int = Runtime.getRuntime().availableProcessors(),
    internal val executor: ExecutorService = Executors.newFixedThreadPool(
        Runtime.getRuntime().availableProcessors()
    ) { r -> Thread(r, "memtable-flush").apply { isDaemon = true } }
) : AutoCloseable {

    private val shards = Array(shardCount) { idx ->
        val per = max(1L, thresholdBytes / shardCount)   // ★
        Shard(idx, per, onFlush, onEvict, maxEntries)
    }

    private val closed = AtomicBoolean(false)

    fun get(key: ByteBufferL): Record? {
        val h = key.hashOfRemaining()
        val shardIdx = Math.floorMod(h, shardCount)
        return shards[shardIdx].get(key)
    }

    fun getAll(): List<Record> = shards.flatMap { it.getAll() }

    fun put(record: Record): Boolean {
        if (closed.get()) return false
        val shardIdx = Math.floorMod(record.keyHash, shardCount)
        return shards[shardIdx].put(record)
    }

    fun flush() {
        shards.forEach { it.flush() }
    }

    fun evictOldest(n: Int = 1) {
        shards.forEach { it.evictOldest(n) }
    }

    fun lastSeq(): Long = shards.maxOf { it.lastSeq() }
    fun nextSeq(): Long {
        val s = shards.maxOf { it.lastSeq() }
        return max(s + 1, 1)
    }

    override fun close() {
        closed.set(true)
        shards.forEach { it.flush() }
        executor.shutdown()
    }

    /* ────────── Shard class ────────── */
    private inner class Shard(
        val id: Int,
        private val thresholdBytes: Long,
        private val onFlush: (MutableList<Record>) -> Unit,
        private val onEvict: ((List<Record>) -> Unit)?,
        private val maxEntries: Int?
    ) {
        private fun newLruMap(): LinkedHashMap<Key, Record> =
            object : LinkedHashMap<Key, Record>(16, 0.75f, true) {
                override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Key, Record>): Boolean {
                    if (maxEntries != null && size > maxEntries) {
                        currentBytes.addAndGet(-sizeOf(eldest.value))
                        onEvict?.invoke(listOf(eldest.value))
                        return true
                    }
                    return false
                }
            }

        private val lock = ReentrantReadWriteLock()
        private val map = newLruMap()
        private val currentBytes = AtomicLong(0)
        private val highestSeqNo = AtomicLong(0)
        private val flushPending = AtomicBoolean(false)

        fun get(key: ByteBufferL): Record? = lock.read { map[wrapKey(key)] }

        fun getAll(): List<Record> = lock.read { map.values.toList() }

        fun put(record: Record): Boolean {
            var shouldFlush = false
            val accepted = lock.write {
                val k = wrapKey(record.key)
                val prev = map[k]
                if (prev != null && record.seqNo < prev.seqNo) return@write false
                map[k] = record
                if (record.seqNo > highestSeqNo.get()) {
                    highestSeqNo.updateAndGet { old -> max(old, record.seqNo) }
                }
                val delta = sizeOf(record) - (prev?.let { sizeOf(it) } ?: 0L)
                val sizeAfter = currentBytes.addAndGet(delta)
                shouldFlush = sizeAfter >= thresholdBytes
                true
            }
            if (shouldFlush && flushPending.compareAndSet(false, true)) {
                executor.execute {
                    try {
                        flushInternal()
                    } finally {
                        flushPending.set(false)
                    }
                }
            }
            return accepted
        }

        fun flush() {
            try {
                flushInternal()
            } finally {
                flushPending.set(false)
            }
        }

        fun evictOldest(n: Int = 1) {
            lock.write {
                val it = map.entries.iterator()
                val evicted = ArrayList<Record>(n)
                var bytesFreed = 0L
                var i = 0
                while (i < n && it.hasNext()) {
                    val e = it.next()
                    evicted += e.value
                    bytesFreed += sizeOf(e.value)
                    it.remove()
                    i++
                }
                if (bytesFreed > 0) currentBytes.addAndGet(-bytesFreed)
                if (evicted.isNotEmpty()) onEvict?.invoke(evicted)
            }
        }

        fun lastSeq(): Long = highestSeqNo.get()

        private fun flushInternal() {
            val toFlush: MutableList<Record>? = lock.write {
                if (map.isEmpty()) return@write null
                val list = ArrayList(map.values)
                val freed = list.sumOf { sizeOf(it) }
                map.clear()
                currentBytes.addAndGet(-freed)
                list
            }
            toFlush?.let {
                try {
                    onFlush(it)
                } catch (e: Exception) {
                    System.err.println("MemTable.Shard#$id onFlush failed: size=${it.size}")
                    e.printStackTrace()
                }
            }
        }

        private fun sizeOf(r: Record): Long =
            (r.key.remaining + r.value.remaining + Long.SIZE_BYTES).toLong()

        private fun wrapKey(key: ByteBufferL): Key {
            val ro = key.asReadOnly()
            val canon = ro.slice(0, ro.remaining)
            return Key(canon, canon.hashOfRemaining())
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
