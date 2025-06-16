package dev.swiftstorm.akkaradb.engine.memtable

import dev.swiftstorm.akkaradb.common.Record
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong

/**
 * In‑memory ordered map that flushes once the estimated size exceeds [thresholdBytes].
 */
class MemTable(
    private val thresholdBytes: Long,
    private val onFlush: (List<Record>) -> Unit
) {
    private val map = ConcurrentSkipListMap<ByteBuffer, Record>(ByteBufferLexicographicComparator)
    private val currentBytes = AtomicLong(0)

    fun put(record: Record) {
        val prev = map.put(record.key, record)
        currentBytes.addAndGet(sizeOf(record) - (prev?.let(::sizeOf) ?: 0))
        if (currentBytes.get() >= thresholdBytes) flush()
    }

    fun get(key: ByteBuffer): Record? = map[key]

    fun flush() {
        if (map.isEmpty()) return
        val snapshot = map.values.toList()
        onFlush(snapshot)
        map.clear()
        currentBytes.set(0)
    }

    private fun sizeOf(r: Record): Long =
        (r.key.remaining() + r.value.remaining() + 16).toLong() // rough estimate
}

/* helpers */
object ByteBufferLexicographicComparator : Comparator<ByteBuffer> {
    override fun compare(a: ByteBuffer, b: ByteBuffer): Int {
        val minLen = minOf(a.remaining(), b.remaining())
        for (i in 0 until minLen) {
            val diff = (a.get(a.position() + i).toInt() and 0xFF) -
                    (b.get(b.position() + i).toInt() and 0xFF)
            if (diff != 0) return diff
        }
        return a.remaining() - b.remaining()
    }
}