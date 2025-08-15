package dev.swiftstorm.akkaradb.engine.cache

import dev.swiftstorm.akkaradb.engine.cache.HotReadCache.Entry
import java.nio.ByteBuffer

/**
 * Read-only hot cache for GET fast path.
 * - Keeps key/value copies (heap) so lifetime is independent.
 * - Size limited by total bytes; eviction is LRU.
 */
class HotReadCache(private val maxBytes: Long) {
    private data class Entry(val key: ByteBuffer, val value: ByteBuffer, val size: Int)

    private val map = object : LinkedHashMap<ByteBuffer, Entry>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<ByteBuffer, Entry>): Boolean {
            return currentBytes > maxBytes
        }
    }

    @Volatile
    private var currentBytes: Long = 0

    @Synchronized
    fun get(key: ByteBuffer): ByteBuffer? {
        val e = map[key] ?: return null
        // hand out a fresh duplicate
        return e.value.duplicate().apply { rewind() }
    }

    @Synchronized
    fun put(key: ByteBuffer, value: ByteBuffer) {
        val k = key.readOnlyHeapCopy()
        val v = value.readOnlyHeapCopy()
        val size = k.remaining() + v.remaining()
        val old = map.put(k, Entry(k, v, size))
        if (old != null) currentBytes -= old.size
        currentBytes += size
        // LRU eviction happens via removeEldestEntry
    }
}

private fun ByteBuffer.readOnlyHeapCopy(): ByteBuffer {
    val d = this.duplicate().apply { rewind() }
    val out = ByteBuffer.allocate(d.remaining())
    out.put(d).flip()
    return out.asReadOnlyBuffer()
}
