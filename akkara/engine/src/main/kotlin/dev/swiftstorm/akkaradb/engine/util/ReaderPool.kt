package dev.swiftstorm.akkaradb.engine.util

import java.util.concurrent.ArrayBlockingQueue

class ReaderPool<T : AutoCloseable>(
    val factory: () -> T,
    max: Int
) : AutoCloseable {
    val q = ArrayBlockingQueue<T>(max)

    inline fun <R> withResource(block: (T) -> R): R {
        val res = q.poll() ?: factory()
        return try {
            block(res)
        } finally {
            if (!q.offer(res)) runCatching { res.close() }
        }
    }

    override fun close() {
        while (true) q.poll()?.let { runCatching { it.close() } } ?: break
    }
}
