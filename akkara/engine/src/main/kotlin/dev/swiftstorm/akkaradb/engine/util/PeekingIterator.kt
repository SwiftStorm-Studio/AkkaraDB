package dev.swiftstorm.akkaradb.engine.util

class PeekingIterator<T>(private val it: Iterator<T>) : Iterator<T> {
    private var hasPeeked = false
    private var peeked: T? = null

    override fun hasNext(): Boolean =
        hasPeeked || it.hasNext()

    override fun next(): T {
        if (!hasNext()) throw NoSuchElementException()
        if (hasPeeked) {
            hasPeeked = false
            val r = peeked
            peeked = null
            return r as T
        }
        return it.next()
    }

    fun peek(): T {
        if (!hasPeeked) {
            if (!it.hasNext()) throw NoSuchElementException()
            peeked = it.next()
            hasPeeked = true
        }
        @Suppress("UNCHECKED_CAST")
        return peeked as T
    }

    fun dropPeeked() {
        hasPeeked = false
        peeked = null
    }
}
