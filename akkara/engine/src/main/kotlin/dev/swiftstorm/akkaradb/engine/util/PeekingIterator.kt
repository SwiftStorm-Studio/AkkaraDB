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
