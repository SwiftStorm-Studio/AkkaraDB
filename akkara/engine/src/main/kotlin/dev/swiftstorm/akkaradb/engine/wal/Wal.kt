package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.Record
import java.io.Closeable

interface Wal : Closeable {
    fun append(rec: Record, sync: Boolean = true)
    fun replay(consumer: (Record) -> Unit)
}