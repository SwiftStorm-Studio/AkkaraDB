package dev.swiftstorm.akkaradb.format.api

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Record

interface RecordReader {
    fun read(buf: ByteBufferL): Record
}