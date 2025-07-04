package dev.swiftstorm.akkaradb.format.api

import dev.swiftstorm.akkaradb.common.Record
import java.nio.ByteBuffer

interface RecordReader {
    fun read(buf: ByteBuffer): Record
}