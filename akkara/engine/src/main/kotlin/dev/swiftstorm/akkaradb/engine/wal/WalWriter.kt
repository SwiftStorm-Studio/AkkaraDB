package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import kotlin.io.path.fileSize

class WalWriter(
    private var path: Path,
    private val segmentBytes: Long = 16L * 1024 * 1024,   // 16 MiB
    private val fsyncEvery: Int = 1                       // n レコード毎に fsync
) : Wal {

    private var ch: FileChannel = open(path)
    private var buf: ByteBuffer = Pools.io().get()
    private var pending = 0
    private var writtenBytes = path.fileSize()

    override fun append(rec: Record, sync: Boolean) {
        WalCodec.write(rec, buf)
        pending++
        if (!buf.hasRemaining()) flush()

        if (sync || pending >= fsyncEvery) flush()
        if (writtenBytes >= segmentBytes) rotate()
    }

    private fun flush() {
        buf.flip()
        while (buf.hasRemaining()) ch.write(buf)
        ch.force(true)
        writtenBytes += buf.limit()
        buf.clear()
        pending = 0
    }

    private fun rotate() {
        ch.close()
        val next = path.resolveSibling(
            "wal_%04d.wal".format(
                path.fileName.toString()
                    .substringAfter("wal_")
                    .substringBefore(".wal")
                    .toInt() + 1
            )
        )
        path = next
        ch = open(next)
        writtenBytes = 0
    }

    override fun replay(consumer: (Record) -> Unit) = Unit

    override fun close() {
        flush(); ch.close()
    }

    private fun open(p: Path) = FileChannel.open(p, CREATE, WRITE, APPEND, DSYNC)
}