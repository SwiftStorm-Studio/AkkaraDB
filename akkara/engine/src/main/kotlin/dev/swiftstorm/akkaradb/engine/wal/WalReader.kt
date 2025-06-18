package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.Record
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import kotlin.io.path.listDirectoryEntries

class WalReader(private val dir: Path) : Wal {

    override fun replay(consumer: (Record) -> Unit) {
        dir.listDirectoryEntries("wal_*.wal").sorted().forEach { file ->
            FileChannel.open(file, READ).use { ch ->
                val buf = ByteBuffer.allocateDirect(64 * 1024)
                var read = ch.read(buf)
                while (read > 0) {
                    buf.flip()
                    while (WalCodec.read(buf, consumer) > 0) { /* loop */
                    }
                    buf.compact()
                    read = ch.read(buf)
                }
            }
        }
    }

    override fun append(rec: Record, sync: Boolean) = error("read-only")

    override fun close() = Unit
}