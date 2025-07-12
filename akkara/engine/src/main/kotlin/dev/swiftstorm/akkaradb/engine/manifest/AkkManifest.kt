package dev.swiftstorm.akkaradb.engine.manifest

import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

/**
 * Append-only manifest that stores the number of stripes durably.
 *
 * Format: Plain UTF-8 decimal followed by '\n'.
 */
class AkkManifest(private val path: Path) {

    @Volatile
    var stripesWritten: Long = 0L
        private set

    /** Load existing value from disk, if any. */
    fun load() {
        if (Files.exists(path)) {
            val txt = Files.readString(path, StandardCharsets.UTF_8).trim()
            stripesWritten = txt.toLongOrNull() ?: 0L
        }
    }

    /**
     * Persist the latest stripe counter using **atomic‐move**.
     */
    @Synchronized
    fun advance(newCount: Long) {
        require(newCount >= stripesWritten) { "stripe counter must be monotonic" }
        stripesWritten = newCount

        val tmp = path.resolveSibling("${path.fileName}.tmp")
        Files.writeString(
            tmp,
            "$stripesWritten\n",
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        )
        FileChannel.open(tmp, StandardOpenOption.WRITE).force(true)

        Files.move(
            tmp,
            path,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING
        )
    }
}
