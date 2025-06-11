package dev.swiftstorm.akkaradb.format.akk.manifest

import java.nio.file.Files
import java.nio.file.Path
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption

object ManifestIO {
    private const val FILE_NAME = "manifest.json"

    fun load(dir: Path): Manifest =
        runCatching {
            val txt = Files.readString(dir.resolve(FILE_NAME), StandardCharsets.UTF_8)
            val blocks = txt.trim().toLong()
            Manifest(blocks)
        }.getOrElse { Manifest() }

    fun store(dir: Path, manifest: Manifest) {
        Files.writeString(
            dir.resolve(FILE_NAME),
            "${manifest.blocksWritten}\n",
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE,
            StandardOpenOption.SYNC
        )
    }
}
