package dev.swiftstorm.akkaradb.debugger.core.inspectors

import dev.swiftstorm.akkaradb.debugger.core.LaneInfo
import dev.swiftstorm.akkaradb.debugger.core.StripeConfig
import dev.swiftstorm.akkaradb.debugger.core.StripeInfo
import dev.swiftstorm.akkaradb.debugger.core.StripeReport
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.fileSize
import kotlin.io.path.isRegularFile
import kotlin.io.path.name
import kotlin.io.path.pathString

class StripeInspector(private val directory: Path) {

    fun inspect(k: Int = 4, m: Int = 2, blockSize: Int = 32768): StripeReport {
        val errors = mutableListOf<String>()

        if (!Files.isDirectory(directory)) {
            return StripeReport(
                directory = directory.pathString,
                config = StripeConfig(k, m, blockSize),
                lanes = emptyList(),
                stripes = emptyList(),
                errors = listOf("Not a directory: ${directory.pathString}")
            )
        }

        // Find lane files
        val laneFiles = Files.list(directory).use { stream ->
            stream.filter { it.isRegularFile() && it.name.startsWith("lane-") }
                .toList()
                .sortedBy { it.name }
        }

        if (laneFiles.isEmpty()) {
            return StripeReport(
                directory = directory.pathString,
                config = StripeConfig(k, m, blockSize),
                lanes = emptyList(),
                stripes = emptyList(),
                errors = listOf("No lane files found")
            )
        }

        val lanes = laneFiles.mapIndexed { idx, path ->
            LaneInfo(
                index = idx,
                type = if (idx < k) "DATA" else "PARITY",
                filePath = path.pathString,
                fileSize = path.fileSize()
            )
        }

        // Validate stripe structure
        val minSize = lanes.minOfOrNull { it.fileSize } ?: 0
        val maxSize = lanes.maxOfOrNull { it.fileSize } ?: 0

        if (maxSize - minSize > blockSize) {
            errors.add("Lane size mismatch: min=$minSize, max=$maxSize")
        }

        val stripeCount = (minSize / blockSize).toInt()
        val stripes = (0 until stripeCount).map { idx ->
            StripeInfo(
                stripeIndex = idx.toLong(),
                blockCount = k,
                reconstructible = lanes.size >= k,
                parityValid = null // Would need actual parity validation
            )
        }

        return StripeReport(
            directory = directory.pathString,
            config = StripeConfig(k, m, blockSize),
            lanes = lanes,
            stripes = stripes,
            errors = errors
        )
    }
}