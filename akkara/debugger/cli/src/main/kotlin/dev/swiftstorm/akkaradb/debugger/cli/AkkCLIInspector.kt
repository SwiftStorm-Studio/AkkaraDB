package dev.swiftstorm.akkaradb.debugger.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.path
import com.github.ajalt.mordant.rendering.TextColors.*
import com.github.ajalt.mordant.rendering.TextStyles.bold
import com.github.ajalt.mordant.table.table
import com.github.ajalt.mordant.terminal.Terminal
import dev.swiftstorm.akkaradb.debugger.core.SSTReport
import dev.swiftstorm.akkaradb.debugger.core.WALReport
import dev.swiftstorm.akkaradb.debugger.core.inspectors.SSTInspector
import dev.swiftstorm.akkaradb.debugger.core.inspectors.StripeInspector
import dev.swiftstorm.akkaradb.debugger.core.inspectors.WALInspector
import java.nio.ByteBuffer

class AkkaraInspect : CliktCommand(name = "akkara-inspect") {
    override fun help(context: Context): String = "AkkaraDB internal format inspector"
    override fun run() = Unit
}

class InspectSST : CliktCommand(name = "sst") {
    private val file by argument(help = "SSTable file path").path(mustExist = true)
    private val verbose by option("-v", "--verbose", help = "Show detailed record info").flag()
    private val decode by option("-d", "--decode", help = "Decode keys/values as UTF-8").flag()
    private val binpack by option("-b", "--binpack", help = "Decode values as BinPack").flag()
    private val json by option("--json", help = "Output as JSON").flag()
    override fun help(context: Context): String = "Inspect SSTable file"

    override fun run() {
        val term = Terminal()
        val inspector = SSTInspector(file)
        val report = inspector.inspect(includeRecordData = decode || binpack)

        if (json) {
            printJSON(report)
            return
        }

        term.println(bold("SSTable: ${report.filePath}"))
        term.println("File size: ${report.fileSize} bytes")
        term.println()

        // Footer
        term.println(bold("Footer:"))
        term.println("  Magic: ${report.footer.magic}")
        term.println("  Version: ${report.footer.version}")
        term.println("  Index offset: ${report.footer.indexOffset}")
        term.println("  Bloom offset: ${report.footer.bloomOffset}")
        term.println("  Entry count: ${report.footer.entryCount}")
        term.println("  CRC32C: 0x${report.footer.crc32c.toString(16)}")
        term.println()

        // Index
        term.println(bold("Index:"))
        term.println("  Entry count: ${report.index.entryCount}")
        term.println("  Key size: ${report.index.keySize} bytes")
        if (verbose && report.index.entries.isNotEmpty()) {
            term.println(table {
                header {
                    row("First Key (32B)", "Block Offset")
                }
                body {
                    report.index.entries.take(10).forEach { entry ->
                        row(entry.firstKey32.take(16) + "...", entry.blockOffset.toString())
                    }
                    if (report.index.entries.size > 10) {
                        row("... (${report.index.entries.size - 10} more)", "...")
                    }
                }
            })
        }
        term.println()

        // Bloom
        report.bloom?.let { bloom ->
            term.println(bold("Bloom Filter:"))
            term.println("  Byte size: ${bloom.byteSize}")
            term.println("  Hash count: ${bloom.hashCount}")
            term.println("  Expected insertions: ${bloom.expectedInsertions}")
            term.println()
        }

        // Blocks
        term.println(bold("Data Blocks: ${report.blocks.size}"))
        val validBlocks = report.blocks.count { it.crcValid }
        val invalidBlocks = report.blocks.size - validBlocks

        term.println("  Valid: ${green(validBlocks.toString())}")
        if (invalidBlocks > 0) {
            term.println("  Invalid: ${red(invalidBlocks.toString())}")
        }
        term.println()

        if (verbose) {
            report.blocks.take(5).forEach { block ->
                term.println("Block @ offset ${block.offset}:")
                term.println("  Payload: ${block.payloadLen} bytes")
                term.println("  Records: ${block.recordCount}")
                term.println(
                    "  CRC: ${if (block.crcValid) green("OK") else red("FAIL")} " +
                            "(stored=0x${block.crcStored.toString(16)}, computed=0x${block.crcComputed.toString(16)})"
                )

                if (block.records.isNotEmpty()) {
                    block.records.take(3).forEach { rec ->
                        term.println()
                        term.println("  Record #${rec.seq}:")
                        term.println("    Key (${rec.keyLen} bytes): ${rec.keyPreview.take(32)}${if (rec.keyPreview.length > 32) "..." else ""}")

                        if (decode && rec.keyData != null) {
                            val keyDecoded = decodeBytes(rec.keyData!!)
                            term.println("    Key (UTF-8): ${cyan(keyDecoded)}")
                        }

                        if (binpack && rec.keyData != null) {
                            val keyBinPack = decodeBinPack(rec.keyData!!)
                            term.println("    Key (BinPack): ${magenta(keyBinPack)}")
                        }

                        if (rec.isTombstone) {
                            term.println("    Value: ${red("TOMBSTONE")}")
                        } else {
                            term.println("    Value (${rec.valueLen} bytes): ${rec.valuePreview?.take(32) ?: "null"}${if ((rec.valuePreview?.length ?: 0) > 32) "..." else ""}")

                            if (decode && rec.valueData != null) {
                                val valueDecoded = decodeBytes(rec.valueData!!)
                                term.println("    Value (UTF-8): ${cyan(valueDecoded)}")
                            }

                            if (binpack && rec.valueData != null) {
                                val valueBinPack = decodeBinPack(rec.valueData!!)
                                term.println("    Value (BinPack): ${magenta(valueBinPack)}")
                            }
                        }
                    }
                }
                term.println()
            }
        }

        // Errors
        if (report.errors.isNotEmpty()) {
            term.println(red(bold("Errors:")))
            report.errors.forEach { term.println(red("  • $it")) }
        }
    }

    private fun decodeBytes(bytes: ByteArray): String {
        return try {
            String(bytes, Charsets.UTF_8)
                .replace(Regex("[\\p{C}&&[^\\n\\t\\r]]"), "�") // 制御文字を置換（改行/タブ/CRは残す）
        } catch (e: Exception) {
            "(decode error: ${e.message})"
        }
    }

    private fun decodeBinPack(bytes: ByteArray): String {
        return try {
            val buf = ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN)
            val result = StringBuilder()

            result.append("{ ")

            var fieldCount = 0
            while (buf.hasRemaining()) {
                if (buf.remaining() < 5) break

                when (val typeOrLen = buf.get().toInt() and 0xFF) {
                    in 1..100 -> {
                        if (buf.remaining() >= typeOrLen) {
                            val strBytes = ByteArray(typeOrLen)
                            buf.get(strBytes)
                            val str = String(strBytes, Charsets.UTF_8)
                            if (str.all { it.isLetterOrDigit() || it in " \"':,.-_" }) {
                                if (fieldCount > 0) result.append(", ")
                                result.append("\"$str\"")
                                fieldCount++
                            }
                        }
                    }

                    0 if buf.remaining() >= 4 -> {
                        val num = buf.getInt()
                        if (fieldCount > 0) result.append(", ")
                        result.append(num)
                        fieldCount++
                    }
                }

                if (fieldCount >= 10) {
                    result.append(", ...")
                    break
                }
            }

            result.append(" }")

            if (fieldCount == 0) {
                hexDump(bytes, 32)
            } else {
                result.toString()
            }
        } catch (e: Exception) {
            "(BinPack decode error: ${e.message})"
        }
    }

    private fun hexDump(bytes: ByteArray, maxBytes: Int = 64): String {
        val preview = bytes.take(maxBytes)
        val hex = preview.joinToString(" ") { "%02x".format(it) }
        val suffix = if (bytes.size > maxBytes) " ... (${bytes.size} bytes total)" else ""
        return hex + suffix
    }

    private fun printJSON(report: SSTReport) {
        println(
            """
{
  "filePath": "${report.filePath}",
  "fileSize": ${report.fileSize},
  "footer": {
    "magic": "${report.footer.magic}",
    "version": ${report.footer.version},
    "indexOffset": ${report.footer.indexOffset},
    "bloomOffset": ${report.footer.bloomOffset},
    "entryCount": ${report.footer.entryCount}
  },
  "blocks": ${report.blocks.size},
  "errors": [${report.errors.joinToString { "\"$it\"" }}]
}
        """.trimIndent()
        )
    }
}

class InspectWAL : CliktCommand(name = "wal") {
    private val file by argument(help = "WAL file path").path(mustExist = true)
    private val limit by option("-n", "--limit", help = "Max entries to show").int().default(100)
    private val json by option("--json", help = "Output as JSON").flag()

    override fun help(context: Context): String = "Inspect WAL file"

    override fun run() {
        val term = Terminal()
        val inspector = WALInspector(file)
        val report = inspector.inspect()

        if (json) {
            printJSON(report)
            return
        }

        term.println(bold("WAL: ${report.filePath}"))
        term.println("File size: ${report.fileSize} bytes")
        term.println("Total entries: ${report.entries.size}")
        if (report.truncatedTail) {
            term.println(yellow("⚠ Truncated tail detected"))
        }
        term.println()

        term.println(table {
            header {
                row("LSN", "Seq", "Op", "Key Len", "Val Len", "Key Preview")
            }
            body {
                report.entries.take(limit).forEach { entry ->
                    row(
                        entry.lsn?.toString() ?: "?",
                        entry.seq.toString(),
                        if (entry.operation == "DELETE") red("DEL") else green("ADD"),
                        entry.keyLen.toString(),
                        entry.valueLen.toString(),
                        entry.keyPreview.take(16) + "..."
                    )
                }
                if (report.entries.size > limit) {
                    row("...", "...", "...", "...", "...", "(${report.entries.size - limit} more)")
                }
            }
        })

        if (report.errors.isNotEmpty()) {
            term.println()
            term.println(red(bold("Errors:")))
            report.errors.forEach { term.println(red("  • $it")) }
        }
    }

    private fun printJSON(report: WALReport) {
        println(
            """
{
  "filePath": "${report.filePath}",
  "fileSize": ${report.fileSize},
  "entryCount": ${report.entries.size},
  "truncatedTail": ${report.truncatedTail},
  "errors": [${report.errors.joinToString { "\"$it\"" }}]
}
        """.trimIndent()
        )
    }
}

class InspectStripe : CliktCommand(name = "stripe") {
    private val dir by argument(help = "Stripe directory path").path(mustExist = true)
    private val k by option("-k", help = "Number of data lanes").int().default(4)
    private val m by option("-m", help = "Number of parity lanes").int().default(2)

    override fun help(context: Context): String = "Inspect Stripe directory"

    override fun run() {
        val term = Terminal()
        val inspector = StripeInspector(dir)
        val report = inspector.inspect(k, m)

        term.println(bold("Stripe Directory: ${report.directory}"))
        term.println("Config: k=${report.config.k}, m=${report.config.m}, block=${report.config.blockSize}")
        term.println()

        term.println(bold("Lanes: ${report.lanes.size}"))
        term.println(table {
            header {
                row("Index", "Type", "File Size", "Path")
            }
            body {
                report.lanes.forEach { lane ->
                    row(
                        lane.index.toString(),
                        if (lane.type == "DATA") cyan("DATA") else magenta("PARITY"),
                        lane.fileSize.toString(),
                        lane.filePath
                    )
                }
            }
        })
        term.println()

        term.println(bold("Stripes: ${report.stripes.size}"))
        val reconstructible = report.stripes.count { it.reconstructible }
        term.println("  Reconstructible: ${green(reconstructible.toString())} / ${report.stripes.size}")

        if (report.errors.isNotEmpty()) {
            term.println()
            term.println(red(bold("Errors:")))
            report.errors.forEach { term.println(red("  • $it")) }
        }
    }
}

fun main(args: Array<String>) = AkkaraInspect()
    .subcommands(InspectSST(), InspectWAL(), InspectStripe())
    .main(args)