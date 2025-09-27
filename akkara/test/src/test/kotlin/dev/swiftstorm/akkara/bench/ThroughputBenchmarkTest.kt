package dev.swiftstorm.akkara.bench

import dev.swiftstorm.akkaradb.common.ShortUUID
import dev.swiftstorm.akkaradb.engine.AkkDSL
import dev.swiftstorm.akkaradb.engine.PackedTable
import dev.swiftstorm.akkaradb.engine.StartupMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.text.DecimalFormat
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

class ThroughputBenchmarkTest {

    @Test
    fun `AkkaraDB DSL throughput benchmark prints aggregated metrics`() {
        val availableProcessors = Runtime.getRuntime().availableProcessors()
        val threadCount = availableProcessors.coerceIn(1, 8)

        val config = BenchmarkConfig(
            warmupSeconds = 1,
            measurementSeconds = 2,
            threadCount = threadCount,
            readRatioPercent = 60,
            keysPerThread = 512,
            payloadBytes = 256
        )

        val result = runDslThroughputBenchmark(config)
        println(result.toPrettyString())

        assertTrue(result.operations > 0, "Benchmark recorded no operations")
        assertEquals(result.operations, result.readOperations + result.writeOperations)
        assertTrue(result.throughputOpsPerSecond > 0.0, "Throughput must be positive")
    }

    private fun runDslThroughputBenchmark(config: BenchmarkConfig): BenchmarkResult {
        require(config.threadCount > 0) { "threadCount must be positive" }
        require(config.measurementSeconds > 0) { "measurementSeconds must be positive" }

        val baseDir = Path.of("akkara-dsl-bench").also { Files.createDirectories(it) }

        return AkkDSL.open<BenchAccount>(baseDir, StartupMode.ULTRA_FAST).use { table ->
            val keys = seedDataset(table, config)
            val result = measureThroughput(table, config, keys)
            table.db.flush()
            result
        }

//        return try {
//            AkkDSL.open<BenchAccount>(baseDir, StartupMode.FAST) {
//                metaCacheCap = 4_096
//                stripe {
//                    k = 4
//                    m = 1
//                    autoFlush = false
//                    flushThreshold = 512L * 1024 * 1024
//                }
//                wal {
//                    disableFsync()
//                    queueCap = 262_144
//                    backoffNanos = 100_000
//                }
//            }.use { table ->
//                val keys = seedDataset(table, config)
//                val result = measureThroughput(table, config, keys)
//                table.db.flush()
//                result
//            }
//        } finally {
//            deleteDirectoryRecursively(baseDir)
//        }
    }

    private fun measureThroughput(
        table: PackedTable<BenchAccount>,
        config: BenchmarkConfig,
        keys: Array<ShortUUID>
    ): BenchmarkResult {
        require(keys.isNotEmpty()) { "Benchmark key space must not be empty" }
        val operations = LongAdder()
        val readOperations = LongAdder()
        val writeOperations = LongAdder()

        val executor = Executors.newFixedThreadPool(config.threadCount)
        val tasks = ArrayList<Future<*>>(config.threadCount)
        val warmupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.warmupSeconds)
        val measurementDurationNanos = TimeUnit.SECONDS.toNanos(config.measurementSeconds)

        try {
            repeat(config.threadCount) { workerIndex ->
                tasks += executor.submit {
                    val rng = ThreadLocalRandom.current()
                    while (System.nanoTime() < warmupDeadline) {
                        val uuid = keys[rng.nextInt(keys.size)]
                        val performRead = rng.nextInt(100) < config.readRatioPercent
                        if (performRead) {
                            table.get("tenant", uuid)
                        } else {
                            performWrite(table, uuid, workerIndex, rng, config.payloadBytes)
                        }
                    }

                    val measurementDeadline = System.nanoTime() + measurementDurationNanos
                    while (System.nanoTime() < measurementDeadline) {
                        val uuid = keys[rng.nextInt(keys.size)]
                        val performRead = rng.nextInt(100) < config.readRatioPercent
                        if (performRead) {
                            table.get("tenant", uuid)
                            readOperations.increment()
                        } else {
                            performWrite(table, uuid, workerIndex, rng, config.payloadBytes)
                            writeOperations.increment()
                        }
                        operations.increment()
                    }
                }
            }
        } finally {
            executor.shutdown()
            try {
                if (!executor.awaitTermination(config.measurementSeconds + config.warmupSeconds + 5, TimeUnit.SECONDS)) {
                    executor.shutdownNow()
                }
                for (future in tasks) {
                    future.get()
                }
            } catch (ie: InterruptedException) {
                executor.shutdownNow()
                Thread.currentThread().interrupt()
            } catch (e: Exception) {
                throw RuntimeException("Benchmark worker failed", e)
            }
        }

        val ops = operations.sum()
        val reads = readOperations.sum()
        val writes = writeOperations.sum()

        return BenchmarkResult(
            config = config,
            operations = ops,
            readOperations = reads,
            writeOperations = writes,
            measurementDurationSeconds = config.measurementSeconds.toDouble()
        )
    }

    private fun seedDataset(
        table: PackedTable<BenchAccount>,
        config: BenchmarkConfig
    ): Array<ShortUUID> {
        val keys = ArrayList<ShortUUID>(config.totalKeySpace)
        val rng = ThreadLocalRandom.current()

        repeat(config.totalKeySpace) {
            val uuid = ShortUUID.generate()
            val payload = ByteArray(config.payloadBytes).also(rng::nextBytes)
            val account = BenchAccount(
                accountId = "acct-${uuid.toShortString()}",
                balance = rng.nextLong(50_000L, 250_000L),
                version = 1L,
                payload = payload,
                tags = listOf(
                    "segment-${keys.size % 32}",
                    "region-${keys.size % 8}",
                    "tier-${keys.size % 5}"
                )
            )
            table.put("tenant", uuid, account)
            keys += uuid
        }

        return keys.toTypedArray()
    }

    private fun performWrite(
        table: PackedTable<BenchAccount>,
        uuid: ShortUUID,
        workerIndex: Int,
        rng: ThreadLocalRandom,
        payloadBytes: Int
    ) {
        val delta = rng.nextLong(1L, 16L)
        val updated = table.update("tenant", uuid) {
            balance += delta
            version += 1
            payload = mutatePayload(payload, rng)
            tags = listOf(
                "segment-${version and 15}",
                "worker-$workerIndex",
                "tier-${(balance / 10_000L) % 8}"
            )
        }

        if (!updated) {
            val payload = ByteArray(payloadBytes).also(rng::nextBytes)
            table.put(
                "tenant",
                uuid,
                BenchAccount(
                    accountId = "acct-reseed-${uuid.toShortString()}",
                    balance = delta,
                    version = 1L,
                    payload = payload,
                    tags = listOf("segment-reseed", "worker-$workerIndex", "tier-reset")
                )
            )
        }
    }

    private fun mutatePayload(source: ByteArray, rng: ThreadLocalRandom): ByteArray {
        if (source.isEmpty()) return source
        val copy = source.copyOf()
        val index = rng.nextInt(copy.size)
        val delta = rng.nextInt(1, 128)
        copy[index] = (copy[index].toInt() xor delta).toByte()
        return copy
    }

    private fun deleteDirectoryRecursively(path: Path) {
        if (!Files.exists(path)) return
        Files.walk(path).use { stream ->
            stream.sorted(Comparator.reverseOrder())
                .forEach { Files.deleteIfExists(it) }
        }
    }

    private data class BenchmarkConfig(
        val warmupSeconds: Long,
        val measurementSeconds: Long,
        val threadCount: Int,
        val readRatioPercent: Int,
        val keysPerThread: Int,
        val payloadBytes: Int
    ) {
        init {
            require(warmupSeconds >= 0) { "warmupSeconds cannot be negative" }
            require(measurementSeconds > 0) { "measurementSeconds must be positive" }
            require(threadCount > 0) { "threadCount must be positive" }
            require(readRatioPercent in 0..100) { "readRatioPercent must be between 0 and 100" }
            require(keysPerThread > 0) { "keysPerThread must be positive" }
            require(payloadBytes > 0) { "payloadBytes must be positive" }
        }

        val totalKeySpace: Int = threadCount * keysPerThread
    }

    private data class BenchmarkResult(
        val config: BenchmarkConfig,
        val operations: Long,
        val readOperations: Long,
        val writeOperations: Long,
        val measurementDurationSeconds: Double
    ) {
        val throughputOpsPerSecond: Double = if (measurementDurationSeconds == 0.0) 0.0 else operations / measurementDurationSeconds
        val throughputPerThread: Double = if (config.threadCount == 0) 0.0 else throughputOpsPerSecond / config.threadCount
        val readSharePercent: Double = if (operations == 0L) 0.0 else (readOperations.toDouble() / operations) * 100.0
        val writeSharePercent: Double = if (operations == 0L) 0.0 else (writeOperations.toDouble() / operations) * 100.0

        fun toPrettyString(): String {
            val integerFormat = DecimalFormat("#,##0")
            val decimalFormat = DecimalFormat("#,##0.00")
            return buildString {
                appendLine("AkkaraDB DSL throughput benchmark")
                appendLine(" Startup mode         : FAST (custom WAL queue)")
                appendLine(" Threads              : ${config.threadCount}")
                appendLine(" Warm-up duration (s) : ${integerFormat.format(config.warmupSeconds)}")
                appendLine(" Measurement (s)      : ${integerFormat.format(config.measurementSeconds)}")
                appendLine(" Key space (records)  : ${integerFormat.format(config.totalKeySpace)}")
                appendLine(" Payload bytes        : ${integerFormat.format(config.payloadBytes)}")
                appendLine(" Target read ratio (%) : ${integerFormat.format(config.readRatioPercent.toLong())}")
                appendLine(" Total operations     : ${integerFormat.format(operations)}")
                appendLine(" Ops/sec              : ${decimalFormat.format(throughputOpsPerSecond)}")
                appendLine(" Ops/sec/thread       : ${decimalFormat.format(throughputPerThread)}")
                appendLine(" Reads                : ${integerFormat.format(readOperations)} (${decimalFormat.format(readSharePercent)}%)")
                appendLine(" Writes               : ${integerFormat.format(writeOperations)} (${decimalFormat.format(writeSharePercent)}%)")
            }
        }
    }

    data class BenchAccount(
        var accountId: String = "",
        var balance: Long = 0,
        var version: Long = 0,
        var payload: ByteArray = ByteArray(0),
        var tags: List<String> = emptyList()
    )
}
