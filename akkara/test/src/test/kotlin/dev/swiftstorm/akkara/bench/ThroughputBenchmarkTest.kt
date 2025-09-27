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
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder
import kotlin.math.max

class ThroughputBenchmarkTest {

    @Test
    fun `AkkaraDB DSL throughput benchmark prints aggregated metrics with op-count stop`() {
        val availableProcessors = Runtime.getRuntime().availableProcessors()
        val threadCount = availableProcessors.coerceIn(1, 8)

        val config = BenchmarkConfig(
            threadCount = threadCount,
            keysPerThread = 512,
            payloadBytes = 256,
            targetReadRatioPercent = 60,
            warmupOpsTotal = 50_000,       // ← warmup は件数で指定
            measurementOpsTotal = 300_000  // ← 測定も件数で指定
        )

        val result = runDslThroughputBenchmark(config)
        println(result.toPrettyString())

        assertTrue(result.operations > 0, "Benchmark recorded no operations")
        assertEquals(result.operations, result.readOperations + result.writeOperations)
        assertEquals(config.measurementOpsTotal.toLong(), result.operations, "Measured ops must match configured total")
        assertTrue(result.throughputOpsPerSecond > 0.0, "Throughput must be positive")
    }

    private fun runDslThroughputBenchmark(config: BenchmarkConfig): BenchmarkResult {
        require(config.threadCount > 0) { "threadCount must be positive" }
        require(config.measurementOpsTotal > 0) { "measurementOpsTotal must be positive" }

        val baseDir = Path.of("akkara-dsl-bench").also { Files.createDirectories(it) }

<<<<<<< Updated upstream
        return AkkDSL.open<BenchAccount>(baseDir, StartupMode.ULTRA_FAST).use { table ->
=======
        // 片付けを入れたいなら finally で deleteDirectoryRecursively(baseDir) を呼ぶ
        return AkkDSL.open<BenchAccount>(baseDir, StartupMode.FAST).use { table ->
>>>>>>> Stashed changes
            val keys = seedDataset(table, config)
            // warmup（件数指定・計測しない）
            runWorkersWithTargets(
                table = table,
                keys = keys,
                threadCount = config.threadCount,
                payloadBytes = config.payloadBytes,
                totalOps = config.warmupOpsTotal,
                targetReadRatioPercent = config.targetReadRatioPercent,
                recordMetrics = false
            )

            // 測定（件数指定・経過時間を測る）
            val (ops, reads, writes, elapsedNanos) = runWorkersWithTargets(
                table = table,
                keys = keys,
                threadCount = config.threadCount,
                payloadBytes = config.payloadBytes,
                totalOps = config.measurementOpsTotal,
                targetReadRatioPercent = config.targetReadRatioPercent,
                recordMetrics = true
            )

            //table.db.close()

            BenchmarkResult(
                config = config,
                operations = ops,
                readOperations = reads,
                writeOperations = writes,
                measurementDurationSeconds = elapsedNanos / 1_000_000_000.0
            )
        }
    }

    /**
     * 全体の目標件数 totalOps を read/write 比率で分割し、
     * 各スレッドがアトミック残量を取り合う形で消費する。
     * 戻り値: 測定モード時のみ (ops, reads, writes, elapsedNanos) を返す。非測定時は 0 を返す。
     */
    private fun runWorkersWithTargets(
        table: PackedTable<BenchAccount>,
        keys: Array<ShortUUID>,
        threadCount: Int,
        payloadBytes: Int,
        totalOps: Long,
        targetReadRatioPercent: Int,
        recordMetrics: Boolean
    ): QuadMetrics {
        require(keys.isNotEmpty()) { "Benchmark key space must not be empty" }
        val ratio = targetReadRatioPercent.coerceIn(0, 100)
        val targetReads = (totalOps * ratio) / 100
        val targetWrites = totalOps - targetReads

        val readRemaining = AtomicLong(targetReads)
        val writeRemaining = AtomicLong(targetWrites)

        val ops = if (recordMetrics) LongAdder() else null
        val reads = if (recordMetrics) LongAdder() else null
        val writes = if (recordMetrics) LongAdder() else null

        val executor = Executors.newFixedThreadPool(threadCount)
        val tasks = ArrayList<Future<*>>(threadCount)

        val start = if (recordMetrics) System.nanoTime() else 0L

        try {
            repeat(threadCount) { workerIndex ->
                tasks += executor.submit {
                    val rng = ThreadLocalRandom.current()
                    while (true) {
                        val rRem = readRemaining.get()
                        val wRem = writeRemaining.get()
                        if (rRem <= 0 && wRem <= 0) break

                        val doRead = decideNextOp(rng, rRem, wRem)
                        if (doRead) {
                            if (readRemaining.get() > 0 && readRemaining.decrementAndGet() >= 0) {
                                val uuid = keys[rng.nextInt(keys.size)]
                                table.get("tenant", uuid)
                                reads?.increment()
                                ops?.increment()
                            }
                        } else {
                            if (writeRemaining.get() > 0 && writeRemaining.decrementAndGet() >= 0) {
                                val uuid = keys[rng.nextInt(keys.size)]
                                performWrite(table, uuid, workerIndex, rng, payloadBytes)
                                writes?.increment()
                                ops?.increment()
                            }
                        }
                    }
                }
            }
        } finally {
            executor.shutdown()
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow()
                }
                // 例外を表面化
                for (f in tasks) f.get()
            } catch (ie: InterruptedException) {
                executor.shutdownNow()
                Thread.currentThread().interrupt()
            }
        }

        val elapsed = if (recordMetrics) (System.nanoTime() - start) else 0L
        return QuadMetrics(
            operations = ops?.sum() ?: 0L,
            reads = reads?.sum() ?: 0L,
            writes = writes?.sum() ?: 0L,
            elapsedNanos = elapsed
        )
    }

    /**
     * 残量に応じて次のオペレーションを選ぶ。
     * 片方 0 → もう片方。両方>0 → 残量比に比例した確率で選択（目標比率に近づける）。
     */
    private fun decideNextOp(rng: ThreadLocalRandom, readRem: Long, writeRem: Long): Boolean {
        return when {
            readRem <= 0 && writeRem > 0 -> false
            writeRem <= 0 && readRem > 0 -> true
            readRem <= 0 && writeRem <= 0 -> true // どちらでもよい（呼び出し側で break される）
            else -> {
                val sum = readRem + writeRem
                val threshold = readRem.toDouble() / sum.toDouble()
                rng.nextDouble() < threshold
            }
        }
    }

    private fun seedDataset(
        table: PackedTable<BenchAccount>,
        config: BenchmarkConfig
    ): Array<ShortUUID> {
        val keySpace = max(1, config.threadCount * config.keysPerThread)
        val keys = ArrayList<ShortUUID>(keySpace)
        val rng = ThreadLocalRandom.current()

        repeat(keySpace) {
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

    // ---- Config / Result / DTOs ----

    private data class BenchmarkConfig(
        val threadCount: Int,
        val keysPerThread: Int,
        val payloadBytes: Int,
        val targetReadRatioPercent: Int,
        val warmupOpsTotal: Long,
        val measurementOpsTotal: Long
    ) {
        init {
            require(threadCount > 0) { "threadCount must be positive" }
            require(keysPerThread > 0) { "keysPerThread must be positive" }
            require(payloadBytes > 0) { "payloadBytes must be positive" }
            require(targetReadRatioPercent in 0..100) { "targetReadRatioPercent must be 0..100" }
            require(warmupOpsTotal >= 0) { "warmupOpsTotal cannot be negative" }
            require(measurementOpsTotal > 0) { "measurementOpsTotal must be positive" }
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
        val throughputOpsPerSecond: Double =
            if (measurementDurationSeconds == 0.0) 0.0 else operations / measurementDurationSeconds
        val throughputPerThread: Double =
            if (config.threadCount == 0) 0.0 else throughputOpsPerSecond / config.threadCount
        val readSharePercent: Double =
            if (operations == 0L) 0.0 else (readOperations.toDouble() / operations) * 100.0
        val writeSharePercent: Double =
            if (operations == 0L) 0.0 else (writeOperations.toDouble() / operations) * 100.0

        fun toPrettyString(): String {
            val integerFormat = DecimalFormat("#,##0")
            val decimalFormat = DecimalFormat("#,##0.00")
            return buildString {
                appendLine("AkkaraDB DSL throughput benchmark")
                appendLine(" Startup mode         : FAST (custom WAL queue)")
                appendLine(" Threads              : ${config.threadCount}")
                appendLine(" Key space (records)  : ${integerFormat.format(config.totalKeySpace)}")
                appendLine(" Payload bytes        : ${integerFormat.format(config.payloadBytes)}")
                appendLine(" Target read ratio (%) : ${integerFormat.format(config.targetReadRatioPercent.toLong())}")
                appendLine(" Warm-up ops (total)  : ${integerFormat.format(config.warmupOpsTotal)}")
                appendLine(" Measurement ops      : ${integerFormat.format(config.measurementOpsTotal)}")
                appendLine(" Total operations     : ${integerFormat.format(operations)}")
                appendLine(" Ops/sec              : ${decimalFormat.format(throughputOpsPerSecond)}")
                appendLine(" Ops/sec/thread       : ${decimalFormat.format(throughputPerThread)}")
                appendLine(" Reads                : ${integerFormat.format(readOperations)} (${decimalFormat.format(readSharePercent)}%)")
                appendLine(" Writes               : ${integerFormat.format(writeOperations)} (${decimalFormat.format(writeSharePercent)}%)")
                appendLine(" Measurement time (s) : ${decimalFormat.format(measurementDurationSeconds)}")
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

    private data class QuadMetrics(
        val operations: Long,
        val reads: Long,
        val writes: Long,
        val elapsedNanos: Long
    )
}
