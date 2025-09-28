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
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder
import kotlin.math.max

class ThroughputBenchmarkTest {

    // ------------------------------
    // Public tests
    // ------------------------------

    @Test
    fun `AkkaraDB DSL throughput benchmark prints aggregated metrics with op-count stop`() {
        val availableProcessors = Runtime.getRuntime().availableProcessors()
        val threadCount = availableProcessors.coerceIn(1, 8)

        val config = BenchmarkConfig(
            threadCount = threadCount,
            keysPerThread = 512,
            payloadBytes = 256,
            preloadOpsTotal = 500_000,           // setup-only: build L0/SST
            preloadReadRatioPercent = 0,         // writes only for preload
            targetReadRatioPercent = 10,
            warmupOpsTotal = 0,             // excluded
            measurementOpsTotal = 500_000        // measured
        )

        val result = runDslThroughputBenchmark(config)
        println(result.toPrettyString())

        assertTrue(result.operations > 0, "Benchmark recorded no operations")
        assertEquals(result.operations, result.readOperations + result.writeOperations)
        assertEquals(config.measurementOpsTotal, result.operations, "Measured ops must match configured total")
        assertTrue(result.throughputOpsPerSecond > 0.0, "Throughput must be positive")
    }

    @Test
    fun `Latency percentiles for READ-ONLY, WRITE-ONLY, and MIXED ratios (includes P999)`() {
        val availableProcessors = Runtime.getRuntime().availableProcessors()
        val threadCount = availableProcessors.coerceIn(1, 8)

        val baseCfg = BenchmarkConfig(
            threadCount = threadCount,
            keysPerThread = 512,
            payloadBytes = 256,
            preloadOpsTotal = 500_000,   // build a steady dataset
            preloadReadRatioPercent = 0,
            targetReadRatioPercent = 100,
            warmupOpsTotal = 50_000,
            measurementOpsTotal = 500_000
        )

        // Common setup (seed + preload + warmup is executed inside)
        println("=== READ-ONLY (100:0) ===")
        runDslThroughputBenchmark(baseCfg.copy(targetReadRatioPercent = 100)).also {
            println(it.toPrettyString())
            val stats = computeLatencyStats(it)
            printPercentileTable("READ-ONLY", stats)
        }

        println("=== WRITE-ONLY (0:100) ===")
        runDslThroughputBenchmark(baseCfg.copy(targetReadRatioPercent = 0)).also {
            println(it.toPrettyString())
            val stats = computeLatencyStats(it)
            printPercentileTable("WRITE-ONLY", stats)
        }

        println("=== MIXED (R:W = 20:80) ===")
        runDslThroughputBenchmark(baseCfg.copy(targetReadRatioPercent = 20)).also {
            val stats = computeLatencyStats(it)
            printPercentileTable("MIXED 20:80", stats)
        }

        println("=== MIXED (R:W = 40:60) ===")
        runDslThroughputBenchmark(baseCfg.copy(targetReadRatioPercent = 40)).also {
            val stats = computeLatencyStats(it)
            printPercentileTable("MIXED 40:60", stats)
        }

        println("=== MIXED (R:W = 80:20) ===")
        runDslThroughputBenchmark(baseCfg.copy(targetReadRatioPercent = 80)).also {
            val stats = computeLatencyStats(it)
            printPercentileTable("MIXED 80:20", stats)
        }
    }

    // ------------------------------
    // Core runner
    // ------------------------------

    private fun runDslThroughputBenchmark(config: BenchmarkConfig): BenchmarkResult {
        require(config.threadCount > 0) { "threadCount must be positive" }
        require(config.measurementOpsTotal > 0) { "measurementOpsTotal must be positive" }

        val baseDir = Path.of("akkara-dsl-bench-${UUID.randomUUID()}").also { Files.createDirectories(it) }

        return AkkDSL.open<BenchAccount>(baseDir, StartupMode.FAST).use { table ->

            val keys = seedDataset(table, config)

            // Preload (excluded from metrics)
            if (config.preloadOpsTotal > 0) {
                runWorkersWithTargets(
                    table = table,
                    keys = keys,
                    threadCount = config.threadCount,
                    payloadBytes = config.payloadBytes,
                    totalOps = config.preloadOpsTotal,
                    targetReadRatioPercent = config.preloadReadRatioPercent,
                    recordMetrics = false
                )
            }

            // Warmup (excluded)
            if (config.warmupOpsTotal > 0) {
                runWorkersWithTargets(
                    table = table,
                    keys = keys,
                    threadCount = config.threadCount,
                    payloadBytes = config.payloadBytes,
                    totalOps = config.warmupOpsTotal,
                    targetReadRatioPercent = config.targetReadRatioPercent,
                    recordMetrics = false
                )
            }

            // Measurement (timed)
            val res = runWorkersWithTargets(
                table = table,
                keys = keys,
                threadCount = config.threadCount,
                payloadBytes = config.payloadBytes,
                totalOps = config.measurementOpsTotal,
                targetReadRatioPercent = config.targetReadRatioPercent,
                recordMetrics = true
            )

            BenchmarkResult(
                config = config,
                operations = res.operations,
                readOperations = res.reads,
                writeOperations = res.writes,
                measurementDurationSeconds = res.elapsedNanos / 1_000_000_000.0,
                readLatUs = res.readLatUs,
                writeLatUs = res.writeLatUs,
                readCount = res.readLatCount,
                writeCount = res.writeLatCount
            )
        }
    }

    /**
     * Run workers toward target counts. If recordMetrics=true, we capture latency per-op
     * (microseconds) separately for reads and writes.
     */
    private fun runWorkersWithTargets(
        table: PackedTable<BenchAccount>,
        keys: Array<ShortUUID>,
        threadCount: Int,
        payloadBytes: Int,
        totalOps: Long,
        targetReadRatioPercent: Int,
        recordMetrics: Boolean
    ): RunResult {
        require(keys.isNotEmpty()) { "Benchmark key space must not be empty" }
        val ratio = targetReadRatioPercent.coerceIn(0, 100)
        val targetReads = (totalOps * ratio) / 100
        val targetWrites = totalOps - targetReads

        val readRemaining = AtomicLong(targetReads)
        val writeRemaining = AtomicLong(targetWrites)

        val ops = LongAdder()
        val reads = LongAdder()
        val writes = LongAdder()

        val executor = Executors.newFixedThreadPool(threadCount)
        val tasks = ArrayList<Future<LocalBuf>>(threadCount)

        val startGate = java.util.concurrent.CountDownLatch(1)
        val doneGate = java.util.concurrent.CountDownLatch(threadCount)

        try {
            repeat(threadCount) { workerIndex ->
                tasks += executor.submit(Callable {
                    val rng = ThreadLocalRandom.current()
                    val buf = LocalBuf()

                    startGate.await()

                    while (true) {
                        val rRemSnapshot = readRemaining.get()
                        val wRemSnapshot = writeRemaining.get()
                        if (rRemSnapshot <= 0 && wRemSnapshot <= 0) break

                        val doRead = decideNextOp(rng, rRemSnapshot, wRemSnapshot)
                        if (doRead) {
                            val ticket = readRemaining.getAndDecrement()
                            if (ticket > 0) {
                                val uuid = keys[rng.nextInt(keys.size)]
                                val t0 = if (recordMetrics) System.nanoTime() else 0L
                                table.get("tenant", uuid)
                                val t1 = if (recordMetrics) System.nanoTime() else 0L
                                if (recordMetrics) buf.addRead((t1 - t0 + 500) / 1_000) // us丸め
                                reads.increment(); ops.increment()
                            } else {
                                readRemaining.incrementAndGet()
                            }
                        } else {
                            val ticket = writeRemaining.getAndDecrement()
                            if (ticket > 0) {
                                val uuid = keys[rng.nextInt(keys.size)]
                                val t0 = if (recordMetrics) System.nanoTime() else 0L
                                performWrite(table, uuid, workerIndex, ThreadLocalRandom.current(), payloadBytes)
                                val t1 = if (recordMetrics) System.nanoTime() else 0L
                                if (recordMetrics) buf.addWrite((t1 - t0 + 500) / 1_000)
                                writes.increment(); ops.increment()
                            } else {
                                writeRemaining.incrementAndGet()
                            }
                        }
                    }

                    doneGate.countDown()
                    buf
                })
            }

            val startNs = System.nanoTime()
            startGate.countDown()

            doneGate.await()

            val elapsed = System.nanoTime() - startNs

            var readCount = 0
            var writeCount = 0
            var readLatUs = LongArray(0)
            var writeLatUs = LongArray(0)

            for (f in tasks) {
                val b = f.get()
                if (b.readSize > 0) {
                    if (readLatUs.size < readCount + b.readSize) readLatUs = readLatUs.copyOf(readCount + b.readSize)
                    System.arraycopy(b.read, 0, readLatUs, readCount, b.readSize)
                    readCount += b.readSize
                }
                if (b.writeSize > 0) {
                    if (writeLatUs.size < writeCount + b.writeSize) writeLatUs = writeLatUs.copyOf(writeCount + b.writeSize)
                    System.arraycopy(b.write, 0, writeLatUs, writeCount, b.writeSize)
                    writeCount += b.writeSize
                }
            }

            return RunResult(
                operations = ops.sum(),
                reads = reads.sum(),
                writes = writes.sum(),
                elapsedNanos = elapsed,
                readLatUs = readLatUs,
                writeLatUs = writeLatUs,
                readLatCount = readCount,
                writeLatCount = writeCount
            )
        } finally {
            executor.shutdown()
            try {
                if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
                    executor.shutdownNow()
                }
            } catch (ie: InterruptedException) {
                executor.shutdownNow()
                Thread.currentThread().interrupt()
            }
        }
    }

    /** Decide next op based on remaining counts (proportional). */
    private fun decideNextOp(rng: ThreadLocalRandom, readRem: Long, writeRem: Long): Boolean {
        return when {
            readRem <= 0 && writeRem > 0 -> false
            writeRem <= 0 && readRem > 0 -> true
            readRem <= 0 && writeRem <= 0 -> true // break by caller soon
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
                    version = 1,
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
        // Preload controls (excluded from metrics)
        val preloadOpsTotal: Long = 0,
        val preloadReadRatioPercent: Int = 0,
        // Measurement controls
        val targetReadRatioPercent: Int,
        val warmupOpsTotal: Long,
        val measurementOpsTotal: Long
    ) {
        init {
            require(threadCount > 0) { "threadCount must be positive" }
            require(keysPerThread > 0) { "keysPerThread must be positive" }
            require(payloadBytes > 0) { "payloadBytes must be positive" }
            require(preloadReadRatioPercent in 0..100) { "preloadReadRatioPercent must be 0..100" }
            require(targetReadRatioPercent in 0..100) { "targetReadRatioPercent must be 0..100" }
            require(warmupOpsTotal >= 0) { "warmupOpsTotal cannot be negative" }
            require(measurementOpsTotal > 0) { "measurementOpsTotal must be positive" }
            require(preloadOpsTotal >= 0) { "preloadOpsTotal cannot be negative" }
        }

        val totalKeySpace: Int = threadCount * keysPerThread
    }

    private data class BenchmarkResult(
        val config: BenchmarkConfig,
        val operations: Long,
        val readOperations: Long,
        val writeOperations: Long,
        val measurementDurationSeconds: Double,
        // latency capture
        val readLatUs: LongArray = LongArray(0),
        val writeLatUs: LongArray = LongArray(0),
        val readCount: Int = 0,
        val writeCount: Int = 0
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
                if (config.preloadOpsTotal > 0) {
                    appendLine(" Preload ops (setup)  : ${integerFormat.format(config.preloadOpsTotal)} [${config.preloadReadRatioPercent}% read]")
                }
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

    private data class RunResult(
        val operations: Long,
        val reads: Long,
        val writes: Long,
        val elapsedNanos: Long,
        val readLatUs: LongArray,
        val writeLatUs: LongArray,
        val readLatCount: Int,
        val writeLatCount: Int
    )

    // Per-thread latency buffer with simple growable arrays
    private class LocalBuf(
        initCap: Int = 1024
    ) {
        var read = LongArray(initCap)
        var write = LongArray(initCap)
        var readSize = 0
        var writeSize = 0
        fun addRead(us: Long) {
            if (readSize == read.size) read = read.copyOf(read.size * 2)
            read[readSize++] = us
        }

        fun addWrite(us: Long) {
            if (writeSize == write.size) write = write.copyOf(write.size * 2)
            write[writeSize++] = us
        }
    }

    data class LatencyStats(
        val count: Int,
        val p50: Double,
        val p90: Double,
        val p99: Double,
        val p999: Double,
        val max: Double
    )

    private fun computeLatencyStats(result: BenchmarkResult): Pair<LatencyStats?, LatencyStats?> {
        val readStats = if (result.readCount > 0) statsOf(result.readLatUs, result.readCount) else null
        val writeStats = if (result.writeCount > 0) statsOf(result.writeLatUs, result.writeCount) else null
        return readStats to writeStats
    }

    private fun statsOf(arr: LongArray, count: Int): LatencyStats {
        val copy = if (arr.size == count) arr.copyOf() else arr.copyOfRange(0, count)
        Arrays.sort(copy)
        fun pick(q: Double): Double {
            if (count == 0) return 0.0
            val idx = kotlin.math.floor(q * (count - 1)).toInt().coerceIn(0, count - 1)
            return copy[idx].toDouble() // already microseconds
        }

        val max = if (count > 0) copy[count - 1].toDouble() else 0.0
        return LatencyStats(
            count = count,
            p50 = pick(0.50),
            p90 = pick(0.90),
            p99 = pick(0.99),
            p999 = pick(0.999),
            max = max
        )
    }

    private fun printPercentileTable(title: String, stats: Pair<LatencyStats?, LatencyStats?>) {
        val (r, w) = stats
        fun fmt(s: LatencyStats?): String = if (s == null) "n/a" else
            "P50 ${s.p50}µs | P90 ${s.p90}µs | P99 ${s.p99}µs | P99.9 ${s.p999}µs | max ${s.max}µs (n=${s.count})"
        println("$title READ : ${fmt(r)} WRITE: ${fmt(w)}")
    }

    // ---- DTOs for the benchmarked entity ----

    data class BenchAccount(
        var accountId: String = "",
        var balance: Long = 0,
        var version: Long = 0,
        var payload: ByteArray = ByteArray(0),
        var tags: List<String> = emptyList()
    )
}
