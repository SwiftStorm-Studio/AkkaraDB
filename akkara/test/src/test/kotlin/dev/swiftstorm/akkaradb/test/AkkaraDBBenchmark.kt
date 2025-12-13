/**
 * AkkaraDB v3 ベンチマークテストコード
 *
 * BENCHMARKS.md に記載されているベンチマーク結果を再測定するためのテストコードです。
 *
 * 依存関係:
 * - dev.swiftstorm.akkaradb:akkara-engine:<version>
 *
 * 使用方法:
 * ./gradlew run --args="--benchmark all"
 * または個別のベンチマーク:
 * ./gradlew run --args="--benchmark write"
 * ./gradlew run --args="--benchmark read"
 * ./gradlew run --args="--benchmark mixed"
 */

package dev.swiftstorm.akkaradb.test

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.engine.AkkDSL
import dev.swiftstorm.akkaradb.engine.AkkaraDB
import dev.swiftstorm.akkaradb.engine.Id
import dev.swiftstorm.akkaradb.engine.StartupMode
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.sqrt
import kotlin.random.Random
import kotlin.system.measureNanoTime

// =============================================================================
// 設定定数
// =============================================================================

object BenchConfig {
    const val DEFAULT_KEY_COUNT = 1_000_000
    const val DEFAULT_VALUE_SIZE = 64
    const val WARMUP_COUNT = 10_000
    const val JVM_WARMUP_ITERATIONS = 3
}

// =============================================================================
// レイテンシ統計クラス
// =============================================================================

class LatencyStats(private val name: String) {
    private val latencies = ArrayList<Long>(BenchConfig.DEFAULT_KEY_COUNT)

    @Synchronized
    fun record(nanos: Long) {
        latencies.add(nanos)
    }

    @Synchronized
    fun recordAll(data: LongArray) {
        for (l in data) latencies.add(l)
    }

    fun report(): StatsReport {
        if (latencies.isEmpty()) return StatsReport(name, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

        val sorted = latencies.sorted()
        val count = sorted.size
        val sum = sorted.sum()
        val mean = sum.toDouble() / count
        val variance = sorted.sumOf { (it - mean) * (it - mean) } / count
        val stdDev = sqrt(variance)

        fun percentile(p: Double): Double {
            val idx = ((p / 100.0) * (count - 1)).toInt().coerceIn(0, count - 1)
            return sorted[idx] / 1000.0 // nanos -> micros
        }

        return StatsReport(
            name = name,
            count = count,
            opsPerSec = count.toDouble() / (sum / 1_000_000_000.0),
            p50 = percentile(50.0),
            p75 = percentile(75.0),
            p90 = percentile(90.0),
            p95 = percentile(95.0),
            p99 = percentile(99.0),
            p999 = percentile(99.9),
            p9999 = percentile(99.99),
            max = sorted.last() / 1000.0
        )
    }
}

data class StatsReport(
    val name: String,
    val count: Int,
    val opsPerSec: Double,
    val p50: Double,
    val p75: Double,
    val p90: Double,
    val p95: Double,
    val p99: Double,
    val p999: Double,
    val p9999: Double,
    val max: Double
) {
    override fun toString(): String = buildString {
        appendLine("=== $name ===")
        appendLine("  Total ops:    %,d".format(count))
        appendLine("  ops/sec:      %,.0f".format(opsPerSec))
        appendLine("  p50:          %.1f µs".format(p50))
        appendLine("  p75:          %.1f µs".format(p75))
        appendLine("  p90:          %.1f µs".format(p90))
        appendLine("  p95:          %.1f µs".format(p95))
        appendLine("  p99:          %.1f µs".format(p99))
        appendLine("  p99.9:        %.1f µs".format(p999))
        appendLine("  p99.99:       %.1f µs".format(p9999))
        appendLine("  max:          %.1f µs".format(max))
    }
}

// =============================================================================
// ユーティリティ関数
// =============================================================================

fun createTempDir(prefix: String): Path {
    val dir = Files.createTempDirectory(prefix)
    Runtime.getRuntime().addShutdownHook(Thread {
        dir.toFile().deleteRecursively()
    })
    return dir
}

fun generateKey(index: Int): ByteBufferL {
    val keyStr = "key:%08d".format(index)
    return ByteBufferL.wrap(StandardCharsets.UTF_8.encode(keyStr)).position(0)
}

fun generateValue(size: Int): ByteBufferL {
    val bytes = ByteArray(size) { (it % 256).toByte() }
    return ByteBufferL.wrap(ByteBuffer.wrap(bytes)).position(0)
}

// =============================================================================
// 1. 書き込みベンチマーク - WAL Group Commit チューニング
// =============================================================================

class WriteWalGroupBenchmark {
    data class WalGroupConfig(
        val walGroupN: Int,
        val walGroupMicros: Long,
        val description: String
    )

    private val configs = listOf(
        WalGroupConfig(64, 1_000, "① WalGroupN=64, Micros=1000 (fsync過多)"),
        WalGroupConfig(128, 5_000, "② WalGroupN=128, Micros=5000"),
        WalGroupConfig(128, 10_000, "③ WalGroupN=128, Micros=10000"),
        WalGroupConfig(256, 1_000, "④ WalGroupN=256, Micros=1000"),
        WalGroupConfig(256, 10_000, "⑤ WalGroupN=256, Micros=10000"),
        WalGroupConfig(512, 10_000, "⑥ WalGroupN=512, Micros=10000"),
        WalGroupConfig(512, 50_000, "⑦ WalGroupN=512, Micros=50000 (最適点)")
    )

    fun run(keyCount: Int = 100_000, valueSize: Int = 64) {
        println("\n" + "=".repeat(70))
        println("📊 書き込みベンチマーク - WAL Group Commit チューニング")
        println("=".repeat(70))
        println("Key count: %,d, Value size: %d bytes".format(keyCount, valueSize))
        println()

        val results = mutableListOf<Pair<WalGroupConfig, StatsReport>>()

        for (cfg in configs) {
            val baseDir = createTempDir("akkdb-wal-bench-")
            val stats = LatencyStats(cfg.description)
            val latencies = LongArray(keyCount)

            try {
                val db = AkkaraDB.open(
                    AkkaraDB.Options(
                        baseDir = baseDir,
                        k = 4,
                        m = 2,
                        walGroupN = cfg.walGroupN,
                        walGroupMicros = cfg.walGroupMicros,
                        walFastMode = true,
                        stripeFastMode = true
                    )
                )

                // Warmup
                repeat(BenchConfig.WARMUP_COUNT.coerceAtMost(keyCount / 10)) { i ->
                    val key = generateKey(i)
                    val value = generateValue(valueSize)
                    db.put(key, value)
                }
                db.flush()

                // Benchmark
                repeat(keyCount) { i ->
                    val key = generateKey(i + 1_000_000)
                    val value = generateValue(valueSize)
                    val elapsed = measureNanoTime {
                        db.put(key, value)
                    }
                    latencies[i] = elapsed
                }

                db.close()
                stats.recordAll(latencies)
                val report = stats.report()
                results.add(cfg to report)

                println("✓ ${cfg.description}")
                println(
                    "  ops/sec: %,.0f, p50: %.1f µs, p99: %.1f µs".format(
                        report.opsPerSec, report.p50, report.p99
                    )
                )
                println()

            } catch (e: Exception) {
                println("✗ ${cfg.description} - Error: ${e.message}")
            } finally {
                baseDir.toFile().deleteRecursively()
            }
        }

        // Summary table
        println("\n📋 サマリーテーブル:")
        println("-".repeat(100))
        println(
            "| %-40s | %10s | %8s | %8s | %8s |".format(
                "設定", "ops/sec", "p50(µs)", "p90(µs)", "p99(µs)"
            )
        )
        println("-".repeat(100))
        for ((cfg, report) in results) {
            println(
                "| %-40s | %,10.0f | %8.1f | %8.1f | %8.1f |".format(
                    cfg.description.take(40), report.opsPerSec, report.p50, report.p90, report.p99
                )
            )
        }
        println("-".repeat(100))
    }
}

// =============================================================================
// 2. 書き込みベンチマーク - キー数によるスケーラビリティ
// =============================================================================

class WriteScalabilityBenchmark {
    fun run(valueSize: Int = 64) {
        println("\n" + "=".repeat(70))
        println("📊 書き込みベンチマーク - キー数によるスケーラビリティ")
        println("=".repeat(70))

        val keyCounts = listOf(10_000, 100_000, 1_000_000)
        val results = mutableListOf<Triple<Int, Double, StatsReport>>()

        for (keyCount in keyCounts) {
            val baseDir = createTempDir("akkdb-scale-bench-")
            val stats = LatencyStats("$keyCount keys")
            val latencies = LongArray(keyCount)

            try {
                val db = AkkaraDB.open(
                    AkkaraDB.Options(
                        baseDir = baseDir,
                        k = 4,
                        m = 2,
                        walGroupN = 512,
                        walGroupMicros = 50_000,
                        walFastMode = true,
                        stripeFastMode = true
                    )
                )

                val startTime = System.nanoTime()

                repeat(keyCount) { i ->
                    val key = generateKey(i)
                    val value = generateValue(valueSize)
                    val elapsed = measureNanoTime {
                        db.put(key, value)
                    }
                    latencies[i] = elapsed
                }

                val totalTime = (System.nanoTime() - startTime) / 1_000_000_000.0
                db.close()

                stats.recordAll(latencies)
                val report = stats.report()
                results.add(Triple(keyCount, totalTime, report))

                println(
                    "✓ %,d keys: %.2fs, %,.0f ops/sec, p99: %.1f µs".format(
                        keyCount, totalTime, report.opsPerSec, report.p99
                    )
                )

            } catch (e: Exception) {
                println("✗ %,d keys - Error: ${e.message}".format(keyCount))
            } finally {
                baseDir.toFile().deleteRecursively()
            }
        }

        println("\n📋 サマリー:")
        println("-".repeat(80))
        println(
            "| %12s | %10s | %12s | %10s | %10s |".format(
                "キー数", "総時間(s)", "ops/sec", "p50(µs)", "p99(µs)"
            )
        )
        println("-".repeat(80))
        for ((count, time, report) in results) {
            println(
                "| %,12d | %10.2f | %,12.0f | %10.1f | %10.1f |".format(
                    count, time, report.opsPerSec, report.p50, report.p99
                )
            )
        }
        println("-".repeat(80))
    }
}

// =============================================================================
// 3. 書き込みベンチマーク - バリューサイズによる影響
// =============================================================================

class WriteValueSizeBenchmark {
    fun run(keyCount: Int = 100_000) {
        println("\n" + "=".repeat(70))
        println("📊 書き込みベンチマーク - バリューサイズによる影響")
        println("=".repeat(70))

        val valueSizes = listOf(16, 64, 256, 1024, 4096, 16384)
        val results = mutableListOf<Pair<Int, StatsReport>>()

        for (valueSize in valueSizes) {
            val baseDir = createTempDir("akkdb-valsize-bench-")
            val stats = LatencyStats("$valueSize bytes")
            val latencies = LongArray(keyCount)

            try {
                val db = AkkaraDB.open(
                    AkkaraDB.Options(
                        baseDir = baseDir,
                        k = 4,
                        m = 2,
                        walGroupN = 512,
                        walGroupMicros = 50_000,
                        walFastMode = true,
                        stripeFastMode = true
                    )
                )

                // Warmup
                repeat(BenchConfig.WARMUP_COUNT.coerceAtMost(keyCount / 10)) { i ->
                    val key = generateKey(i)
                    val value = generateValue(valueSize)
                    db.put(key, value)
                }
                db.flush()

                // Benchmark
                repeat(keyCount) { i ->
                    val key = generateKey(i + 1_000_000)
                    val value = generateValue(valueSize)
                    val elapsed = measureNanoTime {
                        db.put(key, value)
                    }
                    latencies[i] = elapsed
                }

                db.close()
                stats.recordAll(latencies)
                val report = stats.report()
                results.add(valueSize to report)

                val throughputMBps = (keyCount.toLong() * valueSize) /
                        (keyCount.toDouble() / report.opsPerSec) / (1024 * 1024)
                println(
                    "✓ %,6d B: %,10.0f ops/sec, p99: %8.1f µs, %.1f MB/s".format(
                        valueSize, report.opsPerSec, report.p99, throughputMBps
                    )
                )

            } catch (e: Exception) {
                println("✗ %,d B - Error: ${e.message}".format(valueSize))
            } finally {
                baseDir.toFile().deleteRecursively()
            }
        }

        println("\n📋 サマリー:")
        println("-".repeat(80))
        println(
            "| %12s | %12s | %10s | %10s | %14s |".format(
                "ValueSize", "ops/sec", "p50(µs)", "p99(µs)", "スループット(MB/s)"
            )
        )
        println("-".repeat(80))
        for ((size, report) in results) {
            val throughputMBps = (100_000L * size) /
                    (100_000.0 / report.opsPerSec) / (1024 * 1024)
            println(
                "| %,12d | %,12.0f | %10.1f | %10.1f | %14.1f |".format(
                    size, report.opsPerSec, report.p50, report.p99, throughputMBps
                )
            )
        }
        println("-".repeat(80))
    }
}

// =============================================================================
// 4. 読み取りベンチマーク - MemTable vs SST
// =============================================================================

class ReadBenchmark {
    fun run(keyCount: Int = 100_000, valueSize: Int = 64) {
        println("\n" + "=".repeat(70))
        println("📊 読み取りベンチマーク - MemTable vs SST")
        println("=".repeat(70))

        // MemTable読み取り
        runMemTableRead(keyCount, valueSize)

        // SST読み取り
        runSSTRead(keyCount, valueSize)
    }

    private fun runMemTableRead(keyCount: Int, valueSize: Int) {
        println("\n--- MemTable読み取り ---")
        val baseDir = createTempDir("akkdb-read-mem-")
        val stats = LatencyStats("MemTable Read")
        val latencies = LongArray(keyCount)

        try {
            val db = AkkaraDB.open(
                AkkaraDB.Options(
                    baseDir = baseDir,
                    k = 4,
                    m = 2,
                    walGroupN = 512,
                    walGroupMicros = 50_000,
                    walFastMode = true,
                    stripeFastMode = true
                )
            )

            // データ投入（MemTableに残す）
            repeat(keyCount) { i ->
                val key = generateKey(i)
                val value = generateValue(valueSize)
                db.put(key, value)
            }
            // flush()を呼ばないことでMemTableに残す

            // Warmup
            repeat(BenchConfig.WARMUP_COUNT.coerceAtMost(keyCount / 10)) { i ->
                val key = generateKey(Random.nextInt(keyCount))
                db.get(key)
            }

            // Benchmark (ランダム読み取り)
            val indices = (0 until keyCount).shuffled()
            repeat(keyCount) { i ->
                val key = generateKey(indices[i])
                val elapsed = measureNanoTime {
                    db.get(key)
                }
                latencies[i] = elapsed
            }

            db.close()
            stats.recordAll(latencies)
            val report = stats.report()

            println(report)

        } catch (e: Exception) {
            println("Error: ${e.message}")
            e.printStackTrace()
        } finally {
            baseDir.toFile().deleteRecursively()
        }
    }

    private fun runSSTRead(keyCount: Int, valueSize: Int) {
        println("\n--- SST読み取り (Bloomフィルター有効) ---")
        val baseDir = createTempDir("akkdb-read-sst-")
        val stats = LatencyStats("SST Read")
        val latencies = LongArray(keyCount)

        try {
            val db = AkkaraDB.open(
                AkkaraDB.Options(
                    baseDir = baseDir,
                    k = 4,
                    m = 2,
                    walGroupN = 512,
                    walGroupMicros = 50_000,
                    walFastMode = true,
                    stripeFastMode = true,
                    bloomFPRate = 0.01
                )
            )

            // データ投入
            repeat(keyCount) { i ->
                val key = generateKey(i)
                val value = generateValue(valueSize)
                db.put(key, value)
            }
            // SSTにフラッシュ
            db.flush()

            // 新しいDBインスタンスを開いてSSTから読む
            db.close()

            val db2 = AkkaraDB.open(
                AkkaraDB.Options(
                    baseDir = baseDir,
                    k = 4,
                    m = 2,
                    walGroupN = 512,
                    walGroupMicros = 50_000,
                    walFastMode = true,
                    stripeFastMode = true,
                    bloomFPRate = 0.01
                )
            )

            // Warmup
            repeat(BenchConfig.WARMUP_COUNT.coerceAtMost(keyCount / 10)) { i ->
                val key = generateKey(Random.nextInt(keyCount))
                db2.get(key)
            }

            // Benchmark (ランダム読み取り)
            val indices = (0 until keyCount).shuffled()
            repeat(keyCount) { i ->
                val key = generateKey(indices[i])
                val elapsed = measureNanoTime {
                    db2.get(key)
                }
                latencies[i] = elapsed
            }

            db2.close()
            stats.recordAll(latencies)
            val report = stats.report()

            println(report)

        } catch (e: Exception) {
            println("Error: ${e.message}")
            e.printStackTrace()
        } finally {
            baseDir.toFile().deleteRecursively()
        }
    }
}

// =============================================================================
// 5. Bloomフィルター効果ベンチマーク
// =============================================================================

class BloomFilterBenchmark {
    fun run(keyCount: Int = 100_000) {
        println("\n" + "=".repeat(70))
        println("📊 Bloomフィルター効果ベンチマーク")
        println("=".repeat(70))

        // 存在しないキーの検索（Bloomフィルターの効果を見る）
        val baseDir = createTempDir("akkdb-bloom-bench-")

        try {
            val db = AkkaraDB.open(
                AkkaraDB.Options(
                    baseDir = baseDir,
                    k = 4,
                    m = 2,
                    walGroupN = 512,
                    walGroupMicros = 50_000,
                    walFastMode = true,
                    stripeFastMode = true,
                    bloomFPRate = 0.01
                )
            )

            // データ投入（key:00000000 ~ key:00099999）
            repeat(keyCount) { i ->
                val key = generateKey(i)
                val value = generateValue(64)
                db.put(key, value)
            }
            db.flush()
            db.close()

            // 再オープン
            val db2 = AkkaraDB.open(
                AkkaraDB.Options(
                    baseDir = baseDir,
                    k = 4,
                    m = 2,
                    walGroupN = 512,
                    walGroupMicros = 50_000,
                    walFastMode = true,
                    stripeFastMode = true,
                    bloomFPRate = 0.01
                )
            )

            // 存在しないキー（key:10000000 ~ key:10099999）を検索
            val stats = LatencyStats("Negative lookup (Bloom enabled)")
            val latencies = LongArray(keyCount)
            var falsePositives = 0

            repeat(keyCount) { i ->
                val key = generateKey(i + 10_000_000) // 存在しないキー
                val elapsed = measureNanoTime {
                    val result = db2.get(key)
                    if (result != null) falsePositives++
                }
                latencies[i] = elapsed
            }

            db2.close()
            stats.recordAll(latencies)
            val report = stats.report()

            println(report)
            println("  False Positive Count: $falsePositives / $keyCount")
            println("  False Positive Rate:  %.2f%%".format(falsePositives * 100.0 / keyCount))

        } catch (e: Exception) {
            println("Error: ${e.message}")
            e.printStackTrace()
        } finally {
            baseDir.toFile().deleteRecursively()
        }
    }
}

// =============================================================================
// 6. 範囲検索ベンチマーク
// =============================================================================

class RangeScanBenchmark {
    fun run(keyCount: Int = 100_000) {
        println("\n" + "=".repeat(70))
        println("📊 範囲検索ベンチマーク")
        println("=".repeat(70))

        val rangeSizes = listOf(100, 1_000, 10_000, 100_000)
        val results = mutableListOf<Triple<Int, Long, Double>>()

        val baseDir = createTempDir("akkdb-range-bench-")

        try {
            val db = AkkaraDB.open(
                AkkaraDB.Options(
                    baseDir = baseDir,
                    k = 4,
                    m = 2,
                    walGroupN = 512,
                    walGroupMicros = 50_000,
                    walFastMode = true,
                    stripeFastMode = true
                )
            )

            try {
                // データ投入
                repeat(keyCount) { i ->
                    val key = generateKey(i)
                    val value = generateValue(64)
                    db.put(key, value)
                }
                db.flush()

                for (rangeSize in rangeSizes) {
                    if (rangeSize > keyCount) continue

                    val startKey = generateKey(0)
                    val endKey = generateKey(rangeSize)

                    var count = 0
                    val elapsed = measureNanoTime {
                        for (rec in db.range(startKey, endKey)) {
                            count++
                        }
                    }

                    val avgPerEntry = elapsed.toDouble() / count / 1000.0 // µs

                    results.add(Triple(rangeSize, elapsed / 1_000_000, avgPerEntry))
                    println(
                        "✓ Range size %,6d: %,6d ms total, %.1f µs/entry".format(
                            rangeSize, elapsed / 1_000_000, avgPerEntry
                        )
                    )
                }
            } finally {
                db.close()
            }

            println("\n📋 サマリー:")
            println("-".repeat(60))
            println("| %12s | %12s | %14s |".format("範囲サイズ", "総時間(ms)", "平均/エントリ(µs)"))
            println("-".repeat(60))
            for ((size, totalMs, avgUs) in results) {
                println("| %,12d | %,12d | %14.1f |".format(size, totalMs, avgUs))
            }
            println("-".repeat(60))

        } catch (e: Exception) {
            println("Error: ${e.message}")
            e.printStackTrace()
        } finally {
            baseDir.toFile().deleteRecursively()
        }
    }
}

// =============================================================================
// 7. 混合ワークロードベンチマーク
// =============================================================================

class MixedWorkloadBenchmark {
    fun run(totalOps: Int = 1_000_000, valueSize: Int = 64) {
        println("\n" + "=".repeat(70))
        println("📊 混合ワークロードベンチマーク")
        println("=".repeat(70))

        val ratios = listOf(
            Pair(100, 0),   // 100% read
            Pair(80, 20),   // 80% read, 20% write
            Pair(50, 50),   // 50/50
            Pair(20, 80),   // 20% read, 80% write
            Pair(0, 100)    // 100% write
        )

        val results = mutableListOf<MixedResult>()

        for ((readPct, writePct) in ratios) {
            val baseDir = createTempDir("akkdb-mixed-bench-")

            try {
                val db = AkkaraDB.open(
                    AkkaraDB.Options(
                        baseDir = baseDir,
                        k = 4,
                        m = 2,
                        walGroupN = 512,
                        walGroupMicros = 50_000,
                        walFastMode = true,
                        stripeFastMode = true
                    )
                )

                // Seed data
                val seedCount = 100_000
                repeat(seedCount) { i ->
                    val key = generateKey(i)
                    val value = generateValue(valueSize)
                    db.put(key, value)
                }

                val readStats = LatencyStats("Read ($readPct%)")
                val writeStats = LatencyStats("Write ($writePct%)")
                val readLatencies = ArrayList<Long>()
                val writeLatencies = ArrayList<Long>()

                val random = Random(42)
                var writeKeyIdx = seedCount

                val startTime = System.nanoTime()

                repeat(totalOps) {
                    if (random.nextInt(100) < readPct) {
                        // Read
                        val key = generateKey(random.nextInt(seedCount))
                        val elapsed = measureNanoTime {
                            db.get(key)
                        }
                        readLatencies.add(elapsed)
                    } else {
                        // Write
                        val key = generateKey(writeKeyIdx++)
                        val value = generateValue(valueSize)
                        val elapsed = measureNanoTime {
                            db.put(key, value)
                        }
                        writeLatencies.add(elapsed)
                    }
                }

                val totalTime = (System.nanoTime() - startTime) / 1_000_000_000.0
                val totalOpsPerSec = totalOps / totalTime

                db.close()

                readStats.recordAll(readLatencies.toLongArray())
                writeStats.recordAll(writeLatencies.toLongArray())

                val readReport = readStats.report()
                val writeReport = writeStats.report()

                results.add(MixedResult(readPct, writePct, totalOpsPerSec, readReport.p99, writeReport.p99))

                println(
                    "✓ Read %3d%% / Write %3d%%: %,.0f ops/sec, Read p99: %.1f µs, Write p99: %.1f µs".format(
                        readPct, writePct, totalOpsPerSec, readReport.p99, writeReport.p99
                    )
                )

            } catch (e: Exception) {
                println("✗ Read $readPct% / Write $writePct% - Error: ${e.message}")
            } finally {
                baseDir.toFile().deleteRecursively()
            }
        }

        println("\n📋 サマリー:")
        println("-".repeat(80))
        println(
            "| %8s | %8s | %14s | %14s | %14s |".format(
                "Read%", "Write%", "総ops/sec", "Read p99(µs)", "Write p99(µs)"
            )
        )
        println("-".repeat(80))
        for (r in results) {
            println(
                "| %8d | %8d | %,14.0f | %14.1f | %14.1f |".format(
                    r.readPct, r.writePct, r.totalOpsPerSec, r.readP99, r.writeP99
                )
            )
        }
        println("-".repeat(80))
    }

    data class MixedResult(
        val readPct: Int,
        val writePct: Int,
        val totalOpsPerSec: Double,
        val readP99: Double,
        val writeP99: Double
    )
}

// =============================================================================
// 8. マルチスレッドスケーラビリティベンチマーク
// =============================================================================

class MultiThreadBenchmark {
    fun run(opsPerThread: Int = 100_000, valueSize: Int = 64) {
        println("\n" + "=".repeat(70))
        println("📊 マルチスレッドスケーラビリティベンチマーク")
        println("=".repeat(70))

        val threadCounts = listOf(1, 2, 4, 8, 16)
        val results = mutableListOf<Pair<Int, Double>>()

        for (threads in threadCounts) {
            val baseDir = createTempDir("akkdb-mt-bench-")

            try {
                val db = AkkaraDB.open(
                    AkkaraDB.Options(
                        baseDir = baseDir,
                        k = 4,
                        m = 2,
                        walGroupN = 512,
                        walGroupMicros = 50_000,
                        walFastMode = true,
                        stripeFastMode = true
                    )
                )

                val latch = CountDownLatch(threads)
                val executor = Executors.newFixedThreadPool(threads)
                val totalOps = AtomicLong(0)

                val startTime = System.nanoTime()

                repeat(threads) { tid ->
                    executor.submit {
                        try {
                            repeat(opsPerThread) { i ->
                                val key = generateKey(tid * opsPerThread + i)
                                val value = generateValue(valueSize)
                                db.put(key, value)
                                totalOps.incrementAndGet()
                            }
                        } finally {
                            latch.countDown()
                        }
                    }
                }

                latch.await()
                val elapsedNs = System.nanoTime() - startTime
                val opsPerSec = totalOps.get() * 1_000_000_000.0 / elapsedNs

                executor.shutdown()
                db.close()

                results.add(threads to opsPerSec)
                println("✓ %2d threads: %,12.0f ops/sec".format(threads, opsPerSec))

            } catch (e: Exception) {
                println("✗ $threads threads - Error: ${e.message}")
            } finally {
                baseDir.toFile().deleteRecursively()
            }
        }

        val baseline = results.firstOrNull()?.second ?: 1.0

        println("\n📋 サマリー:")
        println("-".repeat(60))
        println(
            "| %10s | %14s | %12s | %10s |".format(
                "スレッド数", "ops/sec", "スケール比", "効率"
            )
        )
        println("-".repeat(60))
        for ((threads, ops) in results) {
            val scale = ops / baseline
            val efficiency = scale / threads * 100
            println(
                "| %10d | %,14.0f | %12.2fx | %9.0f%% |".format(
                    threads, ops, scale, efficiency
                )
            )
        }
        println("-".repeat(60))
    }
}

// =============================================================================
// 9. Typed API (AkkDSL) ベンチマーク
// =============================================================================

// データモデル
data class BenchUser(
    @Id val id: String,
    val name: String,
    val age: Int,
    val email: String
)

class TypedApiBenchmark {
    fun run(keyCount: Int = 100_000) {
        println("\n" + "=".repeat(70))
        println("📊 Typed API (AkkDSL) ベンチマーク")
        println("=".repeat(70))

        val modes = listOf(
            StartupMode.NORMAL to "NORMAL",
            StartupMode.FAST to "FAST",
            StartupMode.ULTRA_FAST to "ULTRA_FAST"
        )

        for ((mode, modeName) in modes) {
            val baseDir = createTempDir("akkdb-typed-bench-")

            try {
                val db = AkkDSL.open<BenchUser, String>(baseDir, mode)

                val writeStats = LatencyStats("$modeName Write")
                val readStats = LatencyStats("$modeName Read")
                val writeLatencies = LongArray(keyCount)
                val readLatencies = LongArray(keyCount)

                // Write benchmark
                repeat(keyCount) { i ->
                    val user = BenchUser(
                        id = "user_%08d".format(i),
                        name = "User $i",
                        age = 20 + (i % 50),
                        email = "user$i@example.com"
                    )
                    val elapsed = measureNanoTime {
                        db.put(user)
                    }
                    writeLatencies[i] = elapsed
                }

                // Read benchmark
                val indices = (0 until keyCount).shuffled()
                repeat(keyCount) { i ->
                    val id = "user_%08d".format(indices[i])
                    try {
                        val elapsed = measureNanoTime {
                            db.get(id)
                        }
                        readLatencies[i] = elapsed
                    } catch (e: Exception) {
                        println("Failed at i=$i, id=$id")
                        throw e
                    }
                }

                db.close()

                writeStats.recordAll(writeLatencies)
                readStats.recordAll(readLatencies)

                val writeReport = writeStats.report()
                val readReport = readStats.report()

                println("\n=== $modeName ===")
                println(
                    "  Write: %,10.0f ops/sec, p50: %.1f µs, p99: %.1f µs".format(
                        writeReport.opsPerSec, writeReport.p50, writeReport.p99
                    )
                )
                println(
                    "  Read:  %,10.0f ops/sec, p50: %.1f µs, p99: %.1f µs".format(
                        readReport.opsPerSec, readReport.p50, readReport.p99
                    )
                )

            } catch (e: Exception) {
                println("✗ $modeName - Error: ${e.message}")
                e.printStackTrace()
            } finally {
                baseDir.toFile().deleteRecursively()
            }
        }
    }
}

// =============================================================================
// メイン
// =============================================================================

fun main(args: Array<String>) {
    println(
        """
        ╔══════════════════════════════════════════════════════════════════════╗
        ║           AkkaraDB v3 ベンチマークスイート                               ║
        ╠══════════════════════════════════════════════════════════════════════╣
        ║  使用方法:                                                            ║
        ║    --benchmark all         すべてのベンチマーク                         ║
        ║    --benchmark write       書き込みベンチマーク                         ║
        ║    --benchmark read        読み取りベンチマーク                         ║
        ║    --benchmark bloom       Bloomフィルターベンチマーク                  ║
        ║    --benchmark range       範囲検索ベンチマーク                         ║
        ║    --benchmark mixed       混合ワークロードベンチマーク                   ║
        ║    --benchmark mt          マルチスレッドベンチマーク                     ║
        ║    --benchmark typed       Typed API ベンチマーク                      ║
        ╚══════════════════════════════════════════════════════════════════════╝
    """.trimIndent()
    )

    val benchmarkType = if (args.contains("--benchmark")) {
        val idx = args.indexOf("--benchmark")
        args.getOrElse(idx + 1) { "all" }
    } else {
        "all"
    }

    println("\n🚀 実行するベンチマーク: $benchmarkType")
    println("⏱️  開始時刻: ${LocalDateTime.now()}")
    println()

    // JVM Warmup
    println("☕ JVMウォームアップ中...")
    repeat(BenchConfig.JVM_WARMUP_ITERATIONS) {
        System.gc()
        Thread.sleep(100)
    }

    when (benchmarkType.lowercase()) {
        "all" -> {
            WriteWalGroupBenchmark().run()
            println("📊 Pool stats: ${Pools.stats()}")
            WriteScalabilityBenchmark().run()
            println("📊 Pool stats: ${Pools.stats()}")
            WriteValueSizeBenchmark().run()
            println("📊 Pool stats: ${Pools.stats()}")
            ReadBenchmark().run()
            println("📊 Pool stats: ${Pools.stats()}")
            BloomFilterBenchmark().run()
            println("📊 Pool stats: ${Pools.stats()}")
            RangeScanBenchmark().run()
            println("📊 Pool stats: ${Pools.stats()}")
            MixedWorkloadBenchmark().run()
            println("📊 Pool stats: ${Pools.stats()}")
            MultiThreadBenchmark().run()
            println("📊 Pool stats: ${Pools.stats()}")
            TypedApiBenchmark().run()
        }

        "write" -> {
            WriteWalGroupBenchmark().run()
            WriteScalabilityBenchmark().run()
            WriteValueSizeBenchmark().run()
        }

        "read" -> {
            ReadBenchmark().run()
        }

        "bloom" -> {
            BloomFilterBenchmark().run()
        }

        "range" -> {
            RangeScanBenchmark().run()
        }

        "mixed" -> {
            MixedWorkloadBenchmark().run()
        }

        "mt" -> {
            MultiThreadBenchmark().run()
        }

        "typed" -> {
            TypedApiBenchmark().run()
        }

        else -> {
            println("Unknown benchmark type: $benchmarkType")
            println("Available: all, write, read, bloom, range, mixed, mt, typed")
        }
    }

    println("\n✅ ベンチマーク完了")
    println("⏱️  終了時刻: ${LocalDateTime.now()}")
}