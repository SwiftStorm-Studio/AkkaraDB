package dev.swiftstorm.akkara.bench

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.text.DecimalFormat
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max
import kotlin.math.min

class ThroughputBenchmarkTest {

    @Test
    @DisplayName("Synthetic throughput benchmark prints aggregated throughput")
    fun syntheticThroughputBenchmark() {
        val threadCount = min(max(1, Runtime.getRuntime().availableProcessors()), 4)
        val result = runSyntheticBenchmark(
            threadCount = threadCount,
            warmupSeconds = 1,
            measurementSeconds = 2
        )

        println(result.toPrettyString())
        assertTrue(result.operations > 0, "Benchmark did not record any operations")
    }

    private fun runSyntheticBenchmark(
        threadCount: Int,
        warmupSeconds: Long,
        measurementSeconds: Long
    ): BenchmarkResult {
        require(threadCount > 0) { "threadCount must be positive" }
        require(warmupSeconds >= 0) { "warmupSeconds cannot be negative" }
        require(measurementSeconds > 0) { "measurementSeconds must be positive" }

        val warmupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(warmupSeconds)
        val measurementDeadline = warmupDeadline + TimeUnit.SECONDS.toNanos(measurementSeconds)

        val executor = Executors.newFixedThreadPool(threadCount)
        val measuredOperations = AtomicLong(0)
        val sink = AtomicLong(0)

        repeat(threadCount) { threadIndex ->
            executor.submit {
                var localOps = 0L
                var state = (threadIndex + 1).toLong()

                while (true) {
                    state = scramble(state)
                    val now = System.nanoTime()

                    if (now >= measurementDeadline) {
                        sink.addAndGet(state)
                        measuredOperations.addAndGet(localOps)
                        break
                    }

                    if (now >= warmupDeadline) {
                        localOps++
                    }
                }
            }
        }

        executor.shutdown()
        val terminated = executor.awaitTermination(
            warmupSeconds + measurementSeconds + 1,
            TimeUnit.SECONDS
        )
        if (!terminated) {
            executor.shutdownNow()
        }

        val operations = measuredOperations.get()
        val measurementSecondsDouble = measurementSeconds.toDouble()
        val throughput = if (measurementSecondsDouble == 0.0) 0.0 else operations / measurementSecondsDouble
        val perThread = throughput / threadCount

        return BenchmarkResult(
            threadCount = threadCount,
            warmupSeconds = warmupSeconds,
            measurementSeconds = measurementSeconds,
            operations = operations,
            operationsPerSecond = throughput,
            operationsPerSecondPerThread = perThread,
            sink = sink.get()
        )
    }

    private fun scramble(value: Long): Long {
        var v = value
        v = v xor (v shl 13)
        v = v xor (v ushr 7)
        v = v xor (v shl 17)
        return v
    }

    private data class BenchmarkResult(
        val threadCount: Int,
        val warmupSeconds: Long,
        val measurementSeconds: Long,
        val operations: Long,
        val operationsPerSecond: Double,
        val operationsPerSecondPerThread: Double,
        val sink: Long
    ) {
        fun toPrettyString(): String {
            val integerFormat = DecimalFormat("#,###")
            val decimalFormat = DecimalFormat("#,###.00")
            return buildString {
                appendLine("Synthetic throughput benchmark (hot loop workload)")
                appendLine(" Threads                 : ${integerFormat.format(threadCount)}")
                appendLine(" Warm-up duration (s)    : ${integerFormat.format(warmupSeconds)}")
                appendLine(" Measurement duration (s): ${integerFormat.format(measurementSeconds)}")
                appendLine(" Total operations        : ${integerFormat.format(operations)}")
                appendLine(" Throughput (ops/s)      : ${decimalFormat.format(operationsPerSecond)}")
                appendLine(" Per-thread throughput   : ${decimalFormat.format(operationsPerSecondPerThread)}")
                append(" Final sink value        : ${integerFormat.format(sink)}")
            }
        }
    }
}
