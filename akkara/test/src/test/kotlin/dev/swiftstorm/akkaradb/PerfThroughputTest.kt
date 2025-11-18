package dev.swiftstorm.akkaradb

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.engine.AkkaraDB
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi

/**
 * Simple microbench-style throughput/latency test using JUnit for convenience.
 * This test is disabled by default. Run with JUnit including @Tag("perf") or manually enable.
 */
@Tag("perf")
//@Disabled("Performance microbench; enable locally when needed")
class PerfThroughputTest {

    var temp: Path = Path.of("./test/")

    private fun bbAscii(s: String): ByteBufferL {
        //println("Generating key: '$s' (len=${s.length})")
        val bytes = s.toByteArray(StandardCharsets.US_ASCII)
        //println("Byte array: ${bytes.joinToString(" ") { String.format("%02x", it) }}")

        val bb = ByteBufferL.allocate(bytes.size)
        //println("Allocated buffer (size=${bb.capacity}): ${bb.debugString()}")

        bb.clear()

        bb.putBytes(bytes)
        //println("After putBytes: ${bb.debugString()}")

        bb.position(0)
        //println("After position(0): ${bb.debugString()}")

        return bb
    }

    private fun bbPayload(size: Int, seed: Int): ByteBufferL {
        val arr = ByteArray(size) { i -> ((i * 1315423911 + seed) ushr 13).toByte() }
        return ByteBufferL.wrap(java.nio.ByteBuffer.wrap(arr))
    }

    private fun percentileNanos(ns: LongArray, p: Double): Long {
        ns.sort()
        val idx = (p * (ns.size - 1)).coerceIn(0.0, (ns.size - 1).toDouble())
        return ns[idx.toInt()]
    }

    private fun nanosToMicros(n: Long): Double = n.toDouble() / 1_000.0

    @Test
    fun writeThroughputAndLatency() {
        val N = System.getProperty("akk.perf.N")?.toIntOrNull() ?: 100_000
        val valueSize = System.getProperty("akk.perf.valueSize")?.toIntOrNull() ?: 64

        val base = temp.resolve("perf-write")

        val db = AkkaraDB.open(
            AkkaraDB.Options(
                baseDir = base,
//                k = 4,
//                m = 1,
                fastMode = true,
                walGroupN = 512,
                walGroupMicros = 50_000,
            )
        )

        val lat = LongArray(N)
        val t0 = System.nanoTime()
        for (i in 0 until N) {
            val k = bbAscii("k%08d".format(i))
            val v = bbPayload(valueSize, i)
            val s = System.nanoTime()
            db.put(k, v)
            lat[i] = System.nanoTime() - s
        }
        db.flush()
        val t1 = System.nanoTime()
        db.close()

        val totalNs = t1 - t0
        val opsPerSec = (N.toDouble() * 1e9) / totalNs.toDouble()
        val p50 = percentileNanos(lat.copyOf(), 0.50)
        val p90 = percentileNanos(lat.copyOf(), 0.90)
        val p99 = percentileNanos(lat.copyOf(), 0.99)

        println(
            "{" +
                    "\"bench\":\"write\"," +
                    "\"N\":$N," +
                    "\"valueSize\":$valueSize," +
                    "\"opsPerSec\":${"%.2f".format(opsPerSec)}," +
                    "\"p50_us\":${"%.2f".format(nanosToMicros(p50))}," +
                    "\"p90_us\":${"%.2f".format(nanosToMicros(p90))}," +
                    "\"p99_us\":${"%.2f".format(nanosToMicros(p99))}" +
                    "}"
        )

        base.toFile().deleteRecursively()
    }

    @Test
    fun readThroughputAndLatency() {
        val N = System.getProperty("akk.perf.N")?.toIntOrNull() ?: 100_000
        val valueSize = System.getProperty("akk.perf.valueSize")?.toIntOrNull() ?: 64

        val base = temp.resolve("perf-read")
        val db = AkkaraDB.open(
            AkkaraDB.Options(
                baseDir = base,
//                k = 4,
//                m = 1,
                fastMode = true,
                walGroupN = 512,
                walGroupMicros = 50_000,
            )
        )

        // preload
        for (i in 0 until N) {
            db.put(bbAscii("k%08d".format(i)), bbPayload(valueSize, i))
        }

        // optional: flush once to persist (if you want realistic read test)
        db.flush()

        // read benchmark
        val lat = LongArray(N)
        val t0 = System.nanoTime()
        for (i in 0 until N) {
            val k = bbAscii("k%08d".format(i))
            val s = System.nanoTime()
            val v = db.get(k)
            lat[i] = System.nanoTime() - s
            if (v == null) throw AssertionError("missing value for key $i")
        }
        val t1 = System.nanoTime()
        db.close()

        val totalNs = t1 - t0
        val opsPerSec = (N.toDouble() * 1e9) / totalNs.toDouble()
        val p50 = percentileNanos(lat.copyOf(), 0.50)
        val p90 = percentileNanos(lat.copyOf(), 0.90)
        val p99 = percentileNanos(lat.copyOf(), 0.99)

        println(
            "{" +
                    "\"bench\":\"read\"," +
                    "\"N\":$N," +
                    "\"valueSize\":$valueSize," +
                    "\"opsPerSec\":${"%.2f".format(opsPerSec)}," +
                    "\"p50_us\":${"%.2f".format(nanosToMicros(p50))}," +
                    "\"p90_us\":${"%.2f".format(nanosToMicros(p90))}," +
                    "\"p99_us\":${"%.2f".format(nanosToMicros(p99))}" +
                    "}"
        )

        base.toFile().deleteRecursively()
    }

    @OptIn(ExperimentalPathApi::class)
    @Test
    fun readFromSSTable() {
        try {
            val N = System.getProperty("akk.perf.N")?.toIntOrNull() ?: 100000
            val valueSize = System.getProperty("akk.perf.valueSize")?.toIntOrNull() ?: 64

            val base = temp.resolve("perf-sst")
            val insertedKeys = mutableListOf<ByteBufferL>()
            run {
                val db = AkkaraDB.open(
                    AkkaraDB.Options(
                        baseDir = base,
                        fastMode = true,
                        walGroupN = 512,
                        walGroupMicros = 50_000,
                    )
                )
                for (i in 0 until N) {
                    val key = "k${i.toString().padStart(8, '0')}"
                    //println("Inserting key: $key")

                    val k = bbAscii(key)
                    //println("Buffered key: ${k.debugString()}")

                    val payload = bbPayload(valueSize, i)
                    //println("Inserting value for key $key: ${payload.debugString()}")

                    db.put(k, payload)
                    insertedKeys.add(k)  // 挿入したキーを保存
                }
                db.close()
            }

            // データベースからの読み取り
            val db = AkkaraDB.open(
                AkkaraDB.Options(
                    baseDir = base,
                    fastMode = true,
                    walGroupN = 512,
                    walGroupMicros = 50_000,
                )
            )

            val lat = LongArray(N)
            val t0 = System.nanoTime()
            for (i in 0 until N) {
                val k = insertedKeys[i]
                val s = System.nanoTime()
                val v = db.get(k)
                lat[i] = System.nanoTime() - s
                if (v == null) throw AssertionError("missing value for key ${i}")
            }
            val t1 = System.nanoTime()
            db.close()

            val totalNs = t1 - t0
            val opsPerSec = (N.toDouble() * 1e9) / totalNs.toDouble()
            val p50 = percentileNanos(lat.copyOf(), 0.50)
            val p90 = percentileNanos(lat.copyOf(), 0.90)
            val p99 = percentileNanos(lat.copyOf(), 0.99)

            println(
                "{" +
                        "\"bench\":\"read-sst\"," +
                        "\"N\":$N," +
                        "\"valueSize\":$valueSize," +
                        "\"opsPerSec\":${"%.2f".format(opsPerSec)}," +
                        "\"p50_us\":${"%.2f".format(nanosToMicros(p50))}," +
                        "\"p90_us\":${"%.2f".format(nanosToMicros(p90))}," +
                        "\"p99_us\":${"%.2f".format(nanosToMicros(p99))}" +
                        "}"
            )

            //base.deleteIfExists()
        } catch (e: Throwable) {
            throw Exception("Failed with: $e", e)
        }
    }
}