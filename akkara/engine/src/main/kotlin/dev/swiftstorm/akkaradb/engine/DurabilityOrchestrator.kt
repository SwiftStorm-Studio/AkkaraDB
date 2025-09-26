package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import dev.swiftstorm.akkaradb.engine.wal.WalWriter
import dev.swiftstorm.akkaradb.format.akk.AkkStripeWriter

class DurabilityOrchestrator(
    private val stripe: AkkStripeWriter?,   // expects: fsyncAll(onlyPageCache: Boolean)
    private val wal: WalWriter?,            // expects: fsync()
    private val manifest: AkkManifest?,     // expects: fsync()
    periodMs: Long,                         // 呼び出し周期（例: 1200）
    private val minIntervalMs: Long = 200,  // これ未満の連続fsyncを抑制
    private val jitterPct: Double = 0.10,   // ±10%ジッタ（0で無効）
) {
    @Volatile
    private var running = true
    @Volatile
    private var lastSyncAtNs = 0L
    private val inProgress = java.util.concurrent.atomic.AtomicBoolean(false)

    private val basePeriodMs = periodMs.coerceAtLeast(1)

    private val t = Thread({ loop() }, "akk-durability-orch").apply {
        isDaemon = true
        start()
    }

    private fun loop() {
        val rnd = java.util.concurrent.ThreadLocalRandom.current()
        while (running) {
            val now = System.nanoTime()
            val sinceMs = if (lastSyncAtNs == 0L) Long.MAX_VALUE else (now - lastSyncAtNs) / 1_000_000
            if (sinceMs >= minIntervalMs && inProgress.compareAndSet(false, true)) {
                try {
                    stripe?.fsyncAll()
                    wal?.fsync()
                    manifest?.fsync()
                    lastSyncAtNs = System.nanoTime()
                } catch (_: Throwable) {
                } finally {
                    inProgress.set(false)
                }
            }

            val jitter = if (jitterPct > 0.0)
                (basePeriodMs * jitterPct * rnd.nextDouble(-1.0, 1.0)).toLong()
            else 0L
            try {
                Thread.sleep((basePeriodMs + jitter).coerceAtLeast(1))
            } catch (_: InterruptedException) {
                break
            }
        }
    }

    fun stop() {
        running = false
        t.interrupt()
    }
}
