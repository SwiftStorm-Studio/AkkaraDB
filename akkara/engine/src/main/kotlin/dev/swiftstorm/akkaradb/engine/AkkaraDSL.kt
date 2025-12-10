/*
 * AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

@file:Suppress("unused", "NOTHING_TO_INLINE")

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.format.akk.parity.RSParityCoder
import dev.swiftstorm.akkaradb.format.api.FlushPolicy
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.file.Path

/**
 * Utility object providing factory methods for working with AkkaraDB in a type-safe and DSL-friendly manner.
 * Offers functionality to open and configure packed tables using a flexible DSL approach.
 */
object AkkDSL {
    // factory helpers
    inline fun <reified T : Any, reified ID : Any> open(baseDir: Path, configure: AkkDSLCfgBuilder.() -> Unit = {}): PackedTable<T, ID> {
        val cfg = AkkDSLCfgBuilder(baseDir).apply(configure).build()
        return open<T, ID>(cfg)
    }

    inline fun <reified T : Any, reified ID : Any> open(
        baseDir: Path,
        mode: StartupMode,
        noinline customize: AkkDSLCfgBuilder.() -> Unit = {}
    ): PackedTable<T, ID> = open<T, ID>(AkkaraPresets.of(baseDir, mode, customize))

    inline fun <reified T : Any, reified ID : Any> open(cfg: AkkDSLCfg): PackedTable<T, ID> {
        val opts = AkkaraDB.Options(
            baseDir = cfg.baseDir,
            k = cfg.k,
            m = cfg.m,
            flushPolicy = FlushPolicy(maxBlocks = cfg.flushMaxBlocks, maxMicros = cfg.flushMaxMicros),
            walFastMode = cfg.walFastMode,
            stripeFastMode = cfg.stripeFastMode,
            walGroupN = cfg.walGroupN,
            walGroupMicros = cfg.walGroupMicros,
            parityCoder = cfg.parityCoder,
            durableCas = cfg.durableCas,
            useStripeForRead = cfg.useStripeForRead,
            bloomFPRate = cfg.bloomFPRate,
            debug = cfg.debug
        )
        val db = AkkaraDB.open(opts)
        return PackedTable(db, T::class, ID::class)
    }

    inline fun <reified T : Any, reified ID : Any> AkkaraDB.open(): PackedTable<T, ID> = PackedTable(this, T::class, ID::class)
}

// ---- configuration (v3-focused, minimal) ----

data class AkkDSLCfg(
    val baseDir: Path,
    val k: Int = 4,
    val m: Int = 2,
    val flushMaxBlocks: Int = 32,
    val flushMaxMicros: Long = 500,
    val walFastMode: Boolean = true,
    val stripeFastMode: Boolean = true,
    val walGroupN: Int = 32,
    val walGroupMicros: Long = 500,
    val parityCoder: ParityCoder? = null,
    val durableCas: Boolean = false,
    val useStripeForRead: Boolean = false,
    val bloomFPRate: Double = 0.01,
    val debug: Boolean = false
)

class AkkDSLCfgBuilder(private val baseDir: Path) {
    var k: Int = 4
    var m: Int = 2
    var flushMaxBlocks: Int = 32
    var flushMaxMicros: Long = 500
    var walFastMode: Boolean = true
    var stripeFastMode: Boolean = true
    var walGroupN: Int = 32
    var walGroupMicros: Long = 500
    var parityCoder: ParityCoder? = null
    var durableCas: Boolean = false
    var useStripeForRead: Boolean = false
    var bloomFPRate: Double = 0.01
    var debug: Boolean = false

    fun build(): AkkDSLCfg {
        require(k >= 1) { "k must be >= 1" }
        require(m >= 0) { "m must be >= 0" }
        require(flushMaxBlocks >= 1) { "flushMaxBlocks must be >= 1" }
        require(flushMaxMicros >= 0) { "flushMaxMicros must be >= 0" }
        require(walGroupN >= 1) { "walGroupN must be >= 1" }
        require(walGroupMicros >= 0) { "walGroupMicros must be >= 0" }
        require(bloomFPRate in 1e-9..0.5) { "bloomFPRate must be in [1e-9, 0.5]" }
        return AkkDSLCfg(
            baseDir,
            k,
            m,
            flushMaxBlocks,
            flushMaxMicros,
            walFastMode,
            stripeFastMode,
            walGroupN,
            walGroupMicros,
            parityCoder,
            durableCas,
            useStripeForRead,
            bloomFPRate,
            debug
        )
    }
}

enum class StartupMode { ULTRA_FAST, FAST, NORMAL, DURABLE, CUSTOM }

object AkkaraPresets {
    fun of(baseDir: Path, mode: StartupMode, customize: AkkDSLCfgBuilder.() -> Unit = {}): AkkDSLCfg {
        val b = AkkDSLCfgBuilder(baseDir)
        when (mode) {
            StartupMode.ULTRA_FAST -> b.configureUltraFast()
            StartupMode.FAST -> b.configureFast()
            StartupMode.NORMAL -> b.configureNormal()
            StartupMode.DURABLE -> b.configureDurable()
            StartupMode.CUSTOM -> {}
        }
        b.apply(customize)
        return b.build()
    }
}

private fun AkkDSLCfgBuilder.configureDurable() {
    k = 4; m = 2
    walFastMode = false
    walGroupN = 1
    walGroupMicros = 0
    flushMaxBlocks = 32
    flushMaxMicros = 500
    parityCoder = RSParityCoder(2)
    durableCas = true
}

private fun AkkDSLCfgBuilder.configureNormal() {
    k = 4; m = 2
    walFastMode = true
    walGroupN = 64
    walGroupMicros = 1000
    flushMaxBlocks = 64
    flushMaxMicros = 1000
    parityCoder = RSParityCoder(2)
}

private fun AkkDSLCfgBuilder.configureFast() {
    k = 4; m = 1
    walFastMode = true
    walGroupN = 256
    walGroupMicros = 12_000
    flushMaxBlocks = 256
    flushMaxMicros = 2_000
    parityCoder = RSParityCoder(1)
}

private fun AkkDSLCfgBuilder.configureUltraFast() {
    k = 4; m = 1
    walFastMode = true
    walGroupN = 512
    walGroupMicros = 50_000
    flushMaxBlocks = 512
    flushMaxMicros = 50_000
    parityCoder = RSParityCoder(1)
}

// ---- annotations ----

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Required

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class NonEmpty

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Positive