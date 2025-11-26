package dev.swiftstorm.akkaradb.plugin.compiler

import org.jetbrains.kotlin.compiler.plugin.AbstractCliOption
import org.jetbrains.kotlin.compiler.plugin.CommandLineProcessor
import org.jetbrains.kotlin.compiler.plugin.ExperimentalCompilerApi

@OptIn(ExperimentalCompilerApi::class)
class AkkCommandLineProcessor : CommandLineProcessor {
    override val pluginId: String
        get() = "dev.swiftstorm.akkaradb.plugin.compiler"
    override val pluginOptions: Collection<AbstractCliOption>
        get() = emptyList()
}