package dev.swiftstorm.akkaradb.plugin.compiler

import dev.swiftstorm.akkaradb.plugin.compiler.ir.OperatorRewriteIrExtension
import org.jetbrains.kotlin.backend.common.extensions.IrGenerationExtension
import org.jetbrains.kotlin.compiler.plugin.CompilerPluginRegistrar
import org.jetbrains.kotlin.compiler.plugin.ExperimentalCompilerApi
import org.jetbrains.kotlin.config.CompilerConfiguration

@OptIn(ExperimentalCompilerApi::class)
class AkkRegisterer : CompilerPluginRegistrar() {
    override val supportsK2: Boolean = true
    override fun ExtensionStorage.registerExtensions(configuration: CompilerConfiguration) {
        println("[AkkaraDB] Registering IR generation extension")
        IrGenerationExtension.registerExtension(
            OperatorRewriteIrExtension()
        )
    }
}