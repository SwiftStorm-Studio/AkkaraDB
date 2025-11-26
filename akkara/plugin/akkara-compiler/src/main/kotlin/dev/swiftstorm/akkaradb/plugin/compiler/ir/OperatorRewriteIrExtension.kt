@file:Suppress("unused")

package dev.swiftstorm.akkaradb.plugin.compiler.ir

import org.jetbrains.kotlin.backend.common.extensions.IrGenerationExtension
import org.jetbrains.kotlin.backend.common.extensions.IrPluginContext
import org.jetbrains.kotlin.ir.declarations.IrModuleFragment
import org.jetbrains.kotlin.ir.visitors.transformChildrenVoid

class OperatorRewriteIrExtension : IrGenerationExtension {
    override fun generate(moduleFragment: IrModuleFragment, pluginContext: IrPluginContext) {
        println("[AkkaraDB] Running Operator Rewrite IR Extension")
        moduleFragment.transformChildrenVoid(QueryCallRewriter(pluginContext))
    }
}