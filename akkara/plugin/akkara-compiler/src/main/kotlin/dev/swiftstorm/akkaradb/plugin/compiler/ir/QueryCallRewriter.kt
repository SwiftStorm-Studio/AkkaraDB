@file:OptIn(UnsafeDuringIrConstructionAPI::class)

package dev.swiftstorm.akkaradb.plugin.compiler.ir

import org.jetbrains.kotlin.backend.common.extensions.IrPluginContext
import org.jetbrains.kotlin.ir.UNDEFINED_OFFSET
import org.jetbrains.kotlin.ir.declarations.IrEnumEntry
import org.jetbrains.kotlin.ir.declarations.IrFunction
import org.jetbrains.kotlin.ir.declarations.IrParameterKind
import org.jetbrains.kotlin.ir.declarations.IrSimpleFunction
import org.jetbrains.kotlin.ir.expressions.*
import org.jetbrains.kotlin.ir.expressions.impl.*
import org.jetbrains.kotlin.ir.symbols.*
import org.jetbrains.kotlin.ir.types.IrType
import org.jetbrains.kotlin.ir.types.classFqName
import org.jetbrains.kotlin.ir.types.classifierOrFail
import org.jetbrains.kotlin.ir.types.makeNullable
import org.jetbrains.kotlin.ir.util.*
import org.jetbrains.kotlin.ir.visitors.IrElementTransformerVoid
import org.jetbrains.kotlin.name.CallableId
import org.jetbrains.kotlin.name.ClassId
import org.jetbrains.kotlin.name.FqName
import org.jetbrains.kotlin.name.Name
import org.jetbrains.kotlin.util.OperatorNameConventions

class QueryCallRewriter(
    private val ctx: IrPluginContext
) : IrElementTransformerVoid() {

    private val fqAkkQuery = FqName("dev.swiftstorm.akkaradb.engine.query.AkkQuery")
    private val fqPackedTable = FqName("dev.swiftstorm.akkaradb.engine.PackedTable")
    private val fqExpr = FqName("dev.swiftstorm.akkaradb.engine.query.AkkExpr")
    private val fqCol = FqName("dev.swiftstorm.akkaradb.engine.query.AkkCol")
    private val fqLit = FqName("dev.swiftstorm.akkaradb.engine.query.AkkLit")
    private val fqBin = FqName("dev.swiftstorm.akkaradb.engine.query.AkkBin")
    private val fqUn = FqName("dev.swiftstorm.akkaradb.engine.query.AkkUn")
    private val fqOp = FqName("dev.swiftstorm.akkaradb.engine.query.AkkOp")

    private val symRunQ: IrSimpleFunctionSymbol by lazy {
        val cls = ctx.referenceClass(ClassId.topLevel(fqPackedTable))
            ?: error("PackedTable not found")

        cls.owner.declarations
            .filterIsInstance<IrSimpleFunction>()
            .firstOrNull { fn ->
                fn.name.asString() == "runQ" &&

                        fn.parameters.any { it.kind == IrParameterKind.DispatchReceiver } &&

                        fn.parameters.count { it.kind == IrParameterKind.Regular } == 1 &&

                        fn.parameters.first { it.kind == IrParameterKind.Regular }
                            .type.classFqName?.asString() == "dev.swiftstorm.akkaradb.engine.query.AkkQuery"
            }
            ?.symbol
            ?: error("PackedTable.runQ not found")
    }

    private val symAny: IrSimpleFunctionSymbol by lazy {
        ctx.referenceFunctions(
            CallableId(
                FqName("kotlin.sequences"),
                Name.identifier("any")
            )
        ).firstOrNull { fn ->
            val params = fn.owner.parameters

            val hasExtension =
                params.any { it.kind == IrParameterKind.ExtensionReceiver }

            val regularCount =
                params.count { it.kind == IrParameterKind.Regular || it.kind == IrParameterKind.Context }

            hasExtension && regularCount == 0
        } ?: error("Sequence<T>.any() not found")
    }

    private val symFirstOrNull: IrSimpleFunctionSymbol by lazy {
        ctx.referenceFunctions(
            CallableId(
                FqName("kotlin.sequences"),
                Name.identifier("firstOrNull")
            )
        ).firstOrNull { fn ->
            val params = fn.owner.parameters

            val hasExtension =
                params.any { it.kind == IrParameterKind.ExtensionReceiver }

            val regularCount =
                params.count { it.kind == IrParameterKind.Regular || it.kind == IrParameterKind.Context }

            hasExtension && regularCount == 0
        } ?: error("Sequence<T>.firstOrNull() not found")
    }

    private val symRunToList: IrSimpleFunctionSymbol by lazy {
        ctx.referenceFunctions(
            CallableId(
                FqName("kotlin.sequences"),
                Name.identifier("toList")
            )
        ).firstOrNull { fn ->
            val params = fn.owner.parameters

            val hasExtension =
                params.any { it.kind == IrParameterKind.ExtensionReceiver }

            val regularCount =
                params.count { it.kind == IrParameterKind.Regular || it.kind == IrParameterKind.Context }

            hasExtension && regularCount == 0
        } ?: error("Sequence<T>.toList() not found")
    }

    private val symAkkOp by lazy { ctx.referenceClass(ClassId.topLevel(fqOp))!! }
    private val symAkkQuery by lazy { primaryCtor(fqAkkQuery) }
    private val symLit by lazy { primaryCtor(fqLit) }
    private val symBin by lazy { primaryCtor(fqBin) }
    private val symUn by lazy { primaryCtor(fqUn) }
    private val symCol by lazy { primaryCtor(fqCol) }

    private val opEQ by lazy { enumEntry("EQ") }
    private val opNEQ by lazy { enumEntry("NEQ") }
    private val opGT by lazy { enumEntry("GT") }
    private val opGE by lazy { enumEntry("GE") }
    private val opLT by lazy { enumEntry("LT") }
    private val opLE by lazy { enumEntry("LE") }
    private val opNOT by lazy { enumEntry("NOT") }
    private val opIS_NULL by lazy { enumEntry("IS_NULL") }
    private val opIS_NOT_NULL by lazy { enumEntry("IS_NOT_NULL") }
    private val opAND by lazy { enumEntry("AND") }
    private val opOR by lazy { enumEntry("OR") }
    private val opIN by lazy { enumEntry("IN") }
    private val opNOT_IN by lazy { enumEntry("NOT_IN") }

    override fun visitCall(expression: IrCall): IrExpression {
        println("Akkara: visiting call to ${expression.symbol.owner.name.asString()}")
        val owner = expression.symbol.owner

        // FQ name-based check
        val fqName = owner.parent.kotlinFqName.asString()

        return if (fqName == "dev.swiftstorm.akkaradb.engine.PackedTable") {
            println("Akkara: rewriting call to ${expression.symbol.owner.name.asString()}")
            when (expression.symbol.owner.name.asString()) {
                "query" -> rewriteQuery(expression)
                "exists" -> rewriteExists(expression)
                "firstOrNull" -> rewriteFirstOrNull(expression)
                "runToList" -> rewriteRunToList(expression)
                else -> super.visitCall(expression)
            }
        } else {
            super.visitCall(expression)
        }
    }

    private fun rewriteQuery(call: IrCall): IrExpression {
        val callee = call.symbol.owner

        val lambdaParam = callee.parameters.lastOrNull { param ->
            param.kind == IrParameterKind.Regular || param.kind == IrParameterKind.Context || param.kind == IrParameterKind.ExtensionReceiver
        } ?: return call

        val lambdaIndex = lambdaParam.indexInParameters

        val lambdaArg = call.arguments.getOrNull(lambdaIndex) as? IrFunctionExpression ?: return call
        val fn = lambdaArg.function

        val returnedExpr = findLambdaReturnExpr(fn) ?: return call

        val akkWhereExpr = rewriteExpr(returnedExpr)

        return IrConstructorCallImpl.fromSymbolOwner(
            call.startOffset,
            call.endOffset,
            symAkkQuery.owner.returnType,
            symAkkQuery
        ).apply {
            arguments[0] = akkWhereExpr
        }
    }

    private fun rewriteExists(call: IrCall): IrExpression {
        // Step 1: rewrite the AkkQuery expression
        val akkQueryExpr = rewriteQuery(call)

        // Step 2: Create runQ(...) call
        val runQCall = IrCallImpl.fromSymbolOwner(
            startOffset = call.startOffset,
            endOffset = call.endOffset,
            type = symRunQ.owner.returnType,
            symbol = symRunQ
        ).apply {
            // --- K2-safe argument mapping ---

            val params = symRunQ.owner.parameters

            // dispatch receiver index
            val dispatchIndex = params.indexOfFirst { it == symRunQ.owner.dispatchReceiverParameter }
            if (dispatchIndex >= 0) {
                arguments[dispatchIndex] = call.dispatchReceiver
                    ?: error("exists() call had no dispatch receiver")
            }

            // value parameter (AkkQuery) index
            val queryIndex = params.indexOfFirst { p ->
                p.type.classFqName?.asString() ==
                        "dev.swiftstorm.akkaradb.engine.query.AkkQuery"
            }
            if (queryIndex >= 0) {
                arguments[queryIndex] = akkQueryExpr
            }
        }

        // Step 3: Sequence<T>.any()
        return IrCallImpl(
            startOffset = call.startOffset,
            endOffset = call.endOffset,
            type = ctx.irBuiltIns.booleanType,
            symbol = symAny
        ).apply {
            val params = symAny.owner.parameters

            // extension receiver slot: Sequence<T>
            val extIndex = params.indexOfFirst { it.kind == IrParameterKind.ExtensionReceiver }
            if (extIndex >= 0) {
                arguments[extIndex] = runQCall
            }
        }
    }

    private fun rewriteFirstOrNull(call: IrCall): IrExpression {
        val akkQueryExpr = rewriteQuery(call)

        val runQCall = IrCallImpl.fromSymbolOwner(
            startOffset = call.startOffset,
            endOffset = call.endOffset,
            type = symRunQ.owner.returnType,
            symbol = symRunQ
        ).apply {
            val params = symRunQ.owner.parameters

            val dispatchIndex = params.indexOfFirst { it == symRunQ.owner.dispatchReceiverParameter }
            if (dispatchIndex >= 0) {
                arguments[dispatchIndex] = call.dispatchReceiver
                    ?: error("firstOrNull() call had no dispatch receiver")
            }

            val queryIndex = params.indexOfFirst { p ->
                p.type.classFqName?.asString() ==
                        "dev.swiftstorm.akkaradb.engine.query.AkkQuery"
            }
            if (queryIndex >= 0) {
                arguments[queryIndex] = akkQueryExpr
            }
        }

        return IrCallImpl(
            startOffset = call.startOffset,
            endOffset = call.endOffset,
            type = call.type,
            symbol = symFirstOrNull
        ).apply {
            val params = symFirstOrNull.owner.parameters

            val extIndex = params.indexOfFirst { it.kind == IrParameterKind.ExtensionReceiver }
            if (extIndex >= 0) {
                arguments[extIndex] = runQCall
            }
        }
    }

    private fun rewriteRunToList(call: IrCall): IrExpression {
        val akkQueryExpr = rewriteQuery(call)

        val runQCall = IrCallImpl.fromSymbolOwner(
            startOffset = call.startOffset,
            endOffset = call.endOffset,
            type = symRunQ.owner.returnType,
            symbol = symRunQ
        ).apply {
            val params = symRunQ.owner.parameters

            val dispatchIndex = params.indexOfFirst { it == symRunQ.owner.dispatchReceiverParameter }
            if (dispatchIndex >= 0) {
                arguments[dispatchIndex] = call.dispatchReceiver
                    ?: error("runToList() call had no dispatch receiver")
            }

            val queryIndex = params.indexOfFirst { p ->
                p.type.classFqName?.asString() ==
                        "dev.swiftstorm.akkaradb.engine.query.AkkQuery"
            }
            if (queryIndex >= 0) {
                arguments[queryIndex] = akkQueryExpr
            }
        }

        return IrCallImpl(
            startOffset = call.startOffset,
            endOffset = call.endOffset,
            type = call.type,
            symbol = symRunToList
        ).apply {
            val params = symRunToList.owner.parameters

            val extIndex = params.indexOfFirst { it.kind == IrParameterKind.ExtensionReceiver }
            if (extIndex >= 0) {
                arguments[extIndex] = runQCall
            }
        }
    }

    private fun rewriteExpr(expr: IrExpression): IrExpression {
        // Already an AkkExpr -> just rewrite children
        if (expr.type.isAkkExprType()) {
            expr.transformChildren(thisAsTransformer(), null)
            return expr
        }

        // Plain literal
        if (expr is IrConst) return lit(expr)

        when (expr) {
            // Field access -> column
            is IrGetField -> {
                val name = expr.symbol.owner.name.asString()
                return col(name, expr.type, expr)
            }

            is IrCall -> {
                val sym = expr.symbol

                // Property getter call -> column
                if (sym.owner.isGetter) {
                    val propName = sym.owner.correspondingPropertySymbol
                        ?.owner
                        ?.name
                        ?.asString()
                        ?: sym.owner.name.asString()

                    return col(propName, expr.type, expr)
                }

                val lhsRaw = expr.arguments.getOrNull(0)
                val rhsRaw = expr.arguments.getOrNull(1)

                // ===== Equality (==) =====
                val isEq = (sym == ctx.irBuiltIns.eqeqSymbol ||
                        sym == ctx.irBuiltIns.eqeqeqSymbol ||
                        sym in ctx.irBuiltIns.ieee754equalsFunByOperandType.values)

                if (isEq && lhsRaw != null && rhsRaw != null) {
                    val lhs = rewriteExpr(lhsRaw)
                    val rhs = rewriteExpr(rhsRaw)

                    if (lhsRaw.isNullConst()) return un(opIS_NULL, rhs)
                    if (rhsRaw.isNullConst()) return un(opIS_NULL, lhs)

                    return bin(opEQ, lhs, rhs)
                }

                // ===== Comparisons (<, <=, >, >=) =====
                fun cmp(
                    map: Map<IrClassifierSymbol, IrSimpleFunctionSymbol>,
                    op: IrEnumEntrySymbol
                ): IrExpression? {
                    val recvRaw = expr.dispatchReceiver ?: return null
                    val expected = map[recvRaw.type.classifierOrFail] ?: return null
                    if (sym != expected) return null
                    val argRaw = expr.arguments.getOrNull(0) ?: return null
                    return bin(op, rewriteExpr(recvRaw), rewriteExpr(argRaw))
                }

                cmp(ctx.irBuiltIns.lessFunByOperandType, opLT)?.let { return it }
                cmp(ctx.irBuiltIns.lessOrEqualFunByOperandType, opLE)?.let { return it }
                cmp(ctx.irBuiltIns.greaterFunByOperandType, opGT)?.let { return it }
                cmp(ctx.irBuiltIns.greaterOrEqualFunByOperandType, opGE)?.let { return it }

                // ===== && / || =====
                if (sym == ctx.irBuiltIns.andandSymbol && lhsRaw != null && rhsRaw != null) {
                    return bin(opAND, rewriteExpr(lhsRaw), rewriteExpr(rhsRaw))
                }
                if (sym == ctx.irBuiltIns.ororSymbol && lhsRaw != null && rhsRaw != null) {
                    return bin(opOR, rewriteExpr(lhsRaw), rewriteExpr(rhsRaw))
                }

                // ===== in (desugared to .contains) =====
                if (sym.owner.name == OperatorNameConventions.CONTAINS) {
                    val recvRaw = expr.dispatchReceiver ?: return expr
                    val argRaw = expr.arguments.getOrNull(0) ?: return expr
                    // x in y  ->  IN(x, y)
                    return bin(opIN, rewriteExpr(argRaw), rewriteExpr(recvRaw))
                }

                // ===== !in :  !(y.contains(x)) =====
                if (sym.owner.name == OperatorNameConventions.NOT) {
                    val recv = expr.dispatchReceiver
                    if (recv is IrCall && recv.symbol.owner.name == OperatorNameConventions.CONTAINS) {
                        val x = recv.arguments.getOrNull(0) ?: return expr
                        val y = recv.dispatchReceiver ?: return expr
                        return bin(opNOT_IN, rewriteExpr(x), rewriteExpr(y))
                    }
                    if (recv != null) {
                        return un(opNOT, rewriteExpr(recv))
                    }
                }

                // ===== Unary !  (including != / x != null) =====
                if (sym.owner.name == OperatorNameConventions.NOT &&
                    expr.dispatchReceiver != null
                ) {

                    val recv = expr.dispatchReceiver!!

                    if (recv is IrCall) {
                        val innerSym = recv.symbol
                        val lhs0 = recv.arguments.getOrNull(0)
                        val rhs0 = recv.arguments.getOrNull(1)
                        val innerIsEq = (innerSym == ctx.irBuiltIns.eqeqSymbol ||
                                innerSym == ctx.irBuiltIns.eqeqeqSymbol ||
                                innerSym in ctx.irBuiltIns.ieee754equalsFunByOperandType.values)

                        if (innerIsEq && lhs0 != null && rhs0 != null) {
                            if (lhs0.isNullConst()) {
                                return un(opIS_NOT_NULL, rewriteExpr(rhs0))
                            }
                            if (rhs0.isNullConst()) {
                                return un(opIS_NOT_NULL, rewriteExpr(lhs0))
                            }

                            return bin(opNEQ, rewriteExpr(lhs0), rewriteExpr(rhs0))
                        }
                    }

                    return un(opNOT, rewriteExpr(recv))
                }
            }
        }

        expr.transformChildren(thisAsTransformer(), null)
        return expr
    }

    // ─────────── construction helpers ───────────

    private fun bin(op: IrEnumEntrySymbol, l: IrExpression, r: IrExpression): IrExpression =
        IrConstructorCallImpl.fromSymbolOwner(
            l.startOffset,
            r.endOffset,
            symBin.owner.returnType,
            symBin
        ).apply {
            typeArguments[0] = ctx.irBuiltIns.booleanType
            arguments[0] = enumConst(op)
            arguments[1] = l
            arguments[2] = r
        }

    private fun un(op: IrEnumEntrySymbol, x: IrExpression): IrExpression =
        IrConstructorCallImpl.fromSymbolOwner(
            x.startOffset,
            x.endOffset,
            symUn.owner.returnType,
            symUn
        ).apply {
            typeArguments[0] = ctx.irBuiltIns.booleanType
            arguments[0] = enumConst(op)
            arguments[1] = x
        }

    private fun lit(c: IrConst): IrExpression =
        IrConstructorCallImpl.fromSymbolOwner(
            c.startOffset,
            c.endOffset,
            symLit.owner.returnType,
            symLit
        ).apply {
            typeArguments[0] = c.type
            arguments[0] = c
        }

    private fun col(name: String, type: IrType, e: IrExpression): IrExpression =
        IrConstructorCallImpl.fromSymbolOwner(
            e.startOffset,
            e.endOffset,
            symCol.owner.returnType,
            symCol
        ).apply {
            typeArguments[0] = type
            arguments[0] = IrConstImpl.constNull(
                UNDEFINED_OFFSET,
                UNDEFINED_OFFSET,
                ctx.irBuiltIns.stringType.makeNullable()
            ) // table = null
            arguments[1] = IrConstImpl.string(
                e.startOffset,
                e.endOffset,
                ctx.irBuiltIns.stringType,
                name
            )
        }

    // ─────────── utilities ───────────

    private fun IrExpression.isNullConst(): Boolean =
        this is IrConst && this.kind == IrConstKind.Null

    private fun IrType.isAkkExprType(): Boolean {
        val exprClass = ctx.referenceClass(ClassId.topLevel(fqExpr)) ?: return false
        return this.isSubtypeOfClass(exprClass)
    }

    private fun enumEntry(name: String): IrEnumEntrySymbol =
        symAkkOp.owner.declarations
            .filterIsInstance<IrEnumEntry>()
            .first { it.name.asString() == name }
            .symbol

    private fun enumConst(sym: IrEnumEntrySymbol): IrExpression =
        IrGetEnumValueImpl(
            UNDEFINED_OFFSET,
            UNDEFINED_OFFSET,
            sym.owner.parentAsClass.defaultType,
            sym
        )

    private fun primaryCtor(fq: FqName): IrConstructorSymbol {
        val cls = ctx.referenceClass(ClassId.topLevel(fq))
            ?: error("Class not found: $fq")
        return cls.owner.constructors.first().symbol
    }

    private fun thisAsTransformer() = object : IrElementTransformerVoid() {
        override fun visitExpression(expression: IrExpression): IrExpression =
            rewriteExpr(expression)
    }

    private fun findLambdaReturnExpr(fn: IrFunction): IrExpression? {
        var result: IrExpression? = null

        fn.body?.transformChildren(object : IrElementTransformerVoid() {
            override fun visitReturn(expression: IrReturn): IrExpression {
                if (expression.returnTargetSymbol == fn.symbol) {
                    result = expression.value
                }
                return expression
            }
        }, null)

        return result
    }
}