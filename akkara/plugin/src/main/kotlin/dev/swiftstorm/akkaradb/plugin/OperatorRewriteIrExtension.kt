@file:OptIn(FirIncompatiblePluginAPI::class, UnsafeDuringIrConstructionAPI::class)
@file:Suppress("unused", "PrivatePropertyName", "DuplicatedCode", "SameParameterValue")

package dev.swiftstorm.akkaradb.plugin

import org.jetbrains.kotlin.backend.common.extensions.FirIncompatiblePluginAPI
import org.jetbrains.kotlin.backend.common.extensions.IrGenerationExtension
import org.jetbrains.kotlin.backend.common.extensions.IrPluginContext
import org.jetbrains.kotlin.ir.UNDEFINED_OFFSET
import org.jetbrains.kotlin.ir.declarations.*
import org.jetbrains.kotlin.ir.expressions.*
import org.jetbrains.kotlin.ir.expressions.impl.*
import org.jetbrains.kotlin.ir.symbols.*
import org.jetbrains.kotlin.ir.types.IrType
import org.jetbrains.kotlin.ir.types.classifierOrFail
import org.jetbrains.kotlin.ir.util.*
import org.jetbrains.kotlin.ir.visitors.IrElementTransformerVoid
import org.jetbrains.kotlin.name.ClassId
import org.jetbrains.kotlin.name.FqName

/**
 * IR rewriter that rewrites boolean expressions inside `PackedTable.query { ... }`
 * into AkkExpr nodes (AkkBin/AkkUn/AkkLit), so the runtime receives a typed AST.
 *
 * Current coverage:
 *  - Equality: `==` / `!=`
 *      * `x == null`  -> Un(IS_NULL, x)
 *      * `x != null`  -> Un(IS_NOT_NULL, x)
 *      * `x == y`     -> Bin(EQ, x, y)
 *      * `x != y`     -> Bin(NEQ, x, y)
 *  - Unary NOT: `!x`   -> Un(NOT, x)
 *  - Literals: constants -> AkkLit(value)
 *  - Columns/Expr: if the expression is already AkkExpr<T>, reuse it
 *
 * Next steps (not implemented here yet):
 *  - `&&` / `||` (short-circuit): transform IrWhen into Bin(AND/OR, ...)
 *  - Comparisons `> < >= <=` (compareTo patterns)
 *  - `in` / `!in`, `like`, arithmetic + - * /
 */
class OperatorRewriteIrExtension : IrGenerationExtension {

    override fun generate(moduleFragment: IrModuleFragment, pluginContext: IrPluginContext) {
        val rewriter = QueryCallRewriter(pluginContext)
        moduleFragment.transformChildren(object : IrElementTransformerVoid() {
            override fun visitCall(expression: IrCall): IrExpression {
                // Intercept `AkkDSL.query { ... }` and rewrite the lambda body
                if (rewriter.isAkkQueryInvocation(expression)) {
                    rewriter.rewriteQueryInvocation(expression)
                    // Still visit children so nested items transform if needed
                }
                return super.visitCall(expression)
            }
        }, null)
    }
}

private class QueryCallRewriter(
    private val ctx: IrPluginContext
) {
    // FQNs in akkara-engine (keep in sync with engine module)
    private val fqAkkDSL = FqName("dev.swiftstorm.akkaradb.engine.AkkDSL")
    private val fqQuery = FqName("dev.swiftstorm.akkaradb.engine.PackedTable.query")

    private val fqExpr = FqName("dev.swiftstorm.akkaradb.engine.AkkExpr")
    private val fqCol = FqName("dev.swiftstorm.akkaradb.engine.AkkCol")
    private val fqLit = FqName("dev.swiftstorm.akkaradb.engine.AkkLit")
    private val fqBin = FqName("dev.swiftstorm.akkaradb.engine.AkkBin")
    private val fqUn = FqName("dev.swiftstorm.akkaradb.engine.AkkUn")
    private val fqOp = FqName("dev.swiftstorm.akkaradb.engine.AkkOp")

    // Cached symbols
    private val symAkkLitCtor by lazy { requirePrimaryCtor(fqLit) }
    private val symAkkBinCtor by lazy { requirePrimaryCtor(fqBin) }
    private val symAkkUnCtor by lazy { requirePrimaryCtor(fqUn) }
    private val symAkkOp by lazy { ctx.referenceClass(ClassId.topLevel(fqOp)) ?: error("AkkOp not found") }

    // AkkOp entries we currently use
    private val opEQ by lazy { enumEntry(symAkkOp, "EQ") }
    private val opNEQ by lazy { enumEntry(symAkkOp, "NEQ") }
    private val opNOT by lazy { enumEntry(symAkkOp, "NOT") }
    private val opIS_NULL by lazy { enumEntry(symAkkOp, "IS_NULL") }
    private val opIS_NOT_NULL by lazy { enumEntry(symAkkOp, "IS_NOT_NULL") }

    /**
     * Returns true if this call is `AkkDSL.query { ... }`.
     */
    fun isAkkQueryInvocation(call: IrCall): Boolean {
        val callee = call.symbol.owner
        val fq = callee.kotlinFqName
        // Cheap check: function fqName match and container is AkkDSL
        return fq == fqQuery
    }

    /**
     * Rewrite the single lambda argument (block) of `AkkDSL.query { ... }`.
     * We replace the lambda body return expression with an AkkExpr tree.
     */
    fun rewriteQueryInvocation(call: IrCall) {
        // `query` takes exactly one function argument () -> AkkExpr<Boolean>
        val fnExpr = call.arguments[0] as? IrFunctionExpression ?: return
        val fn = fnExpr.function

        // Transform the lambda body
        val body = fn.body ?: return
        body.transformChildren(object : IrElementTransformerVoid() {

            // Maintain "inside-query" flag: here we are already inside the lambda

            override fun visitReturn(expression: IrReturn): IrExpression {
                // Only rewrite the return that returns from this lambda
                if (expression.returnTargetSymbol == fn.symbol) {
                    val replaced = rewriteExpr(expression.value)
                    return IrReturnImpl(
                        expression.startOffset, expression.endOffset,
                        expression.type, expression.returnTargetSymbol, replaced
                    )
                }
                return super.visitReturn(expression)
            }
        }, null)
    }

    /**
     * Core expression rewriter: maps IR to AkkExpr constructor calls.
     * Limited set (==, !=, !, literals, already-AkkExpr) for the first cut.
     */
    private fun rewriteExpr(expr: IrExpression): IrExpression {
        // Already an AkkExpr<T> → only rewrite children
        if (expr.type.isAkkExprType())
            return expr.apply { transformChildren(thisAsTransformer(), null) }

        // ----------- Literals -----------
        if (expr is IrConst) return newAkkLit(expr)

        // ----------- Property access: IrGetField → AkkCol -----------
        if (expr is IrGetField) {
            val propName = expr.symbol.owner.name.asString()
            val call = IrConstructorCallImpl.fromSymbolOwner(
                startOffset = expr.startOffset,
                endOffset = expr.endOffset,
                type = ctx.referenceClass(ClassId.topLevel(fqCol))?.owner?.defaultType
                    ?: error("AkkCol not found"),
                constructorSymbol = requirePrimaryCtor(fqCol)
            )
            // <T> AkkCol<T>(table=null, name="field")
            call.typeArguments[0] = expr.type
            call.arguments[0] = IrConstImpl.constNull(
                expr.startOffset, expr.endOffset, ctx.irBuiltIns.stringType
            )
            call.arguments[1] = IrConstImpl.string(
                expr.startOffset, expr.endOffset, ctx.irBuiltIns.stringType, propName
            )
            return call
        }

        // ----------- Value access: IrGetValue (lambda params) -----------
        if (expr is IrGetValue) {
            // If it is a property reference captured into a temporary, resolve name
            val symbol = expr.symbol
            val name = symbol.owner.name.asString()

            // Filter out lambda params & local vars → not columns
            if (name != "this" && !name.startsWith("$")) {
                val call = IrConstructorCallImpl.fromSymbolOwner(
                    startOffset = expr.startOffset,
                    endOffset = expr.endOffset,
                    type = ctx.referenceClass(ClassId.topLevel(fqCol))?.owner?.defaultType
                        ?: error("AkkCol not found"),
                    constructorSymbol = requirePrimaryCtor(fqCol)
                )
                call.typeArguments[0] = expr.type
                call.arguments[0] = IrConstImpl.constNull(
                    expr.startOffset, expr.endOffset, ctx.irBuiltIns.stringType
                )
                call.arguments[1] = IrConstImpl.string(
                    expr.startOffset, expr.endOffset, ctx.irBuiltIns.stringType, name
                )
                return call
            }
            // Otherwise → ignore (local variable etc.)
        }

        // ----------- Equality comparisons -----------
        if (expr is IrCall) {
            val sym = expr.symbol

            val isEq = (sym == ctx.irBuiltIns.eqeqSymbol) ||
                    (sym == ctx.irBuiltIns.eqeqeqSymbol) ||
                    (sym in ctx.irBuiltIns.ieee754equalsFunByOperandType.values)

            if (isEq) {
                val lhs0 = expr.arguments.getOrNull(0) ?: return expr
                val rhs0 = expr.arguments.getOrNull(1) ?: return expr
                val lhs = rewriteExpr(lhs0)
                val rhs = rewriteExpr(rhs0)

                val lhsIsNull = lhs0 is IrConst && lhs0.kind == IrConstKind.Null
                val rhsIsNull = rhs0 is IrConst && rhs0.kind == IrConstKind.Null
                return when {
                    rhsIsNull -> newAkkUn(opIS_NULL, lhs)
                    lhsIsNull -> newAkkUn(opIS_NULL, rhs)
                    else -> newAkkBin(opEQ, lhs, rhs)
                }
            }

            // ----------- Comparisons < > <= >= -----------
            fun match(opMap: Map<IrClassifierSymbol, IrSimpleFunctionSymbol>, akkName: String): IrExpression? {
                val recv = expr.dispatchReceiver ?: return null
                val arg0 = expr.arguments.getOrNull(0) ?: return null
                val expected = opMap[recv.type.classifierOrFail] ?: return null
                if (sym != expected) return null
                return newAkkBin(enumEntry(symAkkOp, akkName), rewriteExpr(recv), rewriteExpr(arg0))
            }

            match(ctx.irBuiltIns.lessFunByOperandType, "LT")?.let { return it }
            match(ctx.irBuiltIns.greaterFunByOperandType, "GT")?.let { return it }
            match(ctx.irBuiltIns.lessOrEqualFunByOperandType, "LE")?.let { return it }
            match(ctx.irBuiltIns.greaterOrEqualFunByOperandType, "GE")?.let { return it }
        }

        // ----------- Unary NOT -----------
        if (expr is IrCall &&
            expr.symbol.owner.name.asString() == "not" &&
            expr.type == ctx.irBuiltIns.booleanType
        ) {
            val recvAny = expr.dispatchReceiver ?: return expr
            return newAkkUn(opNOT, rewriteExpr(recvAny))
        }

        // ----------- When(Boolean) → AND/OR desugaring -----------
        if (expr is IrWhen && expr.type == ctx.irBuiltIns.booleanType && expr.branches.size == 2) {
            val condRaw = expr.branches[0].condition
            val thenRaw = expr.branches[0].result
            val elseRaw = expr.branches[1].result
            val thenConst = (thenRaw as? IrConst)?.value as? Boolean
            val elseConst = (elseRaw as? IrConst)?.value as? Boolean

            if (thenConst == true)  // a || b
                return newAkkBin(enumEntry(symAkkOp, "OR"), rewriteExpr(condRaw), rewriteExpr(elseRaw))
            if (elseConst == false) // a && b
                return newAkkBin(enumEntry(symAkkOp, "AND"), rewriteExpr(condRaw), rewriteExpr(thenRaw))
        }

        // ----------- Children transform only -----------
        if (expr is IrGetValue || expr is IrCall || expr is IrWhen || expr is IrTypeOperatorCall || expr is IrVararg) {
            expr.transformChildren(thisAsTransformer(), null)
            return expr
        }

        // ----------- Everything else: treat as literal null -----------
        return newAkkLit(null)
    }

    // ───────────── helpers for IR construction ─────────────

    private fun newAkkLit(valueConst: IrConst): IrExpression {
        val call = IrConstructorCallImpl.fromSymbolOwner(
            startOffset = valueConst.startOffset,
            endOffset = valueConst.endOffset,
            type = symAkkLitCtor.owner.returnType,
            constructorSymbol = symAkkLitCtor
        )
        // <T> AkkLit<T>(value)
        call.typeArguments[0] = valueConst.type
        call.arguments[0] = valueConst
        return call
    }

    private fun newAkkLit(any: Any?): IrExpression {
        val const: IrConst = when (any) {
            null -> IrConstImpl.constNull(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.anyNType)
            is String -> IrConstImpl.string(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.stringType, any)
            is Boolean -> IrConstImpl.boolean(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.booleanType, any)
            is Int -> IrConstImpl.int(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.intType, any)
            is Long -> IrConstImpl.long(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.longType, any)
            is Float -> IrConstImpl.float(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.floatType, any)
            is Double -> IrConstImpl.double(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.doubleType, any)
            is Short -> IrConstImpl.short(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.shortType, any)
            is Byte -> IrConstImpl.byte(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.byteType, any)
            is Char -> IrConstImpl.char(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.charType, any)
            else -> IrConstImpl.constNull(UNDEFINED_OFFSET, UNDEFINED_OFFSET, ctx.irBuiltIns.anyNType)
        }
        return newAkkLit(const)
    }

    private fun newAkkBin(op: IrEnumEntrySymbol, lhs: IrExpression, rhs: IrExpression): IrExpression {
        val call = IrConstructorCallImpl.fromSymbolOwner(
            startOffset = lhs.startOffset,
            endOffset = rhs.endOffset,
            type = symAkkBinCtor.owner.returnType,
            constructorSymbol = symAkkBinCtor
        )
        call.typeArguments[0] = ctx.irBuiltIns.booleanType // AkkBin<Boolean>(...)
        call.arguments[0] = irGetEnum(op)
        call.arguments[1] = lhs
        call.arguments[2] = rhs
        return call
    }

    private fun newAkkUn(op: IrEnumEntrySymbol, x: IrExpression): IrExpression {
        val call = IrConstructorCallImpl.fromSymbolOwner(
            startOffset = x.startOffset,
            endOffset = x.endOffset,
            type = symAkkUnCtor.owner.returnType,
            constructorSymbol = symAkkUnCtor
        )
        call.typeArguments[0] = ctx.irBuiltIns.booleanType
        call.arguments[0] = irGetEnum(op)
        call.arguments[1] = x
        return call
    }

    private fun irGetEnum(entry: IrEnumEntrySymbol): IrExpression =
        IrGetEnumValueImpl(
            UNDEFINED_OFFSET,
            UNDEFINED_OFFSET,
            entry.owner.parentAsClass.defaultType,
            entry
        )

    private fun requirePrimaryCtor(fq: FqName): IrConstructorSymbol {
        val cls = ctx.referenceClass(ClassId.topLevel(fq))
            ?: error("Class not found: $fq")

        // Prefer the primary constructor when available; fall back to the first one.
        val ctor: IrConstructor = cls.owner.declarations
            .filterIsInstance<IrConstructor>()
            .firstOrNull { it.isPrimary }
            ?: cls.owner.constructors.firstOrNull()
            ?: error("No constructor found for $fq")

        return ctor.symbol
    }

    private fun enumEntry(enumClass: IrClassSymbol, name: String): IrEnumEntrySymbol {
        val entry = enumClass.owner.declarations
            .filterIsInstance<IrEnumEntry>()
            .firstOrNull { it.name.asString() == name }
            ?: error("Enum entry $name not found in ${enumClass.owner.name}")
        return entry.symbol
    }

    private fun IrType.isAkkExprType(): Boolean {
        val exprClass = ctx.referenceClass(ClassId.topLevel(fqExpr)) ?: return false
        return this.isSubtypeOfClass(exprClass)
    }


    private fun thisAsTransformer() = object : IrElementTransformerVoid() {
        override fun visitExpression(expression: IrExpression): IrExpression =
            rewriteExpr(expression)
    }
}

// Small utility for parent FQ name on function symbol
private val IrDeclarationWithName.parentClassFqName: FqName?
    get() = (parent as? IrClass)?.kotlinFqName
