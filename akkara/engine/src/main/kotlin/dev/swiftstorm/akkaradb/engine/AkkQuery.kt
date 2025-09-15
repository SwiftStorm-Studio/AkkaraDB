@file:Suppress("unused")

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.ShortUUID
import dev.swiftstorm.akkaradb.common.internal.binpack.BinPack
import dev.swiftstorm.akkaradb.engine.BoolCNF.singleBoundFor
import dev.swiftstorm.akkaradb.engine.BoolCNF.singleEqOrRangeFor
import kotlin.reflect.KProperty1

/* ───────── DSL AST ───────── */

sealed interface Expr<T>
data class Col<T, R>(val prop: KProperty1<T, R>) : Expr<T>
data class Val<T, R>(val value: R) : Expr<T>
enum class CmpOp { LT, LE, GT, GE, EQ, NE }
data class Cmp<T, R : Comparable<R>>(val left: Col<T, R>, val op: CmpOp, val right: Val<T, R>) : Expr<T>
data class And<T>(val a: Expr<T>, val b: Expr<T>) : Expr<T>
data class Or<T>(val a: Expr<T>, val b: Expr<T>) : Expr<T>
object True : Expr<Any>

infix fun <T> Expr<T>.and(other: Expr<T>) = And(this, other)
infix fun <T> Expr<T>.or(other: Expr<T>) = Or(this, other)

infix fun <T, R : Comparable<R>> KProperty1<T, R>.lt(v: R): Expr<T> = Cmp(Col(this), CmpOp.LT, Val(v))
infix fun <T, R : Comparable<R>> KProperty1<T, R>.le(v: R): Expr<T> = Cmp(Col(this), CmpOp.LE, Val(v))
infix fun <T, R : Comparable<R>> KProperty1<T, R>.gt(v: R): Expr<T> = Cmp(Col(this), CmpOp.GT, Val(v))
infix fun <T, R : Comparable<R>> KProperty1<T, R>.ge(v: R): Expr<T> = Cmp(Col(this), CmpOp.GE, Val(v))
infix fun <T, R : Comparable<R>> KProperty1<T, R>.eq(v: R): Expr<T> = Cmp(Col(this), CmpOp.EQ, Val(v))
infix fun <T, R : Comparable<R>> KProperty1<T, R>.neq(v: R): Expr<T> = Cmp(Col(this), CmpOp.NE, Val(v))


class Query<T : Any>(
    private val table: PackedTable<T>,
    private val root: Expr<T>,
) {
    fun exec(limit: Long? = null): List<T> {
        val plan = KeyRangeCompiler.compile(table, root)

        val onlyKeyPredicates = usesOnlyIdUuid(root)
        val scanLimit = if (plan != null && onlyKeyPredicates) limit else null

        val seq: Sequence<Record> =
            if (plan != null) {
                table.db.scanRange(plan.start, plan.end, scanLimit)
            } else {
                val start = table.prefixBuf("")
                val end = start.prefixUpper()
                table.db.scanRange(start, end, null)
            }

        val kClass = table.kClass
        val filtered = seq
            .mapNotNull { r -> if (r.isTombstone) null else BinPack.decode(kClass, r.value) }
            .filter { entity -> PostFilter.eval(root, entity) }

        return if (limit != null)
            filtered.take(limit.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()).toList()
        else
            filtered.toList()
    }
}

private data class KeyRange(val start: ByteBufferL, val end: ByteBufferL?)

private object KeyRangeCompiler {
    fun <T : Any> compile(table: PackedTable<T>, expr: Expr<T>): KeyRange? {
        val flat = BoolCNF.extract(expr)

        val idC = flat.singleEqOrRangeFor<String, T>("id")
        val uC = flat.singleBoundFor<ShortUUID, T>("uuid")

        when {
            // id == "x"
            idC?.isEq == true -> {
                val id = idC.eqValue!!
                val startPrefix = table.prefixBuf(id)
                var start = startPrefix
                var end = startPrefix.prefixUpper()

                if (uC != null && uC.lower != null) {
                    start = when (uC.lowerOp) {
                        CmpOp.GT -> table.keyBuf(id, uC.lower).bumpAfter()
                        CmpOp.GE -> table.keyBuf(id, uC.lower)
                        else -> start
                    }
                }
                if (uC != null && uC.upper != null) {
                    end = when (uC.upperOp) {
                        CmpOp.LT -> table.keyBuf(id, uC.upper)
                        CmpOp.LE -> table.keyBuf(id, uC.upper).bumpAfter()
                        else -> end
                    }
                }
                return KeyRange(start, end)
            }

            idC?.isRange == true && idC.lower != null && idC.upper != null -> {
                val s = if (idC.lowerInc) {
                    table.prefixBuf(idC.lower)
                } else {
                    table.prefixBuf(idC.lower).prefixUpper()
                }

                val e = if (idC.upperInc) {
                    table.prefixBuf(idC.upper).prefixUpper()
                } else {
                    table.prefixBuf(idC.upper)
                }

                return KeyRange(s, e)
            }
        }
        return null
    }
}

private object PostFilter {
    fun <T : Any> eval(e: Expr<T>, o: T): Boolean = when (e) {
        is True -> true
        is And<T> -> eval(e.a, o) && eval(e.b, o)
        is Or<T> -> eval(e.a, o) || eval(e.b, o)
        is Cmp<T, *> -> {
            @Suppress("UNCHECKED_CAST")
            val p = (e.left.prop as KProperty1<T, Comparable<Any?>>)
            val lv = p.get(o)

            @Suppress("UNCHECKED_CAST")
            val rv = e.right.value as Comparable<Any?>
            when (e.op) {
                CmpOp.LT -> lv < rv
                CmpOp.LE -> lv <= rv
                CmpOp.GT -> lv > rv
                CmpOp.GE -> lv >= rv
                CmpOp.EQ -> lv == rv
                CmpOp.NE -> lv != rv
            }
        }

        is Col<*, *> -> error("bare column is not a predicate")
        is Val<*, *> -> error("bare value is not a predicate")
    }
}

private object BoolCNF {
    data class IdCmp<R : Comparable<R>>(
        val isEq: Boolean = false,
        val eqValue: R? = null,
        val isRange: Boolean = false,
        val lower: R? = null,
        val lowerInc: Boolean = true,
        val upper: R? = null,
        val upperInc: Boolean = false,
    )

    data class Bound<R : Comparable<R>>(
        val lower: R? = null, val lowerOp: CmpOp? = null,
        val upper: R? = null, val upperOp: CmpOp? = null,
    )

    fun <T : Any> extract(e: Expr<T>): List<Expr<T>> {
        fun go(x: Expr<T>, out: MutableList<Expr<T>>) {
            when (x) {
                is And<T> -> {
                    go(x.a, out); go(x.b, out)
                }

                else -> out += x
            }
        }
        return buildList { go(e, this) }
    }

    inline fun <reified R : Comparable<R>, T : Any> List<Expr<T>>.singleEqOrRangeFor(name: String): IdCmp<R>? {
        var eq: R? = null
        var lo: Pair<R, Boolean>? = null
        var hi: Pair<R, Boolean>? = null
        for (p in this) if (p is Cmp<*, *>) {
            @Suppress("UNCHECKED_CAST")
            val c = p as Cmp<T, R>
            if (c.left.prop.name != name) continue
            when (c.op) {
                CmpOp.EQ -> {
                    eq = c.right.value; break
                }

                CmpOp.GE -> lo = c.right.value to true
                CmpOp.GT -> lo = c.right.value to false
                CmpOp.LE -> hi = c.right.value to true
                CmpOp.LT -> hi = c.right.value to false
                else -> {}
            }
        }
        return when {
            eq != null -> IdCmp(isEq = true, eqValue = eq)
            lo != null || hi != null -> IdCmp(
                isRange = true,
                lower = lo?.first, lowerInc = lo?.second ?: true,
                upper = hi?.first, upperInc = hi?.second ?: false,
            )

            else -> null
        }
    }

    inline fun <reified R : Comparable<R>, T : Any> List<Expr<T>>.singleBoundFor(name: String): Bound<R>? {
        var lo: Pair<R, CmpOp>? = null
        var hi: Pair<R, CmpOp>? = null
        for (p in this) if (p is Cmp<*, *>) {
            @Suppress("UNCHECKED_CAST")
            val c = p as Cmp<T, R>
            if (c.left.prop.name != name) continue
            when (c.op) {
                CmpOp.GE, CmpOp.GT -> lo = c.right.value to c.op
                CmpOp.LE, CmpOp.LT -> hi = c.right.value to c.op
                else -> {}
            }
        }
        if (lo == null && hi == null) return null
        return Bound(lower = lo?.first, lowerOp = lo?.second, upper = hi?.first, upperOp = hi?.second)
    }
}

private fun ByteBufferL.prefixUpper(): ByteBufferL {
    val out = ByteBufferL.allocate(this.remaining + 1)
    out.put(this.duplicate()).put(0xFF.toByte())
    out.flip()
    return out.asReadOnly()
}

private fun ByteBufferL.bumpAfter(): ByteBufferL {
    val out = ByteBufferL.allocate(this.remaining + 1)
    out.put(this.duplicate()).put(0x00) // 最小追加で辞書順直後へ
    out.flip()
    return out.asReadOnly()
}

private fun <T : Any> usesOnlyIdUuid(expr: Expr<T>): Boolean = when (expr) {
    is True -> true
    is And<T> -> usesOnlyIdUuid(expr.a) && usesOnlyIdUuid(expr.b)
    is Or<T> -> false
    is Cmp<T, *> -> {
        val n = expr.left.prop.name
        n == "id" || n == "uuid"
    }

    is Col<*, *> -> false
    is Val<*, *> -> false
}

/* ─────────  Entry Point ───────── */
fun <T : Any> PackedTable<T>.query(where: () -> Expr<T>): Query<T> =
    Query(this, where())
