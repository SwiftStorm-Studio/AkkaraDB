package dev.swiftstorm.akkaradb.engine.query

/**
 * Algebraic expression node used by the query DSL.
 * A Kotlin compiler plugin can rewrite operators into this tree.
 */
sealed interface AkkExpr<T>