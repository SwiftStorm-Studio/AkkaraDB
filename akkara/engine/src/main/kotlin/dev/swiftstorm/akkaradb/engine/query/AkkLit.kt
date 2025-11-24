package dev.swiftstorm.akkaradb.engine.query

/** Literal value node. */
data class AkkLit<T>(val value: T) : AkkExpr<T>