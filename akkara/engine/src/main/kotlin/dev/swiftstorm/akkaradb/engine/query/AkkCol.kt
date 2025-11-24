package dev.swiftstorm.akkaradb.engine.query

/** Column reference (e.g., users.age). */
data class AkkCol<T>(val table: String?, val name: String) : AkkExpr<T>