package dev.swiftstorm.akkaradb.format.api

interface AkkFormat {
    val name: String
    fun newEncoder(): AkkEncoder
    fun newDecoder(): AkkDecoder
}
