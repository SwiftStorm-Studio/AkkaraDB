package dev.swiftstorm.akkaradb.format.api

interface AkkDecoder {
    fun decodeKey(encoded: ByteArray): ByteArray
    fun decodeValue(encoded: ByteArray): ByteArray
}
