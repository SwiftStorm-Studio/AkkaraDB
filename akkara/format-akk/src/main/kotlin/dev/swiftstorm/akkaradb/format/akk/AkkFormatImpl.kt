package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.format.api.AkkDecoder
import dev.swiftstorm.akkaradb.format.api.AkkEncoder
import dev.swiftstorm.akkaradb.format.api.AkkFormat

object AkkFormatImpl : AkkFormat {
    override val name: String = "akk"

    override fun newEncoder(): AkkEncoder = object : AkkEncoder {
        override fun encode(key: ByteArray, value: ByteArray): ByteArray {
            val totalLength = 4 + key.size + value.size
            val buffer = ByteArray(totalLength)
            val keyLen = key.size
            val keyLenBytes = intToBytes(keyLen)

            // [keyLen (4B)] + [key] + [value]
            System.arraycopy(keyLenBytes, 0, buffer, 0, 4)
            System.arraycopy(key, 0, buffer, 4, key.size)
            System.arraycopy(value, 0, buffer, 4 + key.size, value.size)
            return buffer
        }
    }

    override fun newDecoder(): AkkDecoder = object : AkkDecoder {
        override fun decodeKey(encoded: ByteArray): ByteArray {
            val keyLen = bytesToInt(encoded.sliceArray(0..<4))
            return encoded.sliceArray(4 until (4 + keyLen))
        }

        override fun decodeValue(encoded: ByteArray): ByteArray {
            val keyLen = bytesToInt(encoded.sliceArray(0..<4))
            return encoded.sliceArray(4 + keyLen until encoded.size)
        }
    }

    private fun intToBytes(value: Int): ByteArray = ByteArray(4) { i ->
        (value ushr ((3 - i) * 8) and 0xFF).toByte()
    }

    private fun bytesToInt(bytes: ByteArray): Int {
        require(bytes.size == 4) { "Expected 4 bytes to convert to Int" }
        return (bytes[0].toInt() and 0xFF shl 24) or
                (bytes[1].toInt() and 0xFF shl 16) or
                (bytes[2].toInt() and 0xFF shl 8) or
                (bytes[3].toInt() and 0xFF)
    }
}
