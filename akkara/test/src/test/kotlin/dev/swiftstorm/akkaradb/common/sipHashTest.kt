package dev.swiftstorm.akkaradb.common

fun main() {
    repeat(1000) {
        val arr = ByteArray((0..64).random()) { it.toByte() }
        val l = ByteBufferL.allocate(arr.size, direct = false).apply { putBytes(arr) }.position(0)
        val a = AKHdr32.sipHash24(arr, AKHdr32.DEFAULT_SIPHASH_SEED)
        val b = AKHdr32.sipHash24(l, AKHdr32.DEFAULT_SIPHASH_SEED)
        check(a == b)
        val am = AKHdr32.buildMiniKeyLE(arr)
        val bm = AKHdr32.buildMiniKeyLE(l)
        check(am == bm)
    }
}