package dev.swiftstorm.akkaradb.engine.util

import dev.swiftstorm.akkaradb.common.ByteBufferL

/**
 * MurmurHash3_x64_128
 * Returns [h1, h2] as a 128-bit hash
 */
fun murmur3_128(bufOrig: ByteBufferL, seed: Long = 0L): LongArray {
    val buf = bufOrig.duplicate()
    var h1 = seed
    var h2 = seed
    val c1 = -0x783c846eeeac2bL  // 0x87c37b91114253d5
    val c2 = -0x7a143588dbe3L    // 0x4cf5ad432745937f

    // Process 16-byte blocks
    while (buf.remaining >= 16) {
        var k1 = buf.i64  // LE-safe relative read
        var k2 = buf.i64

        k1 *= c1
        k1 = rotateLeft(k1, 31)
        k1 *= c2
        h1 = h1 xor k1

        h1 = rotateLeft(h1, 27)
        h1 += h2
        h1 = h1 * 5 + 0x52dce729

        k2 *= c2
        k2 = rotateLeft(k2, 33)
        k2 *= c1
        h2 = h2 xor k2

        h2 = rotateLeft(h2, 31)
        h2 += h1
        h2 = h2 * 5 + 0x38495ab5
    }

    // Process tail (remaining < 16 bytes)
    var k1 = 0L
    var k2 = 0L

    val remaining = buf.remaining
    val base = buf.position

    // k2 (bytes 8-15) - use absolute access via at()
    if (remaining >= 15) k2 = k2 or ((buf.at(base + 14).i8.toLong() and 0xff) shl 48)
    if (remaining >= 14) k2 = k2 or ((buf.at(base + 13).i8.toLong() and 0xff) shl 40)
    if (remaining >= 13) k2 = k2 or ((buf.at(base + 12).i8.toLong() and 0xff) shl 32)
    if (remaining >= 12) k2 = k2 or ((buf.at(base + 11).i8.toLong() and 0xff) shl 24)
    if (remaining >= 11) k2 = k2 or ((buf.at(base + 10).i8.toLong() and 0xff) shl 16)
    if (remaining >= 10) k2 = k2 or ((buf.at(base + 9).i8.toLong() and 0xff) shl 8)
    if (remaining >= 9) k2 = k2 or (buf.at(base + 8).i8.toLong() and 0xff)

    // k1 (bytes 0-7)
    if (remaining >= 8) k1 = k1 or ((buf.at(base + 7).i8.toLong() and 0xff) shl 56)
    if (remaining >= 7) k1 = k1 or ((buf.at(base + 6).i8.toLong() and 0xff) shl 48)
    if (remaining >= 6) k1 = k1 or ((buf.at(base + 5).i8.toLong() and 0xff) shl 40)
    if (remaining >= 5) k1 = k1 or ((buf.at(base + 4).i8.toLong() and 0xff) shl 32)
    if (remaining >= 4) k1 = k1 or ((buf.at(base + 3).i8.toLong() and 0xff) shl 24)
    if (remaining >= 3) k1 = k1 or ((buf.at(base + 2).i8.toLong() and 0xff) shl 16)
    if (remaining >= 2) k1 = k1 or ((buf.at(base + 1).i8.toLong() and 0xff) shl 8)
    if (remaining >= 1) k1 = k1 or (buf.at(base + 0).i8.toLong() and 0xff)

    // Mix tail into hash
    if (k1 != 0L) {
        k1 *= c1
        k1 = rotateLeft(k1, 31)
        k1 *= c2
        h1 = h1 xor k1
    }

    if (k2 != 0L) {
        k2 *= c2
        k2 = rotateLeft(k2, 33)
        k2 *= c1
        h2 = h2 xor k2
    }

    // Finalization
    h1 = h1 xor bufOrig.remaining.toLong()
    h2 = h2 xor bufOrig.remaining.toLong()

    h1 += h2
    h2 += h1

    h1 = fmix64(h1)
    h2 = fmix64(h2)

    h1 += h2
    h2 += h1

    return longArrayOf(h1, h2)
}

private fun fmix64(k: Long): Long {
    var kk = k
    kk = kk xor (kk ushr 33)
    kk *= -0xae502812aa7333L   // 0xff51afd7ed558ccd
    kk = kk xor (kk ushr 33)
    kk *= -0x3b314601e57a13adL // 0xc4ceb9fe1a85ec53
    kk = kk xor (kk ushr 33)
    return kk
}

private fun rotateLeft(value: Long, distance: Int): Long =
    (value shl distance) or (value ushr (64 - distance))