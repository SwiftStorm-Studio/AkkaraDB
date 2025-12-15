package dev.swiftstorm.akkaradb.test

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.engine.AkkaraDB
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets
import java.nio.file.Path

class AkkaraDBTest {

    //    @TempDir
    var temp: Path = Path.of("./test/")

    private fun bb(str: String): ByteBufferL = ByteBufferL.wrap(StandardCharsets.US_ASCII.encode(str))

    private fun toAsciiString(buf: ByteBufferL): String {
        val d = buf.duplicate()
        val arr = ByteArray(d.remaining)
        var i = 0
        while (i < arr.size) {
            arr[i] = d.i8.toByte()
            i++
        }
        return String(arr, StandardCharsets.US_ASCII)
    }

    @Test
    fun putGetAndDelete() {
        val db = AkkaraDB.open(
            AkkaraDB.Options(
                baseDir = temp.resolve("db1"),
                walFastMode = true,
                walGroupN = 8,
                walGroupMicros = 0,
            )
        )
        db.use { db ->
            val k = bb("key-1")
            val v = bb("value-1")
            val seq1 = db.put(k.duplicate(), v.duplicate())
            Assertions.assertTrue(seq1 > 0)

            val got = db.get(k.duplicate())
            Assertions.assertNotNull(got)
            Assertions.assertEquals("value-1", toAsciiString(got!!))

            val seq2 = db.delete(k.duplicate())
            Assertions.assertTrue(seq2 > seq1)

            val gone = db.get(k.duplicate())
            Assertions.assertNull(gone)
        }
    }

    @Test
    fun compareAndSwap_updateAndDelete() {
        val db = AkkaraDB.open(AkkaraDB.Options(baseDir = temp.resolve("db2")))
        db.use { db ->
            val k = bb("cas-key")
            val v1 = bb("v1")
            val s1 = db.put(k.duplicate(), v1.duplicate())
            Assertions.assertTrue(s1 > 0)

            // success update
            val ok = db.compareAndSwap(k.duplicate(), s1, bb("v2").duplicate())
            Assertions.assertTrue(ok)
            val g2 = db.get(k.duplicate())
            Assertions.assertEquals("v2", toAsciiString(g2!!))

            // fail on wrong expected seq
            val bad = db.compareAndSwap(k.duplicate(), s1, bb("v3").duplicate())
            Assertions.assertFalse(bad)

            // delete with CAS
            val s2 = db.lastSeq()
            val delOk = db.compareAndSwap(k.duplicate(), s2, null)
            Assertions.assertTrue(delOk)
            Assertions.assertNull(db.get(k.duplicate()))
        }
    }
}
