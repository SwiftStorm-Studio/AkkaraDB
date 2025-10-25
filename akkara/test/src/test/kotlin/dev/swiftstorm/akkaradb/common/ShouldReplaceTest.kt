package dev.swiftstorm.akkaradb.common

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ShouldReplaceTest {
    @Test
    fun candidateAcceptedWhenNoExisting() {
        val candidate = record(seq = 1)
        assertTrue(shouldReplace(null, candidate))
    }

    @Test
    fun higherSequenceReplacesLower() {
        val existing = record(seq = 1)
        val candidate = record(seq = 2)
        assertTrue(shouldReplace(existing, candidate))
    }

    @Test
    fun lowerSequenceDoesNotReplace() {
        val existing = record(seq = 5)
        val candidate = record(seq = 4)
        assertFalse(shouldReplace(existing, candidate))
    }

    @Test
    fun tombstoneWinsOnEqualSequence() {
        val existing = record(seq = 7)
        val candidate = record(seq = 7, tombstone = true)
        assertTrue(shouldReplace(existing, candidate))
    }

    @Test
    fun tombstoneCannotBeResurrectedAtSameSequence() {
        val existing = record(seq = 9, tombstone = true)
        val candidate = record(seq = 9)
        assertFalse(shouldReplace(existing, candidate))
    }

    @Test
    fun idempotentUpdatesDoNotChurn() {
        val existing = record(seq = 11)
        val candidate = record(seq = 11)
        assertFalse(shouldReplace(existing, candidate))
    }

    private fun record(seq: Long, tombstone: Boolean = false): MemRecord {
        val key = bufOf("key-$seq-${if (tombstone) "t" else "v"}")
        val value = bufOf("value-$seq")
        val base = memRecordOf(key, value, seq)
        return if (tombstone) base.asTombstone() else base
    }

    private fun bufOf(text: String): ByteBufferL {
        val bytes = text.encodeToByteArray()
        val buf = ByteBufferL.allocate(bytes.size, direct = false)
        buf.putBytes(bytes)
        buf.position = 0
        buf.limit = bytes.size
        return buf
    }
}
