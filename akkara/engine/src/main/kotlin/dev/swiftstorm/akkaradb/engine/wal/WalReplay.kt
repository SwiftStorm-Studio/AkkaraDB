package dev.swiftstorm.akkaradb.engine.wal


/** WAL replay for v3 framing. Safe to run on truncated tail. */
object WalReplay {
    data class Result(val applied: Long)


//    fun replay(path: Path, mem: MemTable): Result {
//        if (!Files.exists(path) || Files.size(path) == 0L) return Result(0)
//        var applied = 0L
//        FileChannel.open(path, READ).use { ch ->
//            val map = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size())
//            while (true) {
//                val payload = WalFraming.readOne(map) ?: break // stop at partial/tail
//                // Interpret payload as AKHdr32 + key + value
//                val bb = ByteBufferL.wrap(payload)
//                val hdr = AKHdr32.readRel(bb.rawDuplicate())
//                val keySlice = bb.sliceAt(bb.position, hdr.kLen)
//                bb.position = bb.position + hdr.kLen
//                val vLen = hdr.vLen.toIntExact()
//                val valSlice = if (vLen > 0) bb.sliceAt(bb.position, vLen) else ByteBufferL.allocate(0, direct = false)
//                if ((hdr.flags and RecordFlags.TOMBSTONE.toInt()) != 0) {
//                    mem.delete(keySlice, hdr.seq.raw)
//                } else {
//                    mem.put(keySlice, valSlice, hdr.seq.raw)
//                }
//                applied++
//            }
//        }
//        return Result(applied)
//    }
}