package dev.swiftstorm.akkaradb.engine.sstable.bs

import dev.swiftstorm.akkaradb.common.ByteBufferL

/**
 * BlockSearcher
 *
 * Strategy interface for searching inside a single 32 KiB data block:
 *  - [find]: point lookup, returns value slice or null
 *  - [iter]: range iteration starting at >= startKey within the block
 *
 * Implementations may parse/build a mini-index on the fly.
 */
interface BlockSearcher {
    fun find(blockBuf32k: ByteBufferL, key: ByteBufferL): ByteBufferL?
    fun iter(blockBuf32k: ByteBufferL, startKey: ByteBufferL): Sequence<Pair<ByteBufferL, ByteBufferL>>
}