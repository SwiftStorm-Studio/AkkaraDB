package dev.swiftstorm.akkaradb.format.akk.parity

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityKind

class NoParityCoder : ParityCoder {
    override val parityCount: Int
        get() = 0
    override val kind: ParityKind
        get() = ParityKind.NONE

    override fun encodeInto(
        data: Array<ByteBufferL>,
        parityOut: Array<ByteBufferL>
    ) {
    }

    override fun verify(
        data: Array<ByteBufferL>,
        parity: Array<ByteBufferL>
    ): Boolean {
        return true
    }

    override fun reconstruct(
        lostDataIdx: IntArray,
        lostParityIdx: IntArray,
        data: Array<ByteBufferL?>,
        parity: Array<ByteBufferL?>,
        outData: Array<ByteBufferL>,
        outParity: Array<ByteBufferL>
    ): Int {
        return 0
    }

}