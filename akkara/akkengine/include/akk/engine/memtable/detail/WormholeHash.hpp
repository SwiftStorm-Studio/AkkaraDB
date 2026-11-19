/*
 * AkkaraDB
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. See https://mozilla.org/MPL/2.0/.
 */
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(_M_X64) || defined(__x86_64__) || defined(_M_IX86) || defined(__i386__)
#include <nmmintrin.h>
#if defined(_MSC_VER)
#include <intrin.h>
#else
#include <cpuid.h>
#endif
#endif

namespace akkaradb::engine::memtable::detail::wormhole {
    // Raw incremental CRC state, without the usual final complement. These
    // hashes are transient index metadata, separate from persisted fingerprints.
    inline constexpr uint64_t HASH_SEED = 0xffffffffU;
    inline constexpr auto CRC_TABLE = [] {
        std::array<uint32_t, 256> table{};
        for (uint32_t i = 0; i < table.size(); ++i) {
            uint32_t crc = i;
            for (unsigned bit = 0; bit < 8; ++bit) { crc = (crc >> 1) ^ ((crc & 1) ? 0x82f63b78U : 0); }
            table[i] = crc;
        }
        return table;
    }();
    inline uint64_t extendHash(uint64_t hash, uint8_t byte) noexcept {
        return (static_cast<uint32_t>(hash) >> 8) ^ CRC_TABLE[(hash ^ byte) & 0xff];
    }
    inline uint64_t hashRangePortable(uint64_t hash, const uint8_t* data, size_t size) noexcept {
        hash = static_cast<uint32_t>(hash);
        for (size_t i = 0; i < size; ++i) { hash = extendHash(hash, data[i]); }
        return hash;
    }

    #if defined(_M_X64) || defined(__x86_64__) || defined(_M_IX86) || defined(__i386__)
    inline bool hardwareHashAvailable() noexcept {
        #if defined(_MSC_VER)
        int registers[4];
        __cpuidex(registers, 1, 0);
        return (static_cast<unsigned>(registers[2]) & (1U << 20)) != 0;
        #else
        unsigned eax, ebx, ecx, edx;
        return __get_cpuid(1, &eax, &ebx, &ecx, &edx) && (ecx & (1U << 20)) != 0;
        #endif
    }
    #if defined(__clang__) || defined(__GNUC__)
    __attribute__((target("sse4.2")))
    #endif
    inline uint64_t hashRangeHardware(uint64_t hash, const uint8_t* data, size_t size) noexcept {
        #if defined(_M_X64) || defined(__x86_64__)
        while (size >= 8) {
            uint64_t word;
            std::memcpy(&word, data, sizeof(word));
            hash = _mm_crc32_u64(hash, word);
            data += 8; size -= 8;
        }
        #endif
        uint32_t crc = static_cast<uint32_t>(hash);
        while (size >= 4) {
            uint32_t word;
            std::memcpy(&word, data, sizeof(word));
            crc = _mm_crc32_u32(crc, word);
            data += 4; size -= 4;
        }
        if (size >= 2) {
            uint16_t word;
            std::memcpy(&word, data, sizeof(word));
            crc = _mm_crc32_u16(crc, word);
            data += 2; size -= 2;
        }
        if (size != 0) { crc = _mm_crc32_u8(crc, *data); }
        return crc;
    }
    #else
    inline bool hardwareHashAvailable() noexcept { return false; }
    inline uint64_t hashRangeHardware(uint64_t hash, const uint8_t* data, size_t size) noexcept {
        return hashRangePortable(hash, data, size);
    }
    #endif

    using HashRange = uint64_t (*)(uint64_t, const uint8_t*, size_t) noexcept;
    inline HashRange selectHashRange() noexcept {
        static const HashRange selected = hardwareHashAvailable() ? hashRangeHardware : hashRangePortable;
        return selected;
    }
}
