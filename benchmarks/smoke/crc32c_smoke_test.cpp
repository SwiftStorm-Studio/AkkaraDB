/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include "akk/cpu/CRC32C.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <vector>

#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
#  include <intrin.h>
#endif

#if (defined(__GNUC__) || defined(__clang__)) && (defined(__x86_64__) || defined(__i386__))
#  include <cpuid.h>
#endif

namespace akkaradb::cpu {
    uint32_t CRC32C_Ref(const std::byte* data, size_t length) noexcept;

    #if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
    uint32_t CRC32C_X86_SSE42(const std::byte* data, size_t length) noexcept;
    uint32_t CRC32C_X86_AVX2(const std::byte* data, size_t length) noexcept;
    #endif

    #if defined(__x86_64__) || defined(_M_X64)
    uint32_t CRC32C_X86_AVX512(const std::byte* data, size_t length) noexcept;
    #endif
} // namespace akkaradb::cpu

namespace {
    struct CpuRegs {
        int eax = 0;
        int ebx = 0;
        int ecx = 0;
        int edx = 0;
    };

    [[nodiscard]] bool has_bit(int value, int bit) noexcept {
        return (static_cast<uint32_t>(value) & (uint32_t{1} << bit)) != 0;
    }

    [[nodiscard]] CpuRegs cpuid(int leaf, int subleaf = 0) noexcept {
        CpuRegs regs{};

        #if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
        int raw[4]{};
        __cpuidex(raw, leaf, subleaf);
        regs.eax = raw[0];
        regs.ebx = raw[1];
        regs.ecx = raw[2];
        regs.edx = raw[3];
        #elif (defined(__GNUC__) || defined(__clang__)) && (defined(__x86_64__) || defined(__i386__))
        unsigned int eax = 0;
        unsigned int ebx = 0;
        unsigned int ecx = 0;
        unsigned int edx = 0;
        __cpuid_count(static_cast<unsigned int>(leaf), static_cast<unsigned int>(subleaf), eax, ebx, ecx, edx);
        regs.eax = static_cast<int>(eax);
        regs.ebx = static_cast<int>(ebx);
        regs.ecx = static_cast<int>(ecx);
        regs.edx = static_cast<int>(edx);
        #endif

        return regs;
    }

    [[nodiscard]] uint64_t xgetbv0() noexcept {
        #if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
        return _xgetbv(0);
        #elif (defined(__GNUC__) || defined(__clang__)) && (defined(__x86_64__) || defined(__i386__))
        uint32_t eax = 0;
        uint32_t edx = 0;
        __asm__ volatile ("xgetbv" : "=a"(eax), "=d"(edx) : "c"(0));
        return (static_cast<uint64_t>(edx) << 32) | eax;
        #else
        return 0;
        #endif
    }

    [[nodiscard]] bool supports_sse42() noexcept {
        const auto regs = cpuid(1);
        return has_bit(regs.ecx, 20);
    }

    [[nodiscard]] bool supports_pclmulqdq() noexcept {
        const auto regs = cpuid(1);
        return has_bit(regs.ecx, 1);
    }

    [[nodiscard]] bool supports_avx_state() noexcept {
        const auto regs = cpuid(1);
        if (!has_bit(regs.ecx, 26) || !has_bit(regs.ecx, 27) || !has_bit(regs.ecx, 28)) { return false; }
        return (xgetbv0() & 0x6) == 0x6;
    }

    [[nodiscard]] bool supports_avx2_pclmul_crc() noexcept {
        if (!supports_sse42() || !supports_pclmulqdq() || !supports_avx_state()) { return false; }
        const auto regs = cpuid(7, 0);
        return has_bit(regs.ebx, 5);
    }

    [[nodiscard]] bool supports_avx512_crc() noexcept {
        #if defined(__x86_64__) || defined(_M_X64)
        if (!supports_sse42()) { return false; }
        const auto leaf1 = cpuid(1);
        if (!has_bit(leaf1.ecx, 26) || !has_bit(leaf1.ecx, 27) || !has_bit(leaf1.ecx, 28)) { return false; }
        if ((xgetbv0() & 0xE6) != 0xE6) { return false; }

        const auto leaf7 = cpuid(7, 0);
        return has_bit(leaf7.ebx, 16) && has_bit(leaf7.ebx, 31) && has_bit(leaf7.ecx, 10);
        #else
        return false;
        #endif
    }

    [[nodiscard]] std::vector<size_t> test_sizes() {
        std::vector<size_t> sizes;
        sizes.reserve(4108);
        for (size_t size = 0; size <= 4096; ++size) { sizes.push_back(size); }
        for (const size_t size : std::array<size_t, 11>{4097, 8191, 8192, 8193, 16384, 65535, 65536, 65537, 262144, 524289, 1 << 20}) {
            sizes.push_back(size);
        }
        return sizes;
    }

    [[nodiscard]] std::vector<uint8_t> test_bytes() {
        std::vector<uint8_t> bytes((1 << 20) + 64);
        uint32_t state = 0x12345678u;
        for (auto& byte : bytes) {
            state = state * 1664525u + 1013904223u;
            byte = static_cast<uint8_t>(state >> 24);
        }
        return bytes;
    }
} // namespace

int main() {
    const auto bytes = test_bytes();
    const auto sizes = test_sizes();

    for (size_t offset = 0; offset < 64; ++offset) {
        for (const size_t size : sizes) {
            const auto* data = reinterpret_cast<const std::byte*>(bytes.data() + offset);
            const uint32_t expected = akkaradb::cpu::CRC32C_Ref(data, size);

            const auto check = [&](const char* name, uint32_t actual) -> bool {
                if (actual == expected) { return true; }

                std::cerr << name << " mismatch offset=" << offset << " size=" << size
                          << " expected=" << expected << " actual=" << actual << '\n';
                return false;
            };

            if (!check("dispatch", akkaradb::cpu::CRC32C(data, size))) { return 1; }

            #if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
            if (supports_sse42() && !check("sse4.2", akkaradb::cpu::CRC32C_X86_SSE42(data, size))) { return 1; }
            if (supports_avx2_pclmul_crc() && !check("avx2", akkaradb::cpu::CRC32C_X86_AVX2(data, size))) { return 1; }
            #endif

            #if defined(__x86_64__) || defined(_M_X64)
            if (supports_avx512_crc() && !check("avx512", akkaradb::cpu::CRC32C_X86_AVX512(data, size))) { return 1; }
            #endif
        }
    }

    return 0;
}
