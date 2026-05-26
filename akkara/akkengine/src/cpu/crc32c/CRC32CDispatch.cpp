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

// akkengine/src/cpu/crc32c/CRC32CDispatch.cpp
#include "akk/cpu/CRC32C.hpp"

#include <cstddef>
#include <cstdint>

#if defined(_MSC_VER)
#  include <intrin.h>
#endif

#if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
#  if defined(__GNUC__) || defined(__clang__)
#    include <cpuid.h>
#  endif
#endif

#if defined(__aarch64__) && defined(__linux__)
#  include <sys/auxv.h>
#  include <asm/hwcap.h>
#endif

namespace akkaradb::cpu {
    uint32_t CRC32C_Ref(const std::byte* data, size_t length) noexcept;

    #if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
    uint32_t CRC32C_X86_SSE42(const std::byte* data, size_t length) noexcept;
    uint32_t CRC32C_X86_AVX2(const std::byte* data, size_t length) noexcept;
    uint32_t CRC32C_X86_AVX512(const std::byte* data, size_t length) noexcept;
    #endif

    #if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    uint32_t CRC32C_ARM_CRC(const std::byte* data, size_t length) noexcept;
    #endif

    namespace {
        using Fn = uint32_t (*)(const std::byte*, size_t) noexcept;

        #if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
        struct CpuRegs {
            int eax = 0;
            int ebx = 0;
            int ecx = 0;
            int edx = 0;
        };

        [[nodiscard]] CpuRegs Cpuid(int leaf, int subleaf = 0) noexcept {
            CpuRegs regs{};
        #if defined(__GNUC__) || defined(__clang__)
            unsigned int eax = 0;
            unsigned int ebx = 0;
            unsigned int ecx = 0;
            unsigned int edx = 0;
            __cpuid_count(static_cast<unsigned int>(leaf), static_cast<unsigned int>(subleaf), eax, ebx, ecx, edx);
            regs.eax = static_cast<int>(eax);
            regs.ebx = static_cast<int>(ebx);
            regs.ecx = static_cast<int>(ecx);
            regs.edx = static_cast<int>(edx);
        #elif defined(_MSC_VER)
            int raw[4]{};
            __cpuidex(raw, leaf, subleaf);
            regs.eax = raw[0];
            regs.ebx = raw[1];
            regs.ecx = raw[2];
            regs.edx = raw[3];
        #endif
            return regs;
        }

        [[nodiscard]] uint64_t Xgetbv0() noexcept {
        #if defined(_MSC_VER)
            return _xgetbv(0);
        #elif defined(__GNUC__) || defined(__clang__)
            uint32_t eax = 0;
            uint32_t edx = 0;
            __asm__ volatile("xgetbv" : "=a"(eax), "=d"(edx) : "c"(0));
            return (static_cast<uint64_t>(edx) << 32) | eax;
        #else
            return 0;
        #endif
        }

        [[nodiscard]] bool HasBit(int value, int bit) noexcept {
            return (static_cast<uint32_t>(value) & (uint32_t{1} << bit)) != 0;
        }

        [[nodiscard]] bool SupportsSSE42() noexcept {
            const auto regs = Cpuid(1);
            return HasBit(regs.ecx, 20);
        }

        [[nodiscard]] bool SupportsAVXState() noexcept {
            const auto regs = Cpuid(1);
            if (!HasBit(regs.ecx, 26) || !HasBit(regs.ecx, 27) || !HasBit(regs.ecx, 28)) { return false; }
            return (Xgetbv0() & 0x6) == 0x6;
        }

        [[nodiscard]] bool SupportsAVX2() noexcept {
            if (!SupportsAVXState()) { return false; }
            const auto regs = Cpuid(7, 0);
            return HasBit(regs.ebx, 5);
        }

        [[nodiscard]] bool SupportsAVX512() noexcept {
            const auto leaf1 = Cpuid(1);
            if (!HasBit(leaf1.ecx, 26) || !HasBit(leaf1.ecx, 27) || !HasBit(leaf1.ecx, 28)) { return false; }
            if ((Xgetbv0() & 0xE6) != 0xE6) { return false; }

            const auto leaf7 = Cpuid(7, 0);
            const bool avx512f = HasBit(leaf7.ebx, 16);
            const bool vpclmulqdq = HasBit(leaf7.ecx, 10);
            return avx512f && vpclmulqdq;
        }
        #endif

        #if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
        /**
         * @brief Checks whether AArch64 CRC instructions are available.
         *
         * @return true if CRC instructions can be used.
         */
        [[nodiscard]] bool SupportsARMCRC() noexcept {
        #if defined(__linux__)
        return (getauxval(AT_HWCAP) &HWCAP_CRC32) != 0;
        #else
        return true;
        #endif
        }
        #endif

        /**
         * @brief Resolves the best available CRC32C implementation.
         *
         * @return Function pointer to the selected implementation.
         */
        [[nodiscard]] Fn Resolve() noexcept {
            #if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
            if (SupportsAVX512()) { return &CRC32C_X86_AVX512; }
            if (SupportsAVX2()) { return &CRC32C_X86_AVX2; }
            if (SupportsSSE42()) { return &CRC32C_X86_SSE42; }
            #endif

            #if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
            if (SupportsARMCRC()) { return &CRC32C_ARM_CRC; }
            #endif

            return &CRC32C_Ref;
        }
    } // namespace

    /**
     * @brief Public CRC32C entry point.
     *
     * The selected implementation is cached after the first call.
     *
     * @param data Pointer to the input bytes.
     * @param length Number of bytes to process.
     * @return CRC32C checksum for the input.
     */
    [[nodiscard]] uint32_t CRC32C(const std::byte* data, size_t length) noexcept {
        static const Fn fn = Resolve();
        return fn(data, length);
    }
} // namespace akkaradb::cpu
