/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/core/buffer/BufferView.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <type_traits>

namespace akkaradb::core {
    class OwnedBuffer;

    /**
     * @brief Non-owning, read-only view over a contiguous byte region.
     *
     * BufferView provides zero-copy access to a byte sequence without owning it.
     *
     * Design principles:
     * - Non-owning: lifetime is managed externally
     * - Read-only: no mutation allowed through this type
     * - Trivially copyable: cheap to pass by value
     * - Little-Endian only (for multi-byte reads)
     *
     * @warning
     * The underlying memory is NOT owned. The data is only valid while the source
     * buffer remains alive.
     *
     * If you need to retain the data, call toOwned().
     */
    class AKDB_API BufferView {
        public:
            // ==================== Constructors ====================

            constexpr BufferView() noexcept : data_{nullptr}, size_{0} {}

            constexpr BufferView(const std::byte* data, size_t size) noexcept : data_{data}, size_{size} {}

            constexpr explicit BufferView(std::span<const std::byte> span) noexcept : data_{span.data()}, size_{span.size()} {}

            // ==================== Basic Accessors ====================

            /**
             * @brief Returns the start address of the viewed byte sequence.
             *
             * @return Pointer to the first byte, or nullptr when the view is empty.
             */
            [[nodiscard]] constexpr const std::byte* data() const noexcept { return data_; }

            /**
             * @brief Returns the number of bytes in the view.
             */
            [[nodiscard]] constexpr size_t size() const noexcept { return size_; }

            /**
             * @brief Reports whether the view contains no bytes.
             */
            [[nodiscard]] constexpr bool empty() const noexcept { return size_ == 0; }

            /**
             * @brief Returns the contents as a byte span.
             *
             * @return A span referencing the same underlying storage.
             */
            [[nodiscard]] constexpr std::span<const std::byte> asSpan() const noexcept { return {data_, size_}; }

            /**
             * @brief Returns a typed span view (byte-compatible types only).
             *
             * Only char / unsigned char / std::byte are allowed to avoid
             * strict aliasing violations.
             */
            template <typename T>
            [[nodiscard]] constexpr std::span<const T> asSpan() const noexcept {
                static_assert(
                    std::is_same_v<T, std::byte> || std::is_same_v<T, char> || std::is_same_v<T, unsigned char>,
                    "T must be a byte-like type"
                );
                return {reinterpret_cast<const T*>(data_), size_ / sizeof(T)};
            }

            // ==================== Slicing ====================

            /**
             * @brief Creates a sub-view from a byte range.
             *
             * @param offset Starting byte offset.
             * @param length Number of bytes to include.
             * @return A view over the requested sub-range.
             * @throws std::out_of_range If the requested range exceeds the current view.
             */
            [[nodiscard]] BufferView slice(size_t offset, size_t length) const;

            /**
             * @brief Creates a sub-view from @p offset to the end of the view.
             *
             * @param offset Starting byte offset.
             * @return A view over the suffix starting at @p offset.
             * @throws std::out_of_range If @p offset exceeds the current view.
             */
            [[nodiscard]] BufferView slice(size_t offset) const;

            // ==================== Ownership ====================

            /**
             * @brief Creates an owning copy of this view.
             *
             * Performs a deep copy of the underlying byte sequence.
             *
             * @return A newly allocated buffer containing the same bytes.
             */
            [[nodiscard]] OwnedBuffer toOwned() const;

            // ==================== Little-Endian Reads ====================

            /**
             * @brief Reads an unsigned 8-bit value at the given offset.
             *
             * @param offset Byte offset within the view.
             * @return The byte value at @p offset.
             * @throws std::out_of_range If the read would exceed the view.
             */
            [[nodiscard]] uint8_t readU8(size_t offset) const;

            /**
             * @brief Reads an unsigned 16-bit little-endian value at the given offset.
             *
             * @param offset Byte offset within the view.
             * @return A 16-bit little-endian integer decoded from the buffer.
             * @throws std::out_of_range If the read would exceed the view.
             */
            [[nodiscard]] uint16_t readU16Le(size_t offset) const;

            /**
             * @brief Reads an unsigned 32-bit little-endian value at the given offset.
             *
             * @param offset Byte offset within the view.
             * @return A 32-bit little-endian integer decoded from the buffer.
             * @throws std::out_of_range If the read would exceed the view.
             */
            [[nodiscard]] uint32_t readU32Le(size_t offset) const;

            /**
             * @brief Reads an unsigned 64-bit little-endian value at the given offset.
             *
             * @param offset Byte offset within the view.
             * @return A 64-bit little-endian integer decoded from the buffer.
             * @throws std::out_of_range If the read would exceed the view.
             */
            [[nodiscard]] uint64_t readU64Le(size_t offset) const;

            // ==================== CRC ====================

            /**
             * @brief Computes the CRC32C checksum of a sub-range.
             *
             * @param offset Starting byte offset.
             * @param length Number of bytes to include.
             * @return CRC32C checksum of the requested range.
             * @throws std::out_of_range If the requested range exceeds the current view.
             */
            [[nodiscard]] uint32_t crc32c(size_t offset, size_t length) const;

            /**
             * @brief Computes the CRC32C checksum of the entire view.
             */
            [[nodiscard]] uint32_t crc32c() const noexcept { return crc32c(0, size_); }

            // ==================== String ====================

            /**
             * @brief Creates a string_view over a sub-range of the buffer.
             *
             * @param offset Starting byte offset.
             * @param length Number of bytes to include.
             * @return A non-owning string view referencing the same storage.
             * @throws std::out_of_range If the requested range exceeds the current view.
             */
            [[nodiscard]] std::string_view asStringView(size_t offset, size_t length) const;

            /**
             * @brief Creates a string_view over the entire buffer.
             *
             * @warning Not null-terminated. Lifetime follows BufferView.
             */
            [[nodiscard]] std::string_view asStringView() const noexcept { return {reinterpret_cast<const char*>(data_), size_}; }

        private:
            const std::byte* data_;
            size_t size_;

            /**
             * @brief Validates that a range is fully contained in the view.
             *
             * @param offset Starting byte offset.
             * @param length Number of bytes to cover.
             * @throws std::out_of_range If the range exceeds the current view.
             */
            void checkBounds(size_t offset, size_t length) const;

            /**
             * @brief Fast path wrapper around @ref checkBounds.
             *
             * The validation is always enforced, including release builds.
             */
            void checkBoundsInline(size_t offset, size_t length) const { checkBounds(offset, length); }
    };

    static_assert(std::is_trivially_copyable_v<BufferView>);
    static_assert(std::is_trivially_destructible_v<BufferView>);
} // namespace akkaradb::core
