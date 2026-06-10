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

// akkaradb/include/akkaradb/binpack/BinPack.hpp
#pragma once

#include "TypeAdapter.hpp"

#include <span>
#include <vector>

namespace akkaradb::binpack {
    struct BinPack {
        template <typename T>
        [[nodiscard]] static std::vector<uint8_t> encode(const T& value) {
            std::vector<uint8_t> out;
            out.reserve(TypeAdapter<T>::estimateSize(value));
            TypeAdapter<T>::write(value, out);
            return out;
        }

        template <typename T, typename Out>
        static void encodeInto(const T& value, Out& out) { TypeAdapter<T>::write(value, out); }

        template <typename T>
        [[nodiscard]] static T decode(std::span<const uint8_t> bytes) { return TypeAdapter<T>::read(bytes); }

        template <typename T>
        [[nodiscard]] static T decode(const std::vector<uint8_t>& bytes) {
            return decode<T>(std::span<const uint8_t>{bytes.data(), bytes.size()});
        }

        template <typename T>
        static bool decodeInto(std::span<const uint8_t> bytes, T& out) { return TypeAdapter<T>::readInto(bytes, out); }

        template <typename T>
        [[nodiscard]] static size_t estimateSize(const T& value) { return TypeAdapter<T>::estimateSize(value); }

        BinPack() = delete;
    };
} // namespace akkaradb::binpack
