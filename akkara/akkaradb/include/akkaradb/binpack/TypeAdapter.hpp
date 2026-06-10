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

// akkaradb/include/akkaradb/binpack/TypeAdapter.hpp
#pragma once

#include "detail/WireHelpers.hpp"

#include <array>
#include <cstring>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/pfr.hpp>

namespace akkaradb::binpack {
    namespace detail {
        template <typename T>
        inline constexpr bool alwaysFalse = false;

        template <typename T>
        inline constexpr bool aggregateMemcpyFastPath = std::is_aggregate_v<T> && std::is_trivially_copyable_v<T> && !std::is_array_v<T>;
    } // namespace detail

    template <typename T>
    struct TypeAdapter {
        template <typename Out>
        static void write(const T& v, Out& out) {
            if constexpr (std::is_enum_v<T>) {
                TypeAdapter<std::underlying_type_t<T>>::write(static_cast<std::underlying_type_t<T>>(v), out);
            }
            else if constexpr (detail::aggregateMemcpyFastPath<T>) {
                const auto* p = reinterpret_cast<const uint8_t*>(&v);
                out.insert(out.end(), p, p + sizeof(T));
            }
            else if constexpr (std::is_aggregate_v<T> && !std::is_array_v<T>) {
                boost::pfr::for_each_field(
                    v,
                    [&out](const auto& field) { TypeAdapter<std::remove_cvref_t<decltype(field)>>::write(field, out); }
                );
            }
            else { static_assert(detail::alwaysFalse<T>, "BinPack: no TypeAdapter for this type"); }
        }

        static T read(std::span<const uint8_t>& in) {
            if constexpr (std::is_enum_v<T>) { return static_cast<T>(TypeAdapter<std::underlying_type_t<T>>::read(in)); }
            else if constexpr (detail::aggregateMemcpyFastPath<T>) {
                if (in.size() < sizeof(T)) { throw std::runtime_error("BinPack: buffer underflow (trivial aggregate)"); }
                T out;
                std::memcpy(&out, in.data(), sizeof(T));
                in = in.subspan(sizeof(T));
                return out;
            }
            else if constexpr (std::is_aggregate_v<T> && !std::is_array_v<T>) {
                static_assert(std::is_default_constructible_v<T>, "BinPack aggregate read requires default construction");
                T out{};
                boost::pfr::for_each_field(
                    out,
                    [&in](auto& field) { field = TypeAdapter<std::remove_cvref_t<decltype(field)>>::read(in); }
                );
                return out;
            }
            else { static_assert(detail::alwaysFalse<T>, "BinPack: no TypeAdapter for this type"); }
        }

        static bool readInto(std::span<const uint8_t>& in, T& out) {
            if constexpr (detail::aggregateMemcpyFastPath<T>) {
                if (in.size() < sizeof(T)) { throw std::runtime_error("BinPack: buffer underflow (trivial aggregate)"); }
                std::memcpy(&out, in.data(), sizeof(T));
                in = in.subspan(sizeof(T));
                return true;
            }
            else {
                out = read(in);
                return true;
            }
        }

        static size_t estimateSize(const T& v) {
            if constexpr (std::is_enum_v<T>) { return sizeof(std::underlying_type_t<T>); }
            else if constexpr (detail::aggregateMemcpyFastPath<T>) {
                (void)v;
                return sizeof(T);
            }
            else if constexpr (std::is_aggregate_v<T> && !std::is_array_v<T>) {
                size_t total = 0;
                boost::pfr::for_each_field(
                    v,
                    [&total](const auto& field) { total += TypeAdapter<std::remove_cvref_t<decltype(field)>>::estimateSize(field); }
                );
                return total;
            }
            else { static_assert(detail::alwaysFalse<T>, "BinPack: no TypeAdapter for this type"); }
        }
    };

    template <>
    struct TypeAdapter<bool> {
        template <typename Out>
        static void write(bool v, Out& out) { out.push_back(v ? 1u : 0u); }

        static bool read(std::span<const uint8_t>& in) { return detail::readU8(in) != 0; }

        static bool readInto(std::span<const uint8_t>& in, bool& out) {
            out = read(in);
            return true;
        }

        static size_t estimateSize(bool) { return 1; }
    };

    #define AKKARADB_BINPACK_INT_ADAPTER(Type, WriteFn, ReadFn) \
    template <> struct TypeAdapter<Type> { \
        template <typename Out> \
        static void write(Type v, Out& out) { detail::WriteFn(static_cast<std::make_unsigned_t<Type>>(v), out); } \
        static Type read(std::span<const uint8_t>& in) { return static_cast<Type>(detail::ReadFn(in)); } \
        static bool readInto(std::span<const uint8_t>& in, Type& out) { out = read(in); return true; } \
        static size_t estimateSize(Type) { return sizeof(Type); } \
    }

    AKKARADB_BINPACK_INT_ADAPTER(uint8_t, writeU8, readU8);

    AKKARADB_BINPACK_INT_ADAPTER(uint16_t, writeU16, readU16);

    AKKARADB_BINPACK_INT_ADAPTER(uint32_t, writeU32, readU32);

    AKKARADB_BINPACK_INT_ADAPTER(uint64_t, writeU64, readU64);

    AKKARADB_BINPACK_INT_ADAPTER(int8_t, writeU8, readU8);

    AKKARADB_BINPACK_INT_ADAPTER(int16_t, writeU16, readU16);

    AKKARADB_BINPACK_INT_ADAPTER(int32_t, writeU32, readU32);

    AKKARADB_BINPACK_INT_ADAPTER(int64_t, writeU64, readU64);

    #undef AKKARADB_BINPACK_INT_ADAPTER

    template <>
    struct TypeAdapter<float> {
        template <typename Out>
        static void write(float v, Out& out) {
            uint32_t bits;
            std::memcpy(&bits, &v, sizeof(bits));
            detail::writeU32(bits, out);
        }

        static float read(std::span<const uint8_t>& in) {
            const uint32_t bits = detail::readU32(in);
            float out;
            std::memcpy(&out, &bits, sizeof(out));
            return out;
        }

        static bool readInto(std::span<const uint8_t>& in, float& out) {
            out = read(in);
            return true;
        }

        static size_t estimateSize(float) { return 4; }
    };

    template <>
    struct TypeAdapter<double> {
        template <typename Out>
        static void write(double v, Out& out) {
            uint64_t bits;
            std::memcpy(&bits, &v, sizeof(bits));
            detail::writeU64(bits, out);
        }

        static double read(std::span<const uint8_t>& in) {
            const uint64_t bits = detail::readU64(in);
            double out;
            std::memcpy(&out, &bits, sizeof(out));
            return out;
        }

        static bool readInto(std::span<const uint8_t>& in, double& out) {
            out = read(in);
            return true;
        }

        static size_t estimateSize(double) { return 8; }
    };

    template <>
    struct TypeAdapter<std::string> {
        template <typename Out>
        static void write(const std::string& v, Out& out) {
            detail::writeU32(static_cast<uint32_t>(v.size()), out);
            out.insert(out.end(), reinterpret_cast<const uint8_t*>(v.data()), reinterpret_cast<const uint8_t*>(v.data()) + v.size());
        }

        static std::string read(std::span<const uint8_t>& in) {
            const uint32_t len = detail::readU32(in);
            if (in.size() < len) { throw std::runtime_error("BinPack: buffer underflow (string)"); }
            std::string out(reinterpret_cast<const char*>(in.data()), len);
            in = in.subspan(len);
            return out;
        }

        static bool readInto(std::span<const uint8_t>& in, std::string& out) {
            out = read(in);
            return true;
        }

        static size_t estimateSize(const std::string& v) { return 4 + v.size(); }
    };

    template <>
    struct TypeAdapter<std::string_view> {
        template <typename Out>
        static void write(std::string_view v, Out& out) {
            detail::writeU32(static_cast<uint32_t>(v.size()), out);
            out.insert(out.end(), reinterpret_cast<const uint8_t*>(v.data()), reinterpret_cast<const uint8_t*>(v.data()) + v.size());
        }

        static std::string_view read(std::span<const uint8_t>&) = delete;
        static size_t estimateSize(std::string_view v) { return 4 + v.size(); }
    };

    template <>
    struct TypeAdapter<std::vector<uint8_t>> {
        template <typename Out>
        static void write(const std::vector<uint8_t>& v, Out& out) {
            detail::writeU32(static_cast<uint32_t>(v.size()), out);
            out.insert(out.end(), v.begin(), v.end());
        }

        static std::vector<uint8_t> read(std::span<const uint8_t>& in) {
            const uint32_t len = detail::readU32(in);
            if (in.size() < len) { throw std::runtime_error("BinPack: buffer underflow (bytes)"); }
            std::vector<uint8_t> out(in.data(), in.data() + len);
            in = in.subspan(len);
            return out;
        }

        static bool readInto(std::span<const uint8_t>& in, std::vector<uint8_t>& out) {
            out = read(in);
            return true;
        }

        static size_t estimateSize(const std::vector<uint8_t>& v) { return 4 + v.size(); }
    };

    template <typename T>
    struct TypeAdapter<std::optional<T>> {
        template <typename Out>
        static void write(const std::optional<T>& v, Out& out) {
            out.push_back(v.has_value() ? 1u : 0u);
            if (v) { TypeAdapter<T>::write(*v, out); }
        }

        static std::optional<T> read(std::span<const uint8_t>& in) {
            if (detail::readU8(in) == 0) { return std::nullopt; }
            return TypeAdapter<T>::read(in);
        }

        static bool readInto(std::span<const uint8_t>& in, std::optional<T>& out) {
            out = read(in);
            return true;
        }

        static size_t estimateSize(const std::optional<T>& v) { return 1 + (v ? TypeAdapter<T>::estimateSize(*v) : 0); }
    };

    template <typename T>
    struct TypeAdapter<std::vector<T>> {
        template <typename Out>
        static void write(const std::vector<T>& v, Out& out) {
            detail::writeU32(static_cast<uint32_t>(v.size()), out);
            for (const auto& e : v) { TypeAdapter<T>::write(e, out); }
        }

        static std::vector<T> read(std::span<const uint8_t>& in) {
            const uint32_t count = detail::readU32(in);
            std::vector<T> out;
            out.reserve(count);
            for (uint32_t i = 0; i < count; ++i) { out.push_back(TypeAdapter<T>::read(in)); }
            return out;
        }

        static bool readInto(std::span<const uint8_t>& in, std::vector<T>& out) {
            out = read(in);
            return true;
        }

        static size_t estimateSize(const std::vector<T>& v) {
            size_t total = 4;
            for (const auto& e : v) { total += TypeAdapter<T>::estimateSize(e); }
            return total;
        }
    };

    template <typename T, size_t N>
    struct TypeAdapter<std::array<T, N>> {
        template <typename Out>
        static void write(const std::array<T, N>& v, Out& out) { for (const auto& e : v) { TypeAdapter<T>::write(e, out); } }

        static std::array<T, N> read(std::span<const uint8_t>& in) {
            std::array<T, N> out{};
            for (auto& e : out) { e = TypeAdapter<T>::read(in); }
            return out;
        }

        static bool readInto(std::span<const uint8_t>& in, std::array<T, N>& out) {
            out = read(in);
            return true;
        }

        static size_t estimateSize(const std::array<T, N>& v) {
            size_t total = 0;
            for (const auto& e : v) { total += TypeAdapter<T>::estimateSize(e); }
            return total;
        }
    };

    template <typename K, typename V, typename C, typename A>
    struct TypeAdapter<std::map<K, V, C, A>> {
        template <typename Out>
        static void write(const std::map<K, V, C, A>& v, Out& out) {
            detail::writeU32(static_cast<uint32_t>(v.size()), out);
            for (const auto& [k, val] : v) {
                TypeAdapter<K>::write(k, out);
                TypeAdapter<V>::write(val, out);
            }
        }

        static std::map<K, V, C, A> read(std::span<const uint8_t>& in) {
            const uint32_t count = detail::readU32(in);
            std::map<K, V, C, A> out;
            for (uint32_t i = 0; i < count; ++i) {
                auto key = TypeAdapter<K>::read(in);
                auto value = TypeAdapter<V>::read(in);
                out.emplace(std::move(key), std::move(value));
            }
            return out;
        }

        static bool readInto(std::span<const uint8_t>& in, std::map<K, V, C, A>& out) {
            out = read(in);
            return true;
        }

        static size_t estimateSize(const std::map<K, V, C, A>& v) {
            size_t total = 4;
            for (const auto& [k, val] : v) { total += TypeAdapter<K>::estimateSize(k) + TypeAdapter<V>::estimateSize(val); }
            return total;
        }
    };

    template <typename K, typename V, typename H, typename E, typename A>
    struct TypeAdapter<std::unordered_map<K, V, H, E, A>> {
        template <typename Out>
        static void write(const std::unordered_map<K, V, H, E, A>& v, Out& out) {
            detail::writeU32(static_cast<uint32_t>(v.size()), out);
            for (const auto& [k, val] : v) {
                TypeAdapter<K>::write(k, out);
                TypeAdapter<V>::write(val, out);
            }
        }

        static std::unordered_map<K, V, H, E, A> read(std::span<const uint8_t>& in) {
            const uint32_t count = detail::readU32(in);
            std::unordered_map<K, V, H, E, A> out;
            out.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                auto key = TypeAdapter<K>::read(in);
                auto value = TypeAdapter<V>::read(in);
                out.emplace(std::move(key), std::move(value));
            }
            return out;
        }

        static bool readInto(std::span<const uint8_t>& in, std::unordered_map<K, V, H, E, A>& out) {
            out = read(in);
            return true;
        }

        static size_t estimateSize(const std::unordered_map<K, V, H, E, A>& v) {
            size_t total = 4;
            for (const auto& [k, val] : v) { total += TypeAdapter<K>::estimateSize(k) + TypeAdapter<V>::estimateSize(val); }
            return total;
        }
    };

    template <typename A, typename B>
    struct TypeAdapter<std::pair<A, B>> {
        template <typename Out>
        static void write(const std::pair<A, B>& v, Out& out) {
            TypeAdapter<A>::write(v.first, out);
            TypeAdapter<B>::write(v.second, out);
        }

        static std::pair<A, B> read(std::span<const uint8_t>& in) {
            A a = TypeAdapter<A>::read(in);
            B b = TypeAdapter<B>::read(in);
            return {std::move(a), std::move(b)};
        }

        static bool readInto(std::span<const uint8_t>& in, std::pair<A, B>& out) {
            out = read(in);
            return true;
        }

        static size_t estimateSize(const std::pair<A, B>& v) {
            return TypeAdapter<A>::estimateSize(v.first) + TypeAdapter<B>::estimateSize(v.second);
        }
    };

    template <typename... Ts>
    struct TypeAdapter<std::tuple<Ts...>> {
        using Tuple = std::tuple<Ts...>;

        template <typename Out>
        static void write(const Tuple& v, Out& out) {
            std::apply([&out](const auto&... args) { (TypeAdapter<std::remove_cvref_t<decltype(args)>>::write(args, out), ...); }, v);
        }

        static Tuple read(std::span<const uint8_t>& in) { return readImpl(in, std::index_sequence_for<Ts...>{}); }

        static bool readInto(std::span<const uint8_t>& in, Tuple& out) {
            out = read(in);
            return true;
        }

        static size_t estimateSize(const Tuple& v) {
            size_t total = 0;
            std::apply(
                [&total](const auto&... args) { ((total += TypeAdapter<std::remove_cvref_t<decltype(args)>>::estimateSize(args)), ...); },
                v
            );
            return total;
        }

        private:
            template <size_t... Is>
            static Tuple readImpl(std::span<const uint8_t>& in, std::index_sequence<Is...>) {
                Tuple out{};
                ((std::get<Is>(out) = TypeAdapter<std::tuple_element_t<Is, Tuple>>::read(in)), ...);
                return out;
            }
    };
} // namespace akkaradb::binpack
