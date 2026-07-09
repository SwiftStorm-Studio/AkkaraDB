/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/query/Bytecode.hpp
#pragma once

#include "akkaradb/binpack/detail/MemberPtrTraits.hpp"
#include "akkaradb/binpack/BinPack.hpp"

#include <algorithm>
#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace akkaradb::query::bytecode {
    enum class ValueKind : uint8_t {
        Null,
        Bool,
        Int,
        UInt,
        Double,
        String
    };

    struct Value {
        ValueKind kind = ValueKind::Null;
        bool b = false;
        int64_t i = 0;
        uint64_t u = 0;
        double d = 0.0;
        std::string_view s;

        [[nodiscard]] static constexpr Value null() noexcept { return {}; }

        [[nodiscard]] static constexpr Value boolean(bool value) noexcept {
            Value out;
            out.kind = ValueKind::Bool;
            out.b = value;
            return out;
        }

        [[nodiscard]] static constexpr Value integer(int64_t value) noexcept {
            Value out;
            out.kind = ValueKind::Int;
            out.i = value;
            return out;
        }

        [[nodiscard]] static constexpr Value uinteger(uint64_t value) noexcept {
            Value out;
            out.kind = ValueKind::UInt;
            out.u = value;
            return out;
        }

        [[nodiscard]] static constexpr Value floating(double value) noexcept {
            Value out;
            out.kind = ValueKind::Double;
            out.d = value;
            return out;
        }

        [[nodiscard]] static constexpr Value string(std::string_view value) noexcept {
            Value out;
            out.kind = ValueKind::String;
            out.s = value;
            return out;
        }

        [[nodiscard]] constexpr bool isNumeric() const noexcept {
            return kind == ValueKind::Int || kind == ValueKind::UInt || kind == ValueKind::Double;
        }
    };

    enum class Opcode : uint8_t {
        PushConst = 0x01,
        LoadField = 0x02,
        Eq = 0x03,
        Ne = 0x04,
        Lt = 0x05,
        Le = 0x06,
        Gt = 0x07,
        Ge = 0x08,
        Not = 0x09,
        And = 0x0A,
        Or = 0x0B,
        Jump = 0x0C,
        JumpIfFalse = 0x0D,
        JumpIfTrue = 0x0E,
        HostCallBool = 0x0F,
        Return = 0x10,
        Pop = 0x11,
        StartsWith = 0x12,
        EndsWith = 0x13,
        Contains = 0x14,
        Like = 0x15,
        CallCustom = 0x16,
        Add = 0x17,
        Sub = 0x18,
        Mul = 0x19,
        Div = 0x1A,
        Mod = 0x1B
    };

    inline constexpr uint16_t customOpcodeUserMin = 0x8000;
    inline constexpr uint16_t customOpcodeUserMax = 0xFFFF;
    inline constexpr uint8_t customOpcodeMaxArity = 16;

    enum class PlanHintOp : uint8_t {
        Eq,
        Lt,
        Le,
        Gt,
        Ge,
        StartsWith
    };

    struct PlanHint {
        PlanHintOp op = PlanHintOp::Eq;
        uint16_t field = 0;
        uint16_t constant = 0;
    };

    template <typename T>
    struct IsOptional : std::false_type {};

    template <typename T>
    struct IsOptional<std::optional<T>> : std::true_type {};

    template <typename T>
    inline constexpr bool isOptionalV = IsOptional<std::remove_cvref_t<T>>::value;

    template <typename T>
    [[nodiscard]] Value valueFrom(const T& value);

    template <typename T>
    [[nodiscard]] Value scalarValueFrom(const T& value) {
        using V = std::remove_cvref_t<T>;
        if constexpr (std::is_same_v<V, bool>) { return Value::boolean(value); }
        else if constexpr (std::is_integral_v<V> && std::is_signed_v<V>) { return Value::integer(static_cast<int64_t>(value)); }
        else if constexpr (std::is_integral_v<V> && std::is_unsigned_v<V>) { return Value::uinteger(static_cast<uint64_t>(value)); }
        else if constexpr (std::is_floating_point_v<V>) { return Value::floating(static_cast<double>(value)); }
        else if constexpr (requires { std::string_view{value}; }) { return Value::string(std::string_view{value}); }
        else {
            static_assert(sizeof(V) == 0, "AkkaraDB bytecode field type is not supported yet");
        }
    }

    template <typename T>
    [[nodiscard]] Value valueFrom(const T& value) {
        using V = std::remove_cvref_t<T>;
        if constexpr (isOptionalV<V>) {
            if (!value.has_value()) { return Value::null(); }
            return valueFrom(*value);
        }
        else { return scalarValueFrom(value); }
    }

    template <typename Entity>
    using FieldReader = Value (*)(const Entity&);

    using RawFieldReader = Value (*)(std::span<const uint8_t>);

    template <typename Entity>
    using HostCallThunk = bool (*)(const Entity&, const void*);

    using CustomOpcodeThunk = bool (*)(std::span<const Value> args, Value& out, const void* captures);

    using IndexValueEncoder = bool (*)(const Value&, std::vector<uint8_t>&);

    struct CustomOpcodeBinding {
        uint16_t opcode = 0;
        std::string_view name;
        uint8_t arity = 0;
        ValueKind resultKind = ValueKind::Bool;
        CustomOpcodeThunk thunk = nullptr;
    };

    template <
        auto Function,
        uint16_t OpcodeValue,
        uint8_t ArityValue,
        ValueKind ResultKindValue,
        CustomOpcodeThunk ThunkValue>
    struct StaticCustomOpcodeRegistration {
        static constexpr auto function = Function;
        static constexpr uint16_t opcode = OpcodeValue;
        static constexpr uint8_t arity = ArityValue;
        static constexpr ValueKind resultKind = ResultKindValue;
        static constexpr CustomOpcodeThunk thunk = ThunkValue;

        std::string_view name;

        constexpr explicit StaticCustomOpcodeRegistration(std::string_view nameValue) noexcept
            : name{nameValue} {}

        [[nodiscard]] constexpr CustomOpcodeBinding binding() const noexcept {
            return CustomOpcodeBinding{opcode, name, arity, resultKind, thunk};
        }

        [[nodiscard]] constexpr operator CustomOpcodeBinding() const noexcept { return binding(); }
    };

    class CustomOpcodeRegistry {
    public:
        bool registerOpcode(CustomOpcodeBinding binding, std::ostream* log = &std::clog) {
            if (!validate(binding, log)) { return false; }
            bindings_.push_back(binding);
            return true;
        }

        [[nodiscard]] std::span<const CustomOpcodeBinding> bindings() const noexcept { return bindings_; }

    private:
        [[nodiscard]] bool validate(const CustomOpcodeBinding& binding, std::ostream* log) const {
            auto reject = [&](std::string_view reason) {
                if (log != nullptr) {
                    *log << "[akkaradb::query::bytecode] rejected custom opcode";
                    if (!binding.name.empty()) { *log << " `" << binding.name << '`'; }
                    *log << " (" << binding.opcode << "): " << reason << '\n';
                }
                return false;
            };

            if (binding.opcode < customOpcodeUserMin) { return reject("opcode id must be in the user range 0x8000..0xFFFF"); }
            if (binding.name.empty()) { return reject("name must not be empty"); }
            if (binding.arity > customOpcodeMaxArity) { return reject("arity exceeds the documented maximum of 16"); }
            if (binding.thunk == nullptr) { return reject("thunk must not be null"); }

            for (const auto& existing : bindings_) {
                if (existing.opcode == binding.opcode) { return reject("opcode id is already registered"); }
                if (existing.name == binding.name) { return reject("name is already registered"); }
            }
            return true;
        }

        std::vector<CustomOpcodeBinding> bindings_;
    };

    template <typename Entity>
    struct FieldBinding {
        FieldReader<Entity> read = nullptr;
        RawFieldReader rawRead = nullptr;
        std::string_view name;
        IndexValueEncoder encodeIndexValue = nullptr;
        bool orderedIndexable = false;
    };

    template <auto FieldPtr>
    [[nodiscard]] Value readMemberField(const binpack::detail::classOf<FieldPtr>& entity) {
        return valueFrom(entity.*FieldPtr);
    }

    template <auto FieldPtr>
    [[nodiscard]] consteval FieldReader<binpack::detail::classOf<FieldPtr>> makeFieldReader() {
        return &readMemberField<FieldPtr>;
    }

    template <typename T>
    void skipWireValue(std::span<const uint8_t>& in);

    template <typename T, size_t... Is>
    void skipWireAggregate(std::span<const uint8_t>& in, std::index_sequence<Is...>);

    template <typename T>
    [[nodiscard]] Value readWireValue(std::span<const uint8_t>& in);

    inline void skipBytes(std::span<const uint8_t>& in, size_t size) {
        if (in.size() < size) { throw std::runtime_error("AkkaraDB bytecode raw row buffer underflow"); }
        in = in.subspan(size);
    }

    template <typename T>
    void skipWireValue(std::span<const uint8_t>& in) {
        using V = std::remove_cvref_t<T>;
        if constexpr (std::is_enum_v<V>) { skipWireValue<std::underlying_type_t<V>>(in); }
        else if constexpr (binpack::detail::aggregateMemcpyFastPath<V>) { skipBytes(in, sizeof(V)); }
        else if constexpr (std::is_same_v<V, bool> || std::is_same_v<V, uint8_t> || std::is_same_v<V, int8_t>) { skipBytes(in, 1); }
        else if constexpr (std::is_same_v<V, uint16_t> || std::is_same_v<V, int16_t>) { skipBytes(in, 2); }
        else if constexpr (std::is_same_v<V, uint32_t> || std::is_same_v<V, int32_t> || std::is_same_v<V, float>) { skipBytes(in, 4); }
        else if constexpr (std::is_same_v<V, uint64_t> || std::is_same_v<V, int64_t> || std::is_same_v<V, double>) { skipBytes(in, 8); }
        else if constexpr (std::is_same_v<V, std::string>) {
            const uint32_t len = binpack::detail::readU32(in);
            skipBytes(in, len);
        }
        else if constexpr (isOptionalV<V>) {
            const uint8_t present = binpack::detail::readU8(in);
            if (present != 0) { skipWireValue<typename V::value_type>(in); }
        }
        else if constexpr (requires { typename V::key_type; typename V::mapped_type; }) {
            const uint32_t count = binpack::detail::readU32(in);
            for (uint32_t i = 0; i < count; ++i) {
                skipWireValue<typename V::key_type>(in);
                skipWireValue<typename V::mapped_type>(in);
            }
        }
        else if constexpr (requires { typename V::value_type; std::declval<V>().begin(); std::declval<V>().end(); } && !std::is_same_v<V, std::string>) {
            using Elem = typename V::value_type;
            const uint32_t count = binpack::detail::readU32(in);
            for (uint32_t i = 0; i < count; ++i) { skipWireValue<Elem>(in); }
        }
        else if constexpr (requires { typename V::first_type; typename V::second_type; }) {
            skipWireValue<typename V::first_type>(in);
            skipWireValue<typename V::second_type>(in);
        }
        else if constexpr (std::is_aggregate_v<V> && !std::is_array_v<V>) {
            skipWireAggregate<V>(in, std::make_index_sequence<boost::pfr::tuple_size_v<V>>{});
        }
        else { static_assert(sizeof(V) == 0, "AkkaraDB bytecode raw skip does not support this field type yet"); }
    }

    template <typename T, size_t... Is>
    void skipWireAggregate(std::span<const uint8_t>& in, std::index_sequence<Is...>) {
        (void)sizeof...(Is);
        const uint32_t magic = binpack::detail::readU32(in);
        if (magic != binpack::detail::aggregateOffsetTableMagic) { throw std::runtime_error("AkkaraDB bytecode aggregate offset table magic mismatch"); }
        const uint32_t payloadSize = binpack::detail::readU32(in);
        skipBytes(in, boost::pfr::tuple_size_v<T> * 4 + payloadSize);
    }

    template <typename T>
    [[nodiscard]] Value readWireValue(std::span<const uint8_t>& in) {
        using V = std::remove_cvref_t<T>;
        if constexpr (std::is_same_v<V, bool>) { return Value::boolean(binpack::detail::readU8(in) != 0); }
        else if constexpr (std::is_same_v<V, uint8_t>) { return Value::uinteger(binpack::detail::readU8(in)); }
        else if constexpr (std::is_same_v<V, int8_t>) { return Value::integer(static_cast<int8_t>(binpack::detail::readU8(in))); }
        else if constexpr (std::is_same_v<V, uint16_t>) { return Value::uinteger(binpack::detail::readU16(in)); }
        else if constexpr (std::is_same_v<V, int16_t>) { return Value::integer(static_cast<int16_t>(binpack::detail::readU16(in))); }
        else if constexpr (std::is_same_v<V, uint32_t>) { return Value::uinteger(binpack::detail::readU32(in)); }
        else if constexpr (std::is_same_v<V, int32_t>) { return Value::integer(static_cast<int32_t>(binpack::detail::readU32(in))); }
        else if constexpr (std::is_same_v<V, uint64_t>) { return Value::uinteger(binpack::detail::readU64(in)); }
        else if constexpr (std::is_same_v<V, int64_t>) { return Value::integer(static_cast<int64_t>(binpack::detail::readU64(in))); }
        else if constexpr (std::is_same_v<V, float>) {
            const uint32_t bits = binpack::detail::readU32(in);
            float out;
            std::memcpy(&out, &bits, sizeof(out));
            return Value::floating(out);
        }
        else if constexpr (std::is_same_v<V, double>) {
            const uint64_t bits = binpack::detail::readU64(in);
            double out;
            std::memcpy(&out, &bits, sizeof(out));
            return Value::floating(out);
        }
        else if constexpr (std::is_same_v<V, std::string>) {
            const uint32_t len = binpack::detail::readU32(in);
            if (in.size() < len) { throw std::runtime_error("AkkaraDB bytecode raw string buffer underflow"); }
            const auto* data = reinterpret_cast<const char*>(in.data());
            in = in.subspan(len);
            return Value::string(std::string_view{data, len});
        }
        else if constexpr (isOptionalV<V>) {
            const uint8_t present = binpack::detail::readU8(in);
            if (present == 0) { return Value::null(); }
            return readWireValue<typename V::value_type>(in);
        }
        else {
            static_assert(sizeof(V) == 0, "AkkaraDB bytecode raw read does not support this field type yet");
        }
    }

    template <typename Entity, size_t FieldIndex>
    [[nodiscard]] Value readTopLevelField(std::span<const uint8_t> rowBytes) {
        static_assert(FieldIndex < boost::pfr::tuple_size_v<Entity>, "AkkaraDB bytecode raw field index is out of range");
        constexpr size_t fieldCount = boost::pfr::tuple_size_v<Entity>;
        const uint32_t magic = binpack::detail::readU32(rowBytes);
        if (magic != binpack::detail::aggregateOffsetTableMagic) { throw std::runtime_error("AkkaraDB bytecode aggregate offset table magic mismatch"); }
        const uint32_t payloadSize = binpack::detail::readU32(rowBytes);
        if (rowBytes.size() < fieldCount * 4 + payloadSize) { throw std::runtime_error("AkkaraDB bytecode aggregate offset table is truncated"); }

        const size_t offsetTableBytes = fieldCount * 4;
        const auto* offsets = rowBytes.data();
        const std::span<const uint8_t> payload{rowBytes.data() + offsetTableBytes, payloadSize};

        const uint32_t begin = binpack::detail::readU32At(offsets + FieldIndex * 4);
        const uint32_t end = FieldIndex + 1 < fieldCount ? binpack::detail::readU32At(offsets + (FieldIndex + 1) * 4) : payloadSize;
        if (begin > end || end > payloadSize) { throw std::runtime_error("AkkaraDB bytecode aggregate field offset is invalid"); }

        std::span<const uint8_t> fieldBytes{payload.data() + begin, end - begin};
        Value value = readWireValue<boost::pfr::tuple_element_t<FieldIndex, Entity>>(fieldBytes);
        if (!fieldBytes.empty()) { throw std::runtime_error("AkkaraDB bytecode aggregate field reader left trailing bytes"); }
        return value;
    }

    template <typename Entity, size_t FieldIndex>
    [[nodiscard]] consteval RawFieldReader makeRawFieldReader() {
        return &readTopLevelField<Entity, FieldIndex>;
    }

    [[nodiscard]] inline long double numericAsLongDouble(const Value& value);

    template <typename UInt, typename Out>
    void writeIndexBigEndian(UInt value, Out& out) {
        static_assert(std::is_unsigned_v<UInt>);
        for (size_t i = sizeof(UInt); i > 0; --i) { out.push_back(static_cast<uint8_t>(value >> ((i - 1) * 8))); }
    }

    template <typename Field>
    [[nodiscard]] bool valueToField(const Value& value, Field& out);

    template <typename Field>
    [[nodiscard]] bool valueToScalarField(const Value& value, Field& out) {
        using F = std::remove_cvref_t<Field>;
        if constexpr (std::is_same_v<F, bool>) {
            if (value.kind != ValueKind::Bool) { return false; }
            out = value.b;
            return true;
        }
        else if constexpr (std::is_integral_v<F> && std::is_signed_v<F>) {
            long double number = 0;
            if (value.kind == ValueKind::Int) { number = static_cast<long double>(value.i); }
            else if (value.kind == ValueKind::UInt) { number = static_cast<long double>(value.u); }
            else if (value.kind == ValueKind::Double) { number = static_cast<long double>(value.d); }
            else { return false; }
            if (number < static_cast<long double>(std::numeric_limits<F>::lowest()) || number > static_cast<long double>(std::numeric_limits<F>::max())) {
                return false;
            }
            out = static_cast<F>(number);
            return true;
        }
        else if constexpr (std::is_integral_v<F> && std::is_unsigned_v<F>) {
            long double number = 0;
            if (value.kind == ValueKind::Int) {
                if (value.i < 0) { return false; }
                number = static_cast<long double>(value.i);
            }
            else if (value.kind == ValueKind::UInt) { number = static_cast<long double>(value.u); }
            else if (value.kind == ValueKind::Double) { number = static_cast<long double>(value.d); }
            else { return false; }
            if (number < 0 || number > static_cast<long double>(std::numeric_limits<F>::max())) { return false; }
            out = static_cast<F>(number);
            return true;
        }
        else if constexpr (std::is_floating_point_v<F>) {
            if (!value.isNumeric()) { return false; }
            out = static_cast<F>(numericAsLongDouble(value));
            return true;
        }
        else if constexpr (std::is_same_v<F, std::string>) {
            if (value.kind != ValueKind::String) { return false; }
            out = std::string{value.s};
            return true;
        }
        else { return false; }
    }

    template <typename Field>
    [[nodiscard]] bool valueToField(const Value& value, Field& out) {
        using F = std::remove_cvref_t<Field>;
        if constexpr (isOptionalV<F>) {
            using Inner = typename F::value_type;
            if (value.kind == ValueKind::Null) {
                out = std::nullopt;
                return true;
            }
            Inner inner{};
            if (!valueToField(value, inner)) { return false; }
            out = std::move(inner);
            return true;
        }
        else { return valueToScalarField(value, out); }
    }

    template <typename Field>
    bool encodeIndexLiteralValue(const Value& value, std::vector<uint8_t>& out) {
        using F = std::remove_cvref_t<Field>;
        F field{};
        if (!valueToField(value, field)) { return false; }

        out.clear();
        if constexpr (std::is_integral_v<F> && !std::is_same_v<F, bool>) {
            using Unsigned = std::make_unsigned_t<F>;
            Unsigned sortable = static_cast<Unsigned>(field);
            if constexpr (std::is_signed_v<F>) { sortable ^= (Unsigned{1} << (sizeof(F) * 8 - 1)); }
            writeIndexBigEndian(sortable, out);
        }
        else if constexpr (std::is_same_v<F, float>) {
            uint32_t bits;
            std::memcpy(&bits, &field, sizeof(bits));
            constexpr uint32_t sign = uint32_t{1} << 31;
            bits = (bits & sign) != 0 ? ~bits : bits ^ sign;
            writeIndexBigEndian(bits, out);
        }
        else if constexpr (std::is_same_v<F, double>) {
            uint64_t bits;
            std::memcpy(&bits, &field, sizeof(bits));
            constexpr uint64_t sign = uint64_t{1} << 63;
            bits = (bits & sign) != 0 ? ~bits : bits ^ sign;
            writeIndexBigEndian(bits, out);
        }
        else { binpack::BinPack::encodeInto(field, out); }
        return true;
    }

    template <auto FieldPtr>
    [[nodiscard]] constexpr FieldBinding<binpack::detail::classOf<FieldPtr>> makeFieldBinding(std::string_view name) {
        using Field = binpack::detail::memberOf<FieldPtr>;
        return FieldBinding<binpack::detail::classOf<FieldPtr>>{
            &readMemberField<FieldPtr>,
            nullptr,
            name,
            &encodeIndexLiteralValue<Field>,
            (std::is_arithmetic_v<Field> && !std::is_same_v<Field, bool>)
        };
    }

    template <auto FieldPtr, size_t FieldIndex>
    [[nodiscard]] constexpr FieldBinding<binpack::detail::classOf<FieldPtr>> makeTopLevelFieldBinding(std::string_view name) {
        using Entity = binpack::detail::classOf<FieldPtr>;
        using Field = binpack::detail::memberOf<FieldPtr>;
        static_assert(std::is_same_v<std::remove_cvref_t<boost::pfr::tuple_element_t<FieldIndex, Entity>>, Field>);
        return FieldBinding<Entity>{
            &readMemberField<FieldPtr>,
            &readTopLevelField<Entity, FieldIndex>,
            name,
            &encodeIndexLiteralValue<Field>,
            (std::is_arithmetic_v<Field> && !std::is_same_v<Field, bool>)
        };
    }

    template <typename Entity>
    struct CompiledQueryDescriptor {
        std::span<const uint8_t> code;
        std::span<const Value> constants;
        std::span<const FieldBinding<Entity>> fields;
        std::span<const HostCallThunk<Entity>> hostCalls;
        std::span<const CustomOpcodeBinding> customOpcodes;
        std::span<const PlanHint> planHints;
        std::span<const void* const> hostCallCaptures = {};
        std::span<const void* const> customOpcodeCaptures = {};
        const void* captures = nullptr;
        std::shared_ptr<const void> capturesOwner;
    };

    template <typename Entity>
    struct ComposedQueryStorage {
        std::vector<uint8_t> code;
        std::vector<Value> constants;
        std::vector<FieldBinding<Entity>> fields;
        std::vector<HostCallThunk<Entity>> hostCalls;
        std::vector<CustomOpcodeBinding> customOpcodes;
        std::vector<PlanHint> planHints;
        std::vector<const void*> hostCallCaptures;
        std::vector<const void*> customOpcodeCaptures;
        std::vector<std::shared_ptr<const void>> owners;
    };

    [[nodiscard]] inline uint16_t checkedU16(size_t value, std::string_view message) {
        if (value > std::numeric_limits<uint16_t>::max()) { throw std::runtime_error(std::string{message}); }
        return static_cast<uint16_t>(value);
    }

    [[nodiscard]] inline uint16_t readCodeU16(std::span<const uint8_t> code, size_t offset) {
        if (offset + 2 > code.size()) { throw std::runtime_error("AkkaraDB bytecode query is truncated"); }
        return static_cast<uint16_t>(code[offset]) | static_cast<uint16_t>(static_cast<uint16_t>(code[offset + 1]) << 8);
    }

    inline void appendCodeU16(std::vector<uint8_t>& out, uint16_t value) {
        out.push_back(static_cast<uint8_t>(value & 0xFFU));
        out.push_back(static_cast<uint8_t>((value >> 8) & 0xFFU));
    }

    [[nodiscard]] inline size_t finalReturnOffset(std::span<const uint8_t> code) {
        if (code.empty() || static_cast<Opcode>(code.back()) != Opcode::Return) {
            throw std::runtime_error("AkkaraDB bytecode query composition requires a final return");
        }
        return code.size() - 1;
    }

    [[nodiscard]] inline bool descriptorUsesCapturePointer(std::span<const uint8_t> code) {
        for (size_t pc = 0; pc < code.size();) {
            const auto op = static_cast<Opcode>(code[pc++]);
            switch (op) {
                case Opcode::PushConst:
                case Opcode::LoadField:
                case Opcode::HostCallBool:
                case Opcode::CallCustom:
                    if (op == Opcode::HostCallBool || op == Opcode::CallCustom) { return true; }
                    pc += 2;
                    break;
                case Opcode::Jump:
                case Opcode::JumpIfFalse:
                case Opcode::JumpIfTrue:
                    pc += 2;
                    break;
                default:
                    break;
            }
        }
        return false;
    }

    inline void appendComposableCode(
        std::vector<uint8_t>& out,
        std::span<const uint8_t> code,
        size_t constantOffset,
        size_t fieldOffset,
        size_t hostCallOffset
    ) {
        const size_t returnOffset = finalReturnOffset(code);
        const size_t outStart = out.size();
        size_t pc = 0;
        while (pc < returnOffset) {
            const auto op = static_cast<Opcode>(code[pc++]);
            out.push_back(static_cast<uint8_t>(op));
            switch (op) {
                case Opcode::PushConst:
                    appendCodeU16(out, checkedU16(static_cast<size_t>(readCodeU16(code, pc)) + constantOffset, "AkkaraDB bytecode constant index overflow during composition"));
                    pc += 2;
                    break;
                case Opcode::LoadField:
                    appendCodeU16(out, checkedU16(static_cast<size_t>(readCodeU16(code, pc)) + fieldOffset, "AkkaraDB bytecode field index overflow during composition"));
                    pc += 2;
                    break;
                case Opcode::HostCallBool:
                    appendCodeU16(out, checkedU16(static_cast<size_t>(readCodeU16(code, pc)) + hostCallOffset, "AkkaraDB bytecode host-call index overflow during composition"));
                    pc += 2;
                    break;
                case Opcode::CallCustom:
                    appendCodeU16(out, readCodeU16(code, pc));
                    pc += 2;
                    break;
                case Opcode::Jump:
                case Opcode::JumpIfFalse:
                case Opcode::JumpIfTrue: {
                    const uint16_t target = readCodeU16(code, pc);
                    pc += 2;
                    if (target > returnOffset) { throw std::runtime_error("AkkaraDB bytecode query composition found a jump outside the composable body"); }
                    appendCodeU16(out, checkedU16(outStart + target, "AkkaraDB bytecode jump target overflow during composition"));
                    break;
                }
                case Opcode::Return:
                    throw std::runtime_error("AkkaraDB bytecode query composition found a non-final return");
                default:
                    break;
            }
        }
    }

    [[nodiscard]] inline int planHintScore(PlanHintOp op) noexcept {
        if (op == PlanHintOp::Eq) { return 100; }
        if (op == PlanHintOp::StartsWith) { return 95; }
        return 80;
    }

    template <typename Entity>
    [[nodiscard]] const void* hostCallCaptureFor(const CompiledQueryDescriptor<Entity>& descriptor, size_t index) noexcept {
        if (index < descriptor.hostCallCaptures.size()) { return descriptor.hostCallCaptures[index]; }
        return descriptor.captures;
    }

    template <typename Entity>
    [[nodiscard]] const void* customOpcodeCaptureFor(const CompiledQueryDescriptor<Entity>& descriptor, size_t index) noexcept {
        if (index < descriptor.customOpcodeCaptures.size()) { return descriptor.customOpcodeCaptures[index]; }
        return descriptor.captures;
    }

    inline void appendUniqueCustomOpcode(
        std::vector<CustomOpcodeBinding>& out,
        std::vector<const void*>& captures,
        const CustomOpcodeBinding& binding,
        const void* capture
    ) {
        const auto existing = std::find_if(out.begin(), out.end(), [&](const CustomOpcodeBinding& current) {
            return current.opcode == binding.opcode || current.name == binding.name;
        });
        if (existing == out.end()) {
            out.push_back(binding);
            captures.push_back(capture);
            return;
        }
        const size_t index = static_cast<size_t>(std::distance(out.begin(), existing));
        if (existing->opcode != binding.opcode || existing->name != binding.name || existing->arity != binding.arity
            || existing->resultKind != binding.resultKind || existing->thunk != binding.thunk || captures[index] != capture) {
            throw std::runtime_error("AkkaraDB bytecode query composition found conflicting custom opcode bindings");
        }
    }

    template <typename Entity>
    [[nodiscard]] CompiledQueryDescriptor<Entity> composeAnd(
        const CompiledQueryDescriptor<Entity>& lhs,
        const CompiledQueryDescriptor<Entity>& rhs
    ) {
        auto owner = std::make_shared<ComposedQueryStorage<Entity>>();
        owner->constants.reserve(lhs.constants.size() + rhs.constants.size());
        owner->fields.reserve(lhs.fields.size() + rhs.fields.size());
        owner->hostCalls.reserve(lhs.hostCalls.size() + rhs.hostCalls.size());
        owner->customOpcodes.reserve(lhs.customOpcodes.size() + rhs.customOpcodes.size());
        owner->planHints.reserve(lhs.planHints.size() + rhs.planHints.size());
        owner->hostCallCaptures.reserve(lhs.hostCalls.size() + rhs.hostCalls.size());
        owner->customOpcodeCaptures.reserve(lhs.customOpcodes.size() + rhs.customOpcodes.size());
        owner->code.reserve(lhs.code.size() + rhs.code.size() + 1);

        owner->constants.insert(owner->constants.end(), lhs.constants.begin(), lhs.constants.end());
        owner->fields.insert(owner->fields.end(), lhs.fields.begin(), lhs.fields.end());
        for (size_t i = 0; i < lhs.hostCalls.size(); ++i) {
            owner->hostCalls.push_back(lhs.hostCalls[i]);
            owner->hostCallCaptures.push_back(hostCallCaptureFor(lhs, i));
        }
        for (size_t i = 0; i < lhs.customOpcodes.size(); ++i) {
            appendUniqueCustomOpcode(owner->customOpcodes, owner->customOpcodeCaptures, lhs.customOpcodes[i], customOpcodeCaptureFor(lhs, i));
        }

        const size_t constantOffset = owner->constants.size();
        const size_t fieldOffset = owner->fields.size();
        const size_t hostCallOffset = owner->hostCalls.size();

        owner->constants.insert(owner->constants.end(), rhs.constants.begin(), rhs.constants.end());
        owner->fields.insert(owner->fields.end(), rhs.fields.begin(), rhs.fields.end());
        for (size_t i = 0; i < rhs.hostCalls.size(); ++i) {
            owner->hostCalls.push_back(rhs.hostCalls[i]);
            owner->hostCallCaptures.push_back(hostCallCaptureFor(rhs, i));
        }
        for (size_t i = 0; i < rhs.customOpcodes.size(); ++i) {
            appendUniqueCustomOpcode(owner->customOpcodes, owner->customOpcodeCaptures, rhs.customOpcodes[i], customOpcodeCaptureFor(rhs, i));
        }

        owner->planHints.insert(owner->planHints.end(), lhs.planHints.begin(), lhs.planHints.end());
        for (const auto& hint : rhs.planHints) {
            owner->planHints.push_back(PlanHint{
                hint.op,
                checkedU16(static_cast<size_t>(hint.field) + fieldOffset, "AkkaraDB bytecode plan hint field index overflow during composition"),
                checkedU16(static_cast<size_t>(hint.constant) + constantOffset, "AkkaraDB bytecode plan hint constant index overflow during composition")
            });
        }
        std::stable_sort(owner->planHints.begin(), owner->planHints.end(), [](const PlanHint& a, const PlanHint& b) {
            return planHintScore(a.op) > planHintScore(b.op);
        });

        appendComposableCode(owner->code, lhs.code, 0, 0, 0);
        appendComposableCode(owner->code, rhs.code, constantOffset, fieldOffset, hostCallOffset);
        owner->code.push_back(static_cast<uint8_t>(Opcode::And));
        owner->code.push_back(static_cast<uint8_t>(Opcode::Return));

        if (lhs.capturesOwner) { owner->owners.push_back(lhs.capturesOwner); }
        if (rhs.capturesOwner) { owner->owners.push_back(rhs.capturesOwner); }

        return CompiledQueryDescriptor<Entity>{
            .code = owner->code,
            .constants = owner->constants,
            .fields = owner->fields,
            .hostCalls = owner->hostCalls,
            .customOpcodes = owner->customOpcodes,
            .planHints = owner->planHints,
            .hostCallCaptures = owner->hostCallCaptures,
            .customOpcodeCaptures = owner->customOpcodeCaptures,
            .captures = nullptr,
            .capturesOwner = owner
        };
    }

    template <typename Entity>
    class PreparedQuery {
    public:
        explicit PreparedQuery(CompiledQueryDescriptor<Entity> descriptor)
            : descriptor_{descriptor} {
            validate();
        }

        [[nodiscard]] const CompiledQueryDescriptor<Entity>& descriptor() const noexcept { return descriptor_; }
        [[nodiscard]] bool usesHostCalls() const noexcept { return usesHostCalls_; }

        [[nodiscard]] bool canEvalRaw() const noexcept {
            if (usesHostCalls_) { return false; }
            for (const auto& field : descriptor_.fields) {
                if (field.rawRead == nullptr) { return false; }
            }
            return true;
        }

    private:
        static uint16_t readU16At(std::span<const uint8_t> code, size_t offset) {
            if (offset + 2 > code.size()) { throw std::runtime_error("AkkaraDB bytecode query is truncated"); }
            return static_cast<uint16_t>(code[offset]) | static_cast<uint16_t>(static_cast<uint16_t>(code[offset + 1]) << 8);
        }

        [[nodiscard]] const CustomOpcodeBinding* findCustomOpcode(uint16_t opcode) const noexcept {
            for (const auto& binding : descriptor_.customOpcodes) {
                if (binding.opcode == opcode) { return &binding; }
            }
            return nullptr;
        }

        void validateCustomOpcodeTable() const {
            for (size_t i = 0; i < descriptor_.customOpcodes.size(); ++i) {
                const auto& binding = descriptor_.customOpcodes[i];
                if (binding.opcode < customOpcodeUserMin) { throw std::runtime_error("AkkaraDB bytecode custom opcode id is outside the user range"); }
                if (binding.name.empty()) { throw std::runtime_error("AkkaraDB bytecode custom opcode name is empty"); }
                if (binding.arity > customOpcodeMaxArity) { throw std::runtime_error("AkkaraDB bytecode custom opcode arity is too large"); }
                if (binding.thunk == nullptr) { throw std::runtime_error("AkkaraDB bytecode custom opcode thunk is null"); }
                for (size_t j = i + 1; j < descriptor_.customOpcodes.size(); ++j) {
                    if (descriptor_.customOpcodes[j].opcode == binding.opcode) {
                        throw std::runtime_error("AkkaraDB bytecode custom opcode id is duplicated");
                    }
                    if (descriptor_.customOpcodes[j].name == binding.name) {
                        throw std::runtime_error("AkkaraDB bytecode custom opcode name is duplicated");
                    }
                }
            }
        }

        void validate() {
            if (descriptor_.code.empty()) { throw std::runtime_error("AkkaraDB bytecode query is empty"); }
            if (!descriptor_.hostCallCaptures.empty() && descriptor_.hostCallCaptures.size() != descriptor_.hostCalls.size()) {
                throw std::runtime_error("AkkaraDB bytecode host-call capture table size mismatch");
            }
            if (!descriptor_.customOpcodeCaptures.empty() && descriptor_.customOpcodeCaptures.size() != descriptor_.customOpcodes.size()) {
                throw std::runtime_error("AkkaraDB bytecode custom opcode capture table size mismatch");
            }
            validateCustomOpcodeTable();

            size_t pc = 0;
            bool hasReturn = false;
            int stackDepth = 0;
            usesHostCalls_ = false;
            while (pc < descriptor_.code.size()) {
                const auto op = static_cast<Opcode>(descriptor_.code[pc++]);
                switch (op) {
                    case Opcode::PushConst: {
                        const uint16_t index = readU16At(descriptor_.code, pc);
                        pc += 2;
                        if (index >= descriptor_.constants.size()) { throw std::runtime_error("AkkaraDB bytecode constant index out of range"); }
                        ++stackDepth;
                        break;
                    }
                    case Opcode::LoadField: {
                        const uint16_t index = readU16At(descriptor_.code, pc);
                        pc += 2;
                        if (index >= descriptor_.fields.size()) { throw std::runtime_error("AkkaraDB bytecode field index out of range"); }
                        if (descriptor_.fields[index].read == nullptr) { throw std::runtime_error("AkkaraDB bytecode field reader is null"); }
                        ++stackDepth;
                        break;
                    }
                    case Opcode::HostCallBool: {
                        const uint16_t index = readU16At(descriptor_.code, pc);
                        pc += 2;
                        if (index >= descriptor_.hostCalls.size()) { throw std::runtime_error("AkkaraDB bytecode host-call index out of range"); }
                        usesHostCalls_ = true;
                        ++stackDepth;
                        break;
                    }
                    case Opcode::CallCustom: {
                        const uint16_t opcode = readU16At(descriptor_.code, pc);
                        pc += 2;
                        const CustomOpcodeBinding* binding = findCustomOpcode(opcode);
                        if (binding == nullptr) { throw std::runtime_error("AkkaraDB bytecode custom opcode is not registered"); }
                        if (stackDepth < binding->arity) { throw std::runtime_error("AkkaraDB bytecode stack underflow"); }
                        stackDepth = stackDepth - binding->arity + 1;
                        break;
                    }
                    case Opcode::Eq:
                    case Opcode::Ne:
                    case Opcode::Lt:
                    case Opcode::Le:
                    case Opcode::Gt:
                    case Opcode::Ge:
                    case Opcode::And:
                    case Opcode::Or:
                    case Opcode::StartsWith:
                    case Opcode::EndsWith:
                    case Opcode::Contains:
                    case Opcode::Like:
                    case Opcode::Add:
                    case Opcode::Sub:
                    case Opcode::Mul:
                    case Opcode::Div:
                    case Opcode::Mod:
                        if (stackDepth < 2) { throw std::runtime_error("AkkaraDB bytecode stack underflow"); }
                        --stackDepth;
                        break;
                    case Opcode::Not:
                        if (stackDepth < 1) { throw std::runtime_error("AkkaraDB bytecode stack underflow"); }
                        break;
                    case Opcode::Pop:
                        if (stackDepth < 1) { throw std::runtime_error("AkkaraDB bytecode stack underflow"); }
                        --stackDepth;
                        break;
                    case Opcode::Jump:
                    case Opcode::JumpIfFalse:
                    case Opcode::JumpIfTrue: {
                        const uint16_t target = readU16At(descriptor_.code, pc);
                        pc += 2;
                        if (target >= descriptor_.code.size()) { throw std::runtime_error("AkkaraDB bytecode jump target out of range"); }
                        if ((op == Opcode::JumpIfFalse || op == Opcode::JumpIfTrue) && stackDepth < 1) {
                            throw std::runtime_error("AkkaraDB bytecode stack underflow");
                        }
                        break;
                    }
                    case Opcode::Return:
                        if (stackDepth < 1) { throw std::runtime_error("AkkaraDB bytecode return without a value"); }
                        hasReturn = true;
                        break;
                    default:
                        throw std::runtime_error("AkkaraDB bytecode query contains an unknown opcode");
                }
            }
            if (!hasReturn) { throw std::runtime_error("AkkaraDB bytecode query has no return instruction"); }
        }

        CompiledQueryDescriptor<Entity> descriptor_;
        bool usesHostCalls_ = false;
    };

    [[nodiscard]] inline bool valueIsTrue(const Value& value) noexcept {
        return value.kind == ValueKind::Bool && value.b;
    }

    [[nodiscard]] inline long double numericAsLongDouble(const Value& value) {
        switch (value.kind) {
            case ValueKind::Int: return static_cast<long double>(value.i);
            case ValueKind::UInt: return static_cast<long double>(value.u);
            case ValueKind::Double: return static_cast<long double>(value.d);
            default: throw std::runtime_error("AkkaraDB bytecode comparison requires numeric values");
        }
    }

    [[nodiscard]] inline int compareValues(const Value& lhs, const Value& rhs) {
        if (lhs.isNumeric() && rhs.isNumeric()) {
            const long double a = numericAsLongDouble(lhs);
            const long double b = numericAsLongDouble(rhs);
            return (a > b) - (a < b);
        }
        if (lhs.kind == ValueKind::String && rhs.kind == ValueKind::String) { return (lhs.s > rhs.s) - (lhs.s < rhs.s); }
        if (lhs.kind == ValueKind::Bool && rhs.kind == ValueKind::Bool) { return (lhs.b > rhs.b) - (lhs.b < rhs.b); }
        throw std::runtime_error("AkkaraDB bytecode comparison between incompatible values");
    }

    [[nodiscard]] inline bool valuesEqual(const Value& lhs, const Value& rhs) {
        if (lhs.kind == ValueKind::Null || rhs.kind == ValueKind::Null) { return lhs.kind == rhs.kind; }
        if (lhs.isNumeric() && rhs.isNumeric()) { return compareValues(lhs, rhs) == 0; }
        if (lhs.kind != rhs.kind) { return false; }
        switch (lhs.kind) {
            case ValueKind::Bool: return lhs.b == rhs.b;
            case ValueKind::Int: return lhs.i == rhs.i;
            case ValueKind::UInt: return lhs.u == rhs.u;
            case ValueKind::Double: return lhs.d == rhs.d;
            case ValueKind::String: return lhs.s == rhs.s;
            case ValueKind::Null: return true;
        }
        return false;
    }

    [[nodiscard]] inline int64_t integralAsInt64(const Value& value) {
        if (value.kind == ValueKind::Int) { return value.i; }
        if (value.kind == ValueKind::UInt && value.u <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return static_cast<int64_t>(value.u);
        }
        throw std::runtime_error("AkkaraDB bytecode integer arithmetic value is out of range");
    }

    [[nodiscard]] inline Value evalArithmeticOperation(Opcode op, const Value& lhs, const Value& rhs) {
        if (!lhs.isNumeric() || !rhs.isNumeric()) { throw std::runtime_error("AkkaraDB bytecode arithmetic requires numeric values"); }

        if (op == Opcode::Mod) {
            if (lhs.kind == ValueKind::Double || rhs.kind == ValueKind::Double) {
                throw std::runtime_error("AkkaraDB bytecode modulo requires integer values");
            }
            if (lhs.kind == ValueKind::UInt && rhs.kind == ValueKind::UInt) {
                if (rhs.u == 0) { throw std::runtime_error("AkkaraDB bytecode modulo by zero"); }
                return Value::uinteger(lhs.u % rhs.u);
            }
            const int64_t b = integralAsInt64(rhs);
            if (b == 0) { throw std::runtime_error("AkkaraDB bytecode modulo by zero"); }
            return Value::integer(integralAsInt64(lhs) % b);
        }

        if (lhs.kind == ValueKind::Double || rhs.kind == ValueKind::Double || lhs.kind != rhs.kind) {
            const long double a = numericAsLongDouble(lhs);
            const long double b = numericAsLongDouble(rhs);
            if (op == Opcode::Add) { return Value::floating(static_cast<double>(a + b)); }
            if (op == Opcode::Sub) { return Value::floating(static_cast<double>(a - b)); }
            if (op == Opcode::Mul) { return Value::floating(static_cast<double>(a * b)); }
            if (op == Opcode::Div) {
                if (b == 0) { throw std::runtime_error("AkkaraDB bytecode division by zero"); }
                return Value::floating(static_cast<double>(a / b));
            }
        }

        if (lhs.kind == ValueKind::UInt) {
            if (op == Opcode::Add) { return Value::uinteger(lhs.u + rhs.u); }
            if (op == Opcode::Sub) {
                if (lhs.u >= rhs.u) { return Value::uinteger(lhs.u - rhs.u); }
                const uint64_t diff = rhs.u - lhs.u;
                if (diff > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                    throw std::runtime_error("AkkaraDB bytecode unsigned subtraction underflow is out of range");
                }
                return Value::integer(-static_cast<int64_t>(diff));
            }
            if (op == Opcode::Mul) { return Value::uinteger(lhs.u * rhs.u); }
            if (op == Opcode::Div) {
                if (rhs.u == 0) { throw std::runtime_error("AkkaraDB bytecode division by zero"); }
                return Value::uinteger(lhs.u / rhs.u);
            }
        }

        if (op == Opcode::Add) { return Value::integer(lhs.i + rhs.i); }
        if (op == Opcode::Sub) { return Value::integer(lhs.i - rhs.i); }
        if (op == Opcode::Mul) { return Value::integer(lhs.i * rhs.i); }
        if (op == Opcode::Div) {
            if (rhs.i == 0) { throw std::runtime_error("AkkaraDB bytecode division by zero"); }
            return Value::integer(lhs.i / rhs.i);
        }
        throw std::runtime_error("AkkaraDB bytecode unknown arithmetic operation");
    }

    [[nodiscard]] inline bool stringStartsWith(std::string_view value, std::string_view prefix) noexcept {
        return value.size() >= prefix.size() && value.substr(0, prefix.size()) == prefix;
    }

    [[nodiscard]] inline bool stringEndsWith(std::string_view value, std::string_view suffix) noexcept {
        return value.size() >= suffix.size() && value.substr(value.size() - suffix.size()) == suffix;
    }

    [[nodiscard]] inline bool stringContains(std::string_view value, std::string_view needle) noexcept {
        return value.find(needle) != std::string_view::npos;
    }

    [[nodiscard]] inline bool stringLike(std::string_view value, std::string_view pattern) noexcept {
        size_t valueIndex = 0;
        size_t patternIndex = 0;
        size_t starPattern = std::string_view::npos;
        size_t starValue = 0;

        while (valueIndex < value.size()) {
            if (patternIndex < pattern.size() && (pattern[patternIndex] == '?' || pattern[patternIndex] == value[valueIndex])) {
                ++valueIndex;
                ++patternIndex;
            }
            else if (patternIndex < pattern.size() && pattern[patternIndex] == '*') {
                starPattern = patternIndex++;
                starValue = valueIndex;
            }
            else if (starPattern != std::string_view::npos) {
                patternIndex = starPattern + 1;
                valueIndex = ++starValue;
            }
            else { return false; }
        }

        while (patternIndex < pattern.size() && pattern[patternIndex] == '*') { ++patternIndex; }
        return patternIndex == pattern.size();
    }

    [[nodiscard]] inline bool evalStringOperation(Opcode op, const Value& lhs, const Value& rhs) {
        if (lhs.kind != ValueKind::String || rhs.kind != ValueKind::String) {
            throw std::runtime_error("AkkaraDB bytecode string operation requires string values");
        }
        if (op == Opcode::StartsWith) { return stringStartsWith(lhs.s, rhs.s); }
        if (op == Opcode::EndsWith) { return stringEndsWith(lhs.s, rhs.s); }
        if (op == Opcode::Contains) { return stringContains(lhs.s, rhs.s); }
        if (op == Opcode::Like) { return stringLike(lhs.s, rhs.s); }
        throw std::runtime_error("AkkaraDB bytecode unknown string operation");
    }

    [[nodiscard]] inline const CustomOpcodeBinding* findCustomOpcode(std::span<const CustomOpcodeBinding> bindings, uint16_t opcode) noexcept {
        for (const auto& binding : bindings) {
            if (binding.opcode == opcode) { return &binding; }
        }
        return nullptr;
    }

    inline void invokeCustomOpcode(std::vector<Value>& stack, const CustomOpcodeBinding& binding, const void* captures) {
        if (stack.size() < binding.arity) { throw std::runtime_error("AkkaraDB bytecode stack underflow"); }
        const size_t firstArg = stack.size() - binding.arity;
        const std::span<const Value> args{stack.data() + firstArg, binding.arity};
        Value out;
        if (!binding.thunk(args, out, captures)) { throw std::runtime_error("AkkaraDB bytecode custom opcode thunk rejected its arguments"); }
        if (out.kind != binding.resultKind) { throw std::runtime_error("AkkaraDB bytecode custom opcode returned an unexpected value kind"); }
        stack.resize(firstArg);
        stack.push_back(out);
    }

    template <typename Entity>
    [[nodiscard]] bool eval(const PreparedQuery<Entity>& query, const Entity& entity) {
        const auto& descriptor = query.descriptor();
        std::vector<Value> stack;
        stack.reserve(16);

        auto pop = [&]() {
            if (stack.empty()) { throw std::runtime_error("AkkaraDB bytecode stack underflow"); }
            Value out = stack.back();
            stack.pop_back();
            return out;
        };

        auto readU16 = [&](size_t& pc) {
            if (pc + 2 > descriptor.code.size()) { throw std::runtime_error("AkkaraDB bytecode query is truncated"); }
            const uint16_t out = static_cast<uint16_t>(descriptor.code[pc]) | static_cast<uint16_t>(
                static_cast<uint16_t>(descriptor.code[pc + 1]) << 8
            );
            pc += 2;
            return out;
        };

        size_t pc = 0;
        while (pc < descriptor.code.size()) {
            const auto op = static_cast<Opcode>(descriptor.code[pc++]);
            switch (op) {
                case Opcode::PushConst:
                    stack.push_back(descriptor.constants[readU16(pc)]);
                    break;
                case Opcode::LoadField:
                    stack.push_back(descriptor.fields[readU16(pc)].read(entity));
                    break;
                case Opcode::HostCallBool:
                {
                    const uint16_t index = readU16(pc);
                    stack.push_back(Value::boolean(descriptor.hostCalls[index](entity, hostCallCaptureFor(descriptor, index))));
                    break;
                }
                case Opcode::CallCustom: {
                    const uint16_t opcode = readU16(pc);
                    const CustomOpcodeBinding* binding = findCustomOpcode(descriptor.customOpcodes, opcode);
                    if (binding == nullptr) { throw std::runtime_error("AkkaraDB bytecode custom opcode is not registered"); }
                    const size_t index = static_cast<size_t>(binding - descriptor.customOpcodes.data());
                    invokeCustomOpcode(stack, *binding, customOpcodeCaptureFor(descriptor, index));
                    break;
                }
                case Opcode::Eq: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    stack.push_back(Value::boolean(valuesEqual(lhs, rhs)));
                    break;
                }
                case Opcode::Ne: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    stack.push_back(Value::boolean(!valuesEqual(lhs, rhs)));
                    break;
                }
                case Opcode::Lt:
                case Opcode::Le:
                case Opcode::Gt:
                case Opcode::Ge: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    const int cmp = compareValues(lhs, rhs);
                    bool result = false;
                    if (op == Opcode::Lt) { result = cmp < 0; }
                    else if (op == Opcode::Le) { result = cmp <= 0; }
                    else if (op == Opcode::Gt) { result = cmp > 0; }
                    else { result = cmp >= 0; }
                    stack.push_back(Value::boolean(result));
                    break;
                }
                case Opcode::Add:
                case Opcode::Sub:
                case Opcode::Mul:
                case Opcode::Div:
                case Opcode::Mod: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    stack.push_back(evalArithmeticOperation(op, lhs, rhs));
                    break;
                }
                case Opcode::And: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    stack.push_back(Value::boolean(valueIsTrue(lhs) && valueIsTrue(rhs)));
                    break;
                }
                case Opcode::Or: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    stack.push_back(Value::boolean(valueIsTrue(lhs) || valueIsTrue(rhs)));
                    break;
                }
                case Opcode::StartsWith:
                case Opcode::EndsWith:
                case Opcode::Contains:
                case Opcode::Like: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    stack.push_back(Value::boolean(evalStringOperation(op, lhs, rhs)));
                    break;
                }
                case Opcode::Not:
                    stack.push_back(Value::boolean(!valueIsTrue(pop())));
                    break;
                case Opcode::Pop:
                    (void)pop();
                    break;
                case Opcode::Jump:
                    pc = readU16(pc);
                    break;
                case Opcode::JumpIfFalse: {
                    const uint16_t target = readU16(pc);
                    if (!valueIsTrue(pop())) { pc = target; }
                    break;
                }
                case Opcode::JumpIfTrue: {
                    const uint16_t target = readU16(pc);
                    if (valueIsTrue(pop())) { pc = target; }
                    break;
                }
                case Opcode::Return:
                    return valueIsTrue(pop());
                default:
                    throw std::runtime_error("AkkaraDB bytecode query contains an unknown opcode");
            }
        }

        throw std::runtime_error("AkkaraDB bytecode query reached end of program");
    }

    template <typename Entity>
    [[nodiscard]] bool evalRaw(const PreparedQuery<Entity>& query, std::span<const uint8_t> rowBytes) {
        const auto& descriptor = query.descriptor();
        std::vector<Value> stack;
        stack.reserve(16);

        auto pop = [&]() {
            if (stack.empty()) { throw std::runtime_error("AkkaraDB bytecode stack underflow"); }
            Value out = stack.back();
            stack.pop_back();
            return out;
        };

        auto readU16 = [&](size_t& pc) {
            if (pc + 2 > descriptor.code.size()) { throw std::runtime_error("AkkaraDB bytecode query is truncated"); }
            const uint16_t out = static_cast<uint16_t>(descriptor.code[pc]) | static_cast<uint16_t>(
                static_cast<uint16_t>(descriptor.code[pc + 1]) << 8
            );
            pc += 2;
            return out;
        };

        size_t pc = 0;
        while (pc < descriptor.code.size()) {
            const auto op = static_cast<Opcode>(descriptor.code[pc++]);
            switch (op) {
                case Opcode::PushConst:
                    stack.push_back(descriptor.constants[readU16(pc)]);
                    break;
                case Opcode::LoadField: {
                    const auto& field = descriptor.fields[readU16(pc)];
                    if (field.rawRead == nullptr) { throw std::runtime_error("AkkaraDB bytecode field has no raw reader"); }
                    stack.push_back(field.rawRead(rowBytes));
                    break;
                }
                case Opcode::HostCallBool:
                    throw std::runtime_error("AkkaraDB bytecode raw evaluation cannot execute host calls");
                case Opcode::CallCustom: {
                    const uint16_t opcode = readU16(pc);
                    const CustomOpcodeBinding* binding = findCustomOpcode(descriptor.customOpcodes, opcode);
                    if (binding == nullptr) { throw std::runtime_error("AkkaraDB bytecode custom opcode is not registered"); }
                    const size_t index = static_cast<size_t>(binding - descriptor.customOpcodes.data());
                    invokeCustomOpcode(stack, *binding, customOpcodeCaptureFor(descriptor, index));
                    break;
                }
                case Opcode::Eq: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    stack.push_back(Value::boolean(valuesEqual(lhs, rhs)));
                    break;
                }
                case Opcode::Ne: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    stack.push_back(Value::boolean(!valuesEqual(lhs, rhs)));
                    break;
                }
                case Opcode::Lt:
                case Opcode::Le:
                case Opcode::Gt:
                case Opcode::Ge: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    const int cmp = compareValues(lhs, rhs);
                    bool result = false;
                    if (op == Opcode::Lt) { result = cmp < 0; }
                    else if (op == Opcode::Le) { result = cmp <= 0; }
                    else if (op == Opcode::Gt) { result = cmp > 0; }
                    else { result = cmp >= 0; }
                    stack.push_back(Value::boolean(result));
                    break;
                }
                case Opcode::Add:
                case Opcode::Sub:
                case Opcode::Mul:
                case Opcode::Div:
                case Opcode::Mod: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    stack.push_back(evalArithmeticOperation(op, lhs, rhs));
                    break;
                }
                case Opcode::And: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    stack.push_back(Value::boolean(valueIsTrue(lhs) && valueIsTrue(rhs)));
                    break;
                }
                case Opcode::Or: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    stack.push_back(Value::boolean(valueIsTrue(lhs) || valueIsTrue(rhs)));
                    break;
                }
                case Opcode::StartsWith:
                case Opcode::EndsWith:
                case Opcode::Contains:
                case Opcode::Like: {
                    const Value rhs = pop();
                    const Value lhs = pop();
                    stack.push_back(Value::boolean(evalStringOperation(op, lhs, rhs)));
                    break;
                }
                case Opcode::Not:
                    stack.push_back(Value::boolean(!valueIsTrue(pop())));
                    break;
                case Opcode::Pop:
                    (void)pop();
                    break;
                case Opcode::Jump:
                    pc = readU16(pc);
                    break;
                case Opcode::JumpIfFalse: {
                    const uint16_t target = readU16(pc);
                    if (!valueIsTrue(pop())) { pc = target; }
                    break;
                }
                case Opcode::JumpIfTrue: {
                    const uint16_t target = readU16(pc);
                    if (valueIsTrue(pop())) { pc = target; }
                    break;
                }
                case Opcode::Return:
                    return valueIsTrue(pop());
                default:
                    throw std::runtime_error("AkkaraDB bytecode query contains an unknown opcode");
            }
        }

        throw std::runtime_error("AkkaraDB bytecode query reached end of program");
    }
} // namespace akkaradb::query::bytecode

#define AKKARADB_DETAIL_QUERY_OPCODE_CONCAT_INNER(A, B) A##B
#define AKKARADB_DETAIL_QUERY_OPCODE_CONCAT(A, B) AKKARADB_DETAIL_QUERY_OPCODE_CONCAT_INNER(A, B)

#define AKKARADB_QUERY_OPCODE(Function, OpcodeValue, NameValue, ArityValue, ResultKindValue, ThunkValue) \
    [[maybe_unused]] inline constexpr ::akkaradb::query::bytecode::StaticCustomOpcodeRegistration< \
        &Function, \
        static_cast<::std::uint16_t>(OpcodeValue), \
        static_cast<::std::uint8_t>(ArityValue), \
        ResultKindValue, \
        ThunkValue> \
        AKKARADB_DETAIL_QUERY_OPCODE_CONCAT(__akkaradb_query_opcode_, __LINE__){NameValue}
