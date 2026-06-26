/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// jni/AkkaraJni.cpp
#include "akkaradb/AkkaraDB.hpp"

#include <jni.h>

#include <cstring>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#ifndef AKKARADB_JNI_COMPAT_LINE
#define AKKARADB_JNI_COMPAT_LINE "126.1"
#endif

#ifndef AKKARADB_REQUIRED_NATIVE_GENERATION
#define AKKARADB_REQUIRED_NATIVE_GENERATION "126"
#endif

namespace {
    using akkaradb::AkkaraDB;
    using akkaradb::Codec;
    using akkaradb::StartupMode;
    using akkaradb::core::ArenaGenerator;
    using akkaradb::core::BufferArena;
    using akkaradb::engine::AkkEngine;

    [[nodiscard]] AkkaraDB* dbFrom(jlong handle) noexcept { return reinterpret_cast<AkkaraDB*>(static_cast<std::uintptr_t>(handle)); }

    [[nodiscard]] jlong toHandle(void* ptr) noexcept { return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(ptr)); }

    void throwJava(JNIEnv* env, const char* className, const char* message) {
        jclass cls = env->FindClass(className);
        if (cls != nullptr) { env->ThrowNew(cls, message); }
    }

    void throwRuntime(JNIEnv* env, const std::exception& ex) { throwJava(env, "java/lang/RuntimeException", ex.what()); }

    [[nodiscard]] std::span<const uint8_t> readDirectBuffer(JNIEnv* env, jobject buffer) {
        if (buffer == nullptr) { return {}; }
        void* raw = env->GetDirectBufferAddress(buffer);
        const jlong capacity = env->GetDirectBufferCapacity(buffer);
        if (raw == nullptr || capacity < 0) { throw std::runtime_error("AkkaraDB JNI requires a direct ByteBuffer"); }
        return {reinterpret_cast<const uint8_t*>(raw), static_cast<size_t>(capacity)};
    }

    [[nodiscard]] jobject makeDirectBuffer(JNIEnv* env, std::span<const uint8_t> bytes) {
        if (bytes.size() > static_cast<size_t>(std::numeric_limits<jint>::max())) {
            throw std::runtime_error("AkkaraDB JNI buffer is too large for a JVM ByteBuffer");
        }
        jclass byteBufferCls = env->FindClass("java/nio/ByteBuffer");
        if (byteBufferCls == nullptr) { return nullptr; }
        jmethodID allocateDirect = env->GetStaticMethodID(byteBufferCls, "allocateDirect", "(I)Ljava/nio/ByteBuffer;");
        if (allocateDirect == nullptr) { return nullptr; }

        jobject out = env->CallStaticObjectMethod(byteBufferCls, allocateDirect, static_cast<jint>(bytes.size()));
        if (out == nullptr || env->ExceptionCheck()) { return nullptr; }

        if (!bytes.empty()) {
            void* raw = env->GetDirectBufferAddress(out);
            if (raw == nullptr) { throw std::runtime_error("AkkaraDB JNI failed to allocate a direct ByteBuffer"); }
            std::memcpy(raw, bytes.data(), bytes.size());
        }
        return out;
    }

    [[nodiscard]] jobject makeDirectView(JNIEnv* env, std::span<const uint8_t> bytes) {
        if (bytes.empty()) { return makeDirectBuffer(env, bytes); }
        if (bytes.size() > static_cast<size_t>(std::numeric_limits<jint>::max())) {
            throw std::runtime_error("AkkaraDB JNI buffer is too large for a JVM ByteBuffer");
        }
        jobject direct = env->NewDirectByteBuffer(const_cast<uint8_t*>(bytes.data()), static_cast<jlong>(bytes.size()));
        if (direct == nullptr) { return nullptr; }

        jclass byteBufferCls = env->FindClass("java/nio/ByteBuffer");
        if (byteBufferCls == nullptr) { return nullptr; }
        jmethodID asReadOnly = env->GetMethodID(byteBufferCls, "asReadOnlyBuffer", "()Ljava/nio/ByteBuffer;");
        if (asReadOnly == nullptr) { return nullptr; }
        return env->CallObjectMethod(direct, asReadOnly);
    }

    [[nodiscard]] std::string readString(JNIEnv* env, jstring value) {
        if (value == nullptr) { return {}; }
        const char* raw = env->GetStringUTFChars(value, nullptr);
        if (raw == nullptr) { return {}; }
        std::string out{raw};
        env->ReleaseStringUTFChars(value, raw);
        return out;
    }

    [[nodiscard]] StartupMode startupModeFromOrdinal(jint value) {
        switch (value) {
            case 0: return StartupMode::ULTRA_FAST;
            case 1: return StartupMode::FAST;
            case 2: return StartupMode::NORMAL;
            case 3: return StartupMode::DURABLE;
            default: throw std::runtime_error("Invalid AkkaraDB StartupMode ordinal");
        }
    }

    [[nodiscard]] Codec codecFromOrdinal(jint value) {
        switch (value) {
            case 0: return Codec::NONE;
            case 1: return Codec::ZSTD;
            default: throw std::runtime_error("Invalid AkkaraDB Codec ordinal");
        }
    }

    template <typename T>
    void assignOptionalNonNegative(std::optional<T>& target, jlong value) {
        if (value < 0) { return; }
        target = static_cast<T>(value);
    }

    void assignOptionalBool(std::optional<bool>& target, jint value, const char* name) {
        switch (value) {
            case -1: return;
            case 0: target = false;
                return;
            case 1: target = true;
                return;
            default: throw std::runtime_error(std::string("Invalid boolean override for ") + name);
        }
    }

    class BytesReader {
        public:
            explicit BytesReader(std::span<const uint8_t> bytes) : bytes_(bytes) {}

            [[nodiscard]] bool eof() const noexcept { return pos_ == bytes_.size(); }

            [[nodiscard]] uint8_t u8() {
                ensure(1);
                return bytes_[pos_++];
            }

            [[nodiscard]] uint16_t u16() {
                ensure(2);
                const uint16_t out = static_cast<uint16_t>(bytes_[pos_]) | static_cast<uint16_t>(static_cast<uint16_t>(bytes_[pos_ + 1]) <<
                    8);
                pos_ += 2;
                return out;
            }

            [[nodiscard]] uint32_t u32() {
                ensure(4);
                const uint32_t out = static_cast<uint32_t>(bytes_[pos_]) | (static_cast<uint32_t>(bytes_[pos_ + 1]) << 8) | (static_cast<
                    uint32_t>(bytes_[pos_ + 2]) << 16) | (static_cast<uint32_t>(bytes_[pos_ + 3]) << 24);
                pos_ += 4;
                return out;
            }

            [[nodiscard]] uint64_t u64() {
                const uint64_t lo = u32();
                const uint64_t hi = u32();
                return lo | (hi << 32);
            }

            [[nodiscard]] std::string strU16() {
                const auto len = static_cast<size_t>(u16());
                ensure(len);
                std::string out(reinterpret_cast<const char*>(bytes_.data() + pos_), len);
                pos_ += len;
                return out;
            }

            [[nodiscard]] std::string strI32() {
                const auto len = static_cast<int32_t>(u32());
                if (len < 0) { throw std::runtime_error("Negative string length in AkkaraDB query payload"); }
                ensure(static_cast<size_t>(len));
                std::string out(reinterpret_cast<const char*>(bytes_.data() + pos_), static_cast<size_t>(len));
                pos_ += static_cast<size_t>(len);
                return out;
            }

            void skip(size_t len) {
                ensure(len);
                pos_ += len;
            }

        private:
            void ensure(size_t len) const {
                if (len > bytes_.size() - pos_) { throw std::runtime_error("Truncated AkkaraDB query payload"); }
            }

            std::span<const uint8_t> bytes_;
            size_t pos_ = 0;
    };

    struct TypeDesc;
    using TypePtr = std::shared_ptr<TypeDesc>;

    struct FieldDesc {
        std::string name;
        TypePtr type;
    };

    struct TypeDesc {
        enum Kind : uint8_t {
            BOOL = 0x01,
            INT8 = 0x02,
            INT16 = 0x03,
            INT32 = 0x04,
            INT64 = 0x05,
            FLOAT = 0x06,
            DOUBLE = 0x07,
            STRING = 0x08,
            LIST = 0x0A,
            MAP = 0x0B,
            STRUCT = 0x0C,
            NULLABLE = 0x0D, };

        Kind kind;
        std::vector<FieldDesc> fields;
        TypePtr elem;
        TypePtr key;
        TypePtr value;
    };

    [[nodiscard]] TypePtr parseType(BytesReader& in);

    void parseStructFields(BytesReader& in, TypeDesc& out) {
        const auto count = in.u8();
        out.fields.reserve(count);
        for (uint8_t i = 0; i < count; ++i) { out.fields.push_back(FieldDesc{in.strU16(), parseType(in)}); }
    }

    [[nodiscard]] TypePtr parseType(BytesReader& in) {
        auto out = std::make_shared<TypeDesc>();
        out->kind = static_cast<TypeDesc::Kind>(in.u8());
        switch (out->kind) {
            case TypeDesc::BOOL:
            case TypeDesc::INT8:
            case TypeDesc::INT16:
            case TypeDesc::INT32:
            case TypeDesc::INT64:
            case TypeDesc::FLOAT:
            case TypeDesc::DOUBLE:
            case TypeDesc::STRING: break;
            case TypeDesc::LIST: out->elem = parseType(in);
                break;
            case TypeDesc::MAP: out->key = parseType(in);
                out->value = parseType(in);
                break;
            case TypeDesc::STRUCT: parseStructFields(in, *out);
                break;
            case TypeDesc::NULLABLE: out->elem = parseType(in);
                break;
            default: throw std::runtime_error("Unknown AkkaraDB schema kind");
        }
        return out;
    }

    [[nodiscard]] TypeDesc parseRootSchema(std::span<const uint8_t> schemaBytes) {
        BytesReader in(schemaBytes);
        TypeDesc root{TypeDesc::STRUCT};
        parseStructFields(in, root);
        if (!in.eof()) { throw std::runtime_error("Trailing bytes in AkkaraDB schema payload"); }
        return root;
    }

    struct Value {
        enum Kind {
            MISSING, NULL_VALUE, BOOL, INT, DOUBLE, STRING, LIST, MAP
        } kind = MISSING;

        bool b = false;
        int64_t i = 0;
        double d = 0.0;
        std::string s;
        std::vector<Value> list;
        std::vector<std::pair<Value, Value>> map;

        [[nodiscard]] static Value missing() { return {}; }

        [[nodiscard]] static Value null() {
            Value v;
            v.kind = NULL_VALUE;
            return v;
        }

        [[nodiscard]] static Value boolean(bool value) {
            Value v;
            v.kind = BOOL;
            v.b = value;
            return v;
        }

        [[nodiscard]] static Value integer(int64_t value) {
            Value v;
            v.kind = INT;
            v.i = value;
            return v;
        }

        [[nodiscard]] static Value floating(double value) {
            Value v;
            v.kind = DOUBLE;
            v.d = value;
            return v;
        }

        [[nodiscard]] static Value string(std::string value) {
            Value v;
            v.kind = STRING;
            v.s = std::move(value);
            return v;
        }

        [[nodiscard]] static Value listValue(std::vector<Value> values) {
            Value v;
            v.kind = LIST;
            v.list = std::move(values);
            return v;
        }

        [[nodiscard]] static Value mapValue(std::vector<std::pair<Value, Value>> values) {
            Value v;
            v.kind = MAP;
            v.map = std::move(values);
            return v;
        }

        [[nodiscard]] bool isNumeric() const noexcept { return kind == INT || kind == DOUBLE; }
        [[nodiscard]] double asDouble() const noexcept { return kind == INT ? static_cast<double>(i) : d; }
    };

    [[nodiscard]] Value readBinpackValue(BytesReader& in, const TypeDesc& type);

    void skipBinpackValue(BytesReader& in, const TypeDesc& type) {
        switch (type.kind) {
            case TypeDesc::BOOL:
            case TypeDesc::INT8: in.skip(1);
                break;
            case TypeDesc::INT16: in.skip(2);
                break;
            case TypeDesc::INT32:
            case TypeDesc::FLOAT: in.skip(4);
                break;
            case TypeDesc::INT64:
            case TypeDesc::DOUBLE: in.skip(8);
                break;
            case TypeDesc::STRING: in.skip(static_cast<size_t>(in.u32()));
                break;
            case TypeDesc::NULLABLE: if (in.u8() != 0) { skipBinpackValue(in, *type.elem); }
                break;
            case TypeDesc::STRUCT: for (const auto& field : type.fields) { skipBinpackValue(in, *field.type); }
                break;
            case TypeDesc::LIST: {
                const auto count = in.u32();
                for (uint32_t i = 0; i < count; ++i) { skipBinpackValue(in, *type.elem); }
                break;
            }
            case TypeDesc::MAP: {
                const auto count = in.u32();
                for (uint32_t i = 0; i < count; ++i) {
                    skipBinpackValue(in, *type.key);
                    skipBinpackValue(in, *type.value);
                }
                break;
            }
            default: throw std::runtime_error("Unknown AkkaraDB schema kind while skipping value");
        }
    }

    [[nodiscard]] float bitsToFloat(uint32_t bits) {
        float out;
        std::memcpy(&out, &bits, sizeof(out));
        return out;
    }

    [[nodiscard]] double bitsToDouble(uint64_t bits) {
        double out;
        std::memcpy(&out, &bits, sizeof(out));
        return out;
    }

    [[nodiscard]] Value readBinpackValue(BytesReader& in, const TypeDesc& type) {
        switch (type.kind) {
            case TypeDesc::BOOL: return Value::boolean(in.u8() != 0);
            case TypeDesc::INT8: return Value::integer(static_cast<int8_t>(in.u8()));
            case TypeDesc::INT16: return Value::integer(static_cast<int16_t>(in.u16()));
            case TypeDesc::INT32: return Value::integer(static_cast<int32_t>(in.u32()));
            case TypeDesc::INT64: return Value::integer(static_cast<int64_t>(in.u64()));
            case TypeDesc::FLOAT: return Value::floating(static_cast<double>(bitsToFloat(in.u32())));
            case TypeDesc::DOUBLE: return Value::floating(bitsToDouble(in.u64()));
            case TypeDesc::STRING: return Value::string(in.strI32());
            case TypeDesc::NULLABLE: return in.u8() == 0 ? Value::null() : readBinpackValue(in, *type.elem);
            case TypeDesc::LIST: {
                const auto count = in.u32();
                std::vector<Value> values;
                values.reserve(count);
                for (uint32_t i = 0; i < count; ++i) { values.push_back(readBinpackValue(in, *type.elem)); }
                return Value::listValue(std::move(values));
            }
            case TypeDesc::MAP: {
                const auto count = in.u32();
                std::vector<std::pair<Value, Value>> values;
                values.reserve(count);
                for (uint32_t i = 0; i < count; ++i) {
                    Value key = readBinpackValue(in, *type.key);
                    Value value = readBinpackValue(in, *type.value);
                    values.emplace_back(std::move(key), std::move(value));
                }
                return Value::mapValue(std::move(values));
            }
            case TypeDesc::STRUCT: skipBinpackValue(in, type);
                return Value::missing();
            default: throw std::runtime_error("Unknown AkkaraDB schema kind while reading value");
        }
    }

    [[nodiscard]] std::vector<std::string> splitColumnPath(const std::string& path) {
        std::vector<std::string> out;
        size_t start = 0;
        while (start <= path.size()) {
            const size_t dot = path.find('.', start);
            const size_t end = dot == std::string::npos ? path.size() : dot;
            out.emplace_back(path.substr(start, end - start));
            if (dot == std::string::npos) { break; }
            start = dot + 1;
        }
        return out;
    }

    [[nodiscard]] Value extractFromStruct(BytesReader& in, const TypeDesc& type, const std::vector<std::string>& path, size_t part);

    [[nodiscard]] Value extractNested(BytesReader& in, const TypeDesc& type, const std::vector<std::string>& path, size_t part) {
        if (type.kind == TypeDesc::NULLABLE) {
            if (in.u8() == 0) { return Value::null(); }
            return extractNested(in, *type.elem, path, part);
        }
        if (type.kind != TypeDesc::STRUCT) { throw std::runtime_error("AkkaraDB query column path descends into a non-struct field"); }
        return extractFromStruct(in, type, path, part);
    }

    [[nodiscard]] Value extractFromStruct(BytesReader& in, const TypeDesc& type, const std::vector<std::string>& path, size_t part) {
        if (type.kind != TypeDesc::STRUCT) { throw std::runtime_error("AkkaraDB query root schema is not a struct"); }
        for (const auto& field : type.fields) {
            if (field.name == path[part]) {
                if (part + 1 == path.size()) { return readBinpackValue(in, *field.type); }
                return extractNested(in, *field.type, path, part + 1);
            }
            skipBinpackValue(in, *field.type);
        }
        throw std::runtime_error("AkkaraDB query column not found in schema: " + path[part]);
    }

    [[nodiscard]] Value readColumn(std::span<const uint8_t> rowValue, const TypeDesc& schema, const std::string& name) {
        const auto path = splitColumnPath(name);
        if (path.empty() || path[0].empty()) { throw std::runtime_error("AkkaraDB query column name is empty"); }
        BytesReader in(rowValue);
        return extractFromStruct(in, schema, path, 0);
    }

    struct Expr {
        enum Tag : uint8_t {
            BIN = 0x01, UN = 0x02, LIT = 0x03, COL = 0x04, CAP = 0x05
        };

        Tag tag = LIT;
        uint8_t op = 0;
        Value literal;
        std::string column;
        uint32_t capture = 0;
        std::unique_ptr<Expr> lhs;
        std::unique_ptr<Expr> rhs;
    };

    struct QueryProgram {
        std::vector<Value> captures;
        TypeDesc schema{TypeDesc::STRUCT};
        Expr where;
    };

    [[nodiscard]] Value parseLiteral(BytesReader& in) {
        switch (in.u8()) {
            case 0x01: return Value::boolean(in.u8() != 0);
            case 0x02: return Value::integer(static_cast<int8_t>(in.u8()));
            case 0x03: return Value::integer(static_cast<int16_t>(in.u16()));
            case 0x04: return Value::integer(static_cast<int32_t>(in.u32()));
            case 0x05: return Value::integer(static_cast<int64_t>(in.u64()));
            case 0x06: return Value::floating(static_cast<double>(bitsToFloat(in.u32())));
            case 0x07: return Value::floating(bitsToDouble(in.u64()));
            case 0x08: return Value::string(in.strI32());
            case 0x09: return Value::null();
            case 0x0A: {
                const auto count = in.u32();
                std::vector<Value> values;
                values.reserve(count);
                for (uint32_t i = 0; i < count; ++i) { values.push_back(parseLiteral(in)); }
                return Value::listValue(std::move(values));
            }
            case 0x0B: {
                const auto count = in.u32();
                std::vector<std::pair<Value, Value>> values;
                values.reserve(count);
                for (uint32_t i = 0; i < count; ++i) {
                    Value key = parseLiteral(in);
                    Value value = parseLiteral(in);
                    values.emplace_back(std::move(key), std::move(value));
                }
                return Value::mapValue(std::move(values));
            }
            default: throw std::runtime_error("Unknown AkkaraDB query literal tag");
        }
    }

    [[nodiscard]] Expr parseExpr(BytesReader& in) {
        Expr expr;
        expr.tag = static_cast<Expr::Tag>(in.u8());
        switch (expr.tag) {
            case Expr::BIN: expr.op = in.u8();
                expr.lhs = std::make_unique<Expr>(parseExpr(in));
                expr.rhs = std::make_unique<Expr>(parseExpr(in));
                break;
            case Expr::UN: expr.op = in.u8();
                expr.lhs = std::make_unique<Expr>(parseExpr(in));
                break;
            case Expr::LIT: expr.literal = parseLiteral(in);
                break;
            case Expr::COL: expr.column = in.strU16();
                break;
            case Expr::CAP: expr.capture = in.u32();
                break;
            default: throw std::runtime_error("Unknown AkkaraDB query expression tag");
        }
        return expr;
    }

    [[nodiscard]] QueryProgram parseQuery(std::span<const uint8_t> queryBytes, std::span<const uint8_t> schemaBytes) {
        BytesReader in(queryBytes);
        QueryProgram program;
        const auto captureCount = in.u32();
        if (captureCount > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
            throw std::runtime_error("Too many AkkaraDB query captures");
        }
        program.captures.reserve(static_cast<size_t>(captureCount));
        for (uint32_t i = 0; i < captureCount; ++i) { program.captures.push_back(parseLiteral(in)); }
        program.where = parseExpr(in);
        if (!in.eof()) { throw std::runtime_error("Trailing bytes in AkkaraDB query payload"); }
        program.schema = parseRootSchema(schemaBytes);
        return program;
    }

    [[nodiscard]] bool valueIsTrue(const Value& value) noexcept { return value.kind == Value::BOOL && value.b; }

    [[nodiscard]] int compareValues(const Value& lhs, const Value& rhs) {
        if (lhs.isNumeric() && rhs.isNumeric()) {
            const double a = lhs.asDouble();
            const double b = rhs.asDouble();
            return (a > b) - (a < b);
        }
        if (lhs.kind == Value::STRING && rhs.kind == Value::STRING) { return (lhs.s > rhs.s) - (lhs.s < rhs.s); }
        if (lhs.kind == Value::BOOL && rhs.kind == Value::BOOL) { return (lhs.b > rhs.b) - (lhs.b < rhs.b); }
        throw std::runtime_error("AkkaraDB query comparison between incompatible values");
    }

    [[nodiscard]] bool valuesEqual(const Value& lhs, const Value& rhs) {
        if (lhs.kind == Value::NULL_VALUE || rhs.kind == Value::NULL_VALUE) { return lhs.kind == rhs.kind; }
        if (lhs.kind == Value::MISSING || rhs.kind == Value::MISSING) { return false; }
        if (lhs.isNumeric() && rhs.isNumeric()) { return compareValues(lhs, rhs) == 0; }
        if (lhs.kind != rhs.kind) { return false; }
        switch (lhs.kind) {
            case Value::BOOL: return lhs.b == rhs.b;
            case Value::INT: return lhs.i == rhs.i;
            case Value::DOUBLE: return lhs.d == rhs.d;
            case Value::STRING: return lhs.s == rhs.s;
            case Value::LIST: if (lhs.list.size() != rhs.list.size()) { return false; }
                for (size_t i = 0; i < lhs.list.size(); ++i) { if (!valuesEqual(lhs.list[i], rhs.list[i])) { return false; } }
                return true;
            case Value::MAP: if (lhs.map.size() != rhs.map.size()) { return false; }
                for (const auto& [lhsKey, lhsValue] : lhs.map) {
                    bool found = false;
                    for (const auto& [rhsKey, rhsValue] : rhs.map) {
                        if (valuesEqual(lhsKey, rhsKey) && valuesEqual(lhsValue, rhsValue)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) { return false; }
                }
                return true;
            default: return false;
        }
    }

    [[nodiscard]] bool valueInList(const Value& needle, const Value& haystack) {
        if (haystack.kind != Value::LIST) { return false; }
        for (const Value& value : haystack.list) { if (valuesEqual(needle, value)) { return true; } }
        return false;
    }

    [[nodiscard]] Value mapGetValue(const Value& map, const Value& key) {
        if (map.kind != Value::MAP) { return Value::null(); }
        for (const auto& [candidateKey, value] : map.map) { if (valuesEqual(candidateKey, key)) { return value; } }
        return Value::null();
    }

    [[nodiscard]] bool stringStartsWith(const Value& value, const Value& prefix) {
        return value.kind == Value::STRING && prefix.kind == Value::STRING && value.s.starts_with(prefix.s);
    }

    [[nodiscard]] bool stringContains(const Value& value, const Value& needle) {
        return value.kind == Value::STRING && needle.kind == Value::STRING && value.s.find(needle.s) != std::string::npos;
    }

    [[nodiscard]] bool likeMatch(std::string_view value, size_t vi, std::string_view pattern, size_t pi) {
        while (pi < pattern.size()) {
            if (pattern[pi] == '%') {
                while (pi + 1 < pattern.size() && pattern[pi + 1] == '%') { ++pi; }
                if (pi + 1 == pattern.size()) { return true; }
                for (size_t next = vi; next <= value.size(); ++next) { if (likeMatch(value, next, pattern, pi + 1)) { return true; } }
                return false;
            }
            if (pattern[pi] == '_') {
                if (vi >= value.size()) { return false; }
                ++vi;
                ++pi;
                continue;
            }
            if (vi >= value.size() || value[vi] != pattern[pi]) { return false; }
            ++vi;
            ++pi;
        }
        return vi == value.size();
    }

    [[nodiscard]] bool stringLike(const Value& value, const Value& pattern) {
        return value.kind == Value::STRING && pattern.kind == Value::STRING && likeMatch(value.s, 0, pattern.s, 0);
    }

    [[nodiscard]] Value evalExpr(const QueryProgram& program, const Expr& expr, std::span<const uint8_t> rowValue) {
        switch (expr.tag) {
            case Expr::LIT: return expr.literal;
            case Expr::CAP: if (expr.capture >= program.captures.size()) {
                    throw std::runtime_error("AkkaraDB query capture index out of range");
                }
                return program.captures[expr.capture];
            case Expr::COL: return readColumn(rowValue, program.schema, expr.column);
            case Expr::UN: {
                const Value x = evalExpr(program, *expr.lhs, rowValue);
                switch (expr.op) {
                    case 9: return Value::boolean(!valueIsTrue(x));
                    case 12: return Value::boolean(x.kind == Value::NULL_VALUE);
                    case 13: return Value::boolean(x.kind != Value::NULL_VALUE);
                    default: throw std::runtime_error("Unsupported AkkaraDB unary query operator");
                }
            }
            case Expr::BIN: {
                if (expr.op == 7) {
                    const Value lhs = evalExpr(program, *expr.lhs, rowValue);
                    return Value::boolean(valueIsTrue(lhs) && valueIsTrue(evalExpr(program, *expr.rhs, rowValue)));
                }
                if (expr.op == 8) {
                    const Value lhs = evalExpr(program, *expr.lhs, rowValue);
                    return Value::boolean(valueIsTrue(lhs) || valueIsTrue(evalExpr(program, *expr.rhs, rowValue)));
                }

                const Value lhs = evalExpr(program, *expr.lhs, rowValue);
                const Value rhs = evalExpr(program, *expr.rhs, rowValue);
                switch (expr.op) {
                    case 1: return Value::boolean(compareValues(lhs, rhs) > 0);
                    case 2: return Value::boolean(compareValues(lhs, rhs) >= 0);
                    case 3: return Value::boolean(compareValues(lhs, rhs) < 0);
                    case 4: return Value::boolean(compareValues(lhs, rhs) <= 0);
                    case 5: return Value::boolean(valuesEqual(lhs, rhs));
                    case 6: return Value::boolean(!valuesEqual(lhs, rhs));
                    case 9: return Value::boolean(!valueIsTrue(lhs));
                    case 10: return Value::boolean(valueInList(lhs, rhs));
                    case 11: return Value::boolean(!valueInList(lhs, rhs));
                    case 12: return Value::boolean(lhs.kind == Value::NULL_VALUE);
                    case 13: return Value::boolean(lhs.kind != Value::NULL_VALUE);
                    case 14: return mapGetValue(lhs, rhs);
                    case 15: return Value::boolean(stringStartsWith(lhs, rhs));
                    case 16: return Value::boolean(stringContains(lhs, rhs));
                    case 17: return Value::boolean(stringLike(lhs, rhs));
                    default: throw std::runtime_error("Unsupported AkkaraDB binary query operator");
                }
            }
            default: throw std::runtime_error("Unknown AkkaraDB query expression tag");
        }
    }

    [[nodiscard]] bool matchesQuery(const QueryProgram& program, std::span<const uint8_t> rowValue) {
        return valueIsTrue(evalExpr(program, program.where, rowValue));
    }

    struct ScanCursor {
        AkkaraDB* db = nullptr;
        BufferArena arena;
        ArenaGenerator<AkkEngine::ScanRecordView> rows;
        ArenaGenerator<AkkEngine::ScanRecordView>::iterator it;
        std::unique_ptr<QueryProgram> query;
    };

    [[nodiscard]] ScanCursor* cursorFrom(jlong handle) noexcept {
        return reinterpret_cast<ScanCursor*>(static_cast<std::uintptr_t>(handle));
    }

    [[nodiscard]] jobject makeRow(JNIEnv* env, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        jclass rowCls = env->FindClass("dev/swiftstorm/akkaradb/engine/RowView");
        if (rowCls == nullptr) { return nullptr; }
        jmethodID ctor = env->GetMethodID(
            rowCls,
            "<init>",
            "(Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Lkotlin/jvm/internal/DefaultConstructorMarker;)V"
        );
        if (ctor == nullptr) { return nullptr; }
        jobject keyBuffer = makeDirectView(env, key);
        if (keyBuffer == nullptr || env->ExceptionCheck()) { return nullptr; }
        jobject valueBuffer = makeDirectView(env, value);
        if (valueBuffer == nullptr || env->ExceptionCheck()) { return nullptr; }
        return env->NewObject(rowCls, ctor, keyBuffer, valueBuffer, nullptr);
    }

    template <typename F>
    auto guard(JNIEnv* env, F&& f) -> decltype(f()) {
        try { return f(); }
        catch (const std::exception& ex) {
            throwRuntime(env, ex);
            using R = decltype(f());
            if constexpr (std::is_pointer_v<R>) { return nullptr; }
            else if constexpr (std::is_same_v<R, jboolean>) { return JNI_FALSE; }
            else if constexpr (std::is_integral_v<R>) { return 0; }
            else { return R{}; }
        }
    }
}

extern "C" {
    JNIEXPORT jstring JNICALL Java_dev_swiftstorm_akkaradb_engine_NativeLibrary_nativeJniCompatLine(JNIEnv* env, jclass) {
        return env->NewStringUTF(AKKARADB_JNI_COMPAT_LINE);
    }

    JNIEXPORT jstring JNICALL Java_dev_swiftstorm_akkaradb_engine_NativeLibrary_nativeRequiredNativeGeneration(JNIEnv* env, jclass) {
        return env->NewStringUTF(AKKARADB_REQUIRED_NATIVE_GENERATION);
    }

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeOpen(
        JNIEnv* env,
        jclass,
        jstring path,
        jint mode,
        jlong memtableThresholdPerShard,
        jint versionLogEnabled,
        jint sstCodec,
        jint blobCodec,
        jlong blobThresholdBytes,
        jint sstPromoteReads,
        jlong sstBloomBitsPerKey,
        jlong maxL0SstFiles
    ) {
        return guard(
            env,
            [&]() -> jlong {
                AkkaraDB::Options options;
                options.dataDir = readString(env, path);
                options.mode = startupModeFromOrdinal(mode);

                assignOptionalNonNegative(options.overrides.memtableThresholdPerShard, memtableThresholdPerShard);
                assignOptionalBool(options.overrides.versionLogEnabled, versionLogEnabled, "versionLogEnabled");
                if (sstCodec >= 0) { options.overrides.sstCodec = codecFromOrdinal(sstCodec); }
                if (blobCodec >= 0) { options.overrides.blobCodec = codecFromOrdinal(blobCodec); }
                assignOptionalNonNegative(options.overrides.blobThresholdBytes, blobThresholdBytes);
                assignOptionalBool(options.overrides.sstPromoteReads, sstPromoteReads, "sstPromoteReads");
                assignOptionalNonNegative(options.overrides.sstBloomBitsPerKey, sstBloomBitsPerKey);
                assignOptionalNonNegative(options.overrides.maxL0SstFiles, maxL0SstFiles);

                auto db = AkkaraDB::open(std::move(options));
                return toHandle(db.release());
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativePut(
        JNIEnv* env,
        jobject,
        jlong handle,
        jobject key,
        jobject value
    ) {
        guard(
            env,
            [&]() {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto k = readDirectBuffer(env, key);
                const auto v = readDirectBuffer(env, value);
                db->engine().put(k, v);
                return 0;
            }
        );
    }

    JNIEXPORT jobject JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeGet(JNIEnv* env, jobject, jlong handle, jobject key) {
        return guard(
            env,
            [&]() -> jobject {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto k = readDirectBuffer(env, key);
                auto value = db->engine().get(k);
                if (!value) { return nullptr; }
                return makeDirectBuffer(env, *value);
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeRemove(JNIEnv* env, jobject, jlong handle, jobject key) {
        guard(
            env,
            [&]() {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto k = readDirectBuffer(env, key);
                db->engine().remove(k);
                return 0;
            }
        );
    }

    JNIEXPORT jboolean JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeExists(JNIEnv* env, jobject, jlong handle, jobject key) {
        return guard(
            env,
            [&]() -> jboolean {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto k = readDirectBuffer(env, key);
                return db->engine().exists(k) ? JNI_TRUE : JNI_FALSE;
            }
        );
    }

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeCount(
        JNIEnv* env,
        jobject,
        jlong handle,
        jobject startKey,
        jobject endKey
    ) {
        return guard(
            env,
            [&]() -> jlong {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto start = readDirectBuffer(env, startKey);
                const auto end = readDirectBuffer(env, endKey);
                return static_cast<jlong>(db->engine().count(start, end));
            }
        );
    }

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeOpenScan(
        JNIEnv* env,
        jobject,
        jlong handle,
        jobject startKey,
        jobject endKey
    ) {
        return guard(
            env,
            [&]() -> jlong {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto start = readDirectBuffer(env, startKey);
                const auto end = readDirectBuffer(env, endKey);

                auto cursor = std::make_unique<ScanCursor>();
                cursor->db = db;
                cursor->rows = db->engine().scan(cursor->arena, start, end);
                cursor->it = cursor->rows.begin();
                return toHandle(cursor.release());
            }
        );
    }

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeOpenQueryScan(
        JNIEnv* env,
        jobject,
        jlong handle,
        jobject startKey,
        jobject endKey,
        jobject queryBytes,
        jobject schemaBytes
    ) {
        return guard(
            env,
            [&]() -> jlong {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto start = readDirectBuffer(env, startKey);
                const auto end = readDirectBuffer(env, endKey);
                const auto query = readDirectBuffer(env, queryBytes);
                const auto schema = readDirectBuffer(env, schemaBytes);

                auto cursor = std::make_unique<ScanCursor>();
                cursor->db = db;
                cursor->query = std::make_unique<QueryProgram>(parseQuery(query, schema));
                cursor->rows = db->engine().scan(cursor->arena, start, end);
                cursor->it = cursor->rows.begin();
                return toHandle(cursor.release());
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeRollbackTo(
        JNIEnv* env,
        jobject,
        jlong handle,
        jlong targetSeq
    ) {
        guard(
            env,
            [&]() {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                db->engine().rollbackTo(static_cast<uint64_t>(targetSeq));
                return 0;
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeClose(JNIEnv* env, jobject, jlong handle) {
        guard(
            env,
            [&]() {
                std::unique_ptr<AkkaraDB> db{dbFrom(handle)};
                if (db) { db->close(); }
                return 0;
            }
        );
    }

    JNIEXPORT jobject JNICALL Java_dev_swiftstorm_akkaradb_engine_NativeScanCursor_nativeNext(JNIEnv* env, jobject, jlong handle) {
        return guard(
            env,
            [&]() -> jobject {
                auto* cursor = cursorFrom(handle);
                if (cursor == nullptr) { throw std::runtime_error("NativeScanCursor handle is null"); }
                while (cursor->it != cursor->rows.end()) {
                    const auto& row = *cursor->it;
                    const bool matched = cursor->query == nullptr || matchesQuery(*cursor->query, row.value);
                    if (matched) {
                        jobject out = makeRow(env, row.key, row.value);
                        ++cursor->it;
                        return out;
                    }
                    ++cursor->it;
                }
                return nullptr;
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_NativeScanCursor_nativeClose(JNIEnv* env, jobject, jlong handle) {
        guard(
            env,
            [&]() {
                std::unique_ptr<ScanCursor> cursor{cursorFrom(handle)};
                return 0;
            }
        );
    }
}
