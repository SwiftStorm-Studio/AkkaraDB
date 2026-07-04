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

#include <cstdio>
#include <cstring>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
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

    struct JniBindings {
        jclass runtimeExceptionClass = nullptr;
        jclass byteBufferClass = nullptr;
        jmethodID byteBufferAllocateDirect = nullptr;
        jmethodID byteBufferAsReadOnly = nullptr;
        jclass rowViewClass = nullptr;
        jmethodID rowViewCtor = nullptr;
        jclass versionEntryClass = nullptr;
        jmethodID versionEntryCtor = nullptr;
        jclass batchGetResultClass = nullptr;
        jmethodID batchGetResultCtor = nullptr;
    };

    [[nodiscard]] const JniBindings& bindings(JNIEnv* env) {
        static JniBindings cached;
        static std::once_flag once;
        static std::exception_ptr initError;

        std::call_once(
            once,
            [&]() {
                try {
                    jclass runtimeExceptionLocal = env->FindClass("java/lang/RuntimeException");
                    if (runtimeExceptionLocal == nullptr) { throw std::runtime_error("Failed to resolve RuntimeException class"); }
                    cached.runtimeExceptionClass = static_cast<jclass>(env->NewGlobalRef(runtimeExceptionLocal));
                    env->DeleteLocalRef(runtimeExceptionLocal);
                    if (cached.runtimeExceptionClass == nullptr) { throw std::runtime_error("Failed to cache RuntimeException class"); }

                    jclass byteBufferLocal = env->FindClass("java/nio/ByteBuffer");
                    if (byteBufferLocal == nullptr) { throw std::runtime_error("Failed to resolve ByteBuffer class"); }
                    cached.byteBufferClass = static_cast<jclass>(env->NewGlobalRef(byteBufferLocal));
                    env->DeleteLocalRef(byteBufferLocal);
                    if (cached.byteBufferClass == nullptr) { throw std::runtime_error("Failed to cache ByteBuffer class"); }
                    cached.byteBufferAllocateDirect = env->GetStaticMethodID(
                        cached.byteBufferClass,
                        "allocateDirect",
                        "(I)Ljava/nio/ByteBuffer;"
                    );
                    cached.byteBufferAsReadOnly = env->GetMethodID(
                        cached.byteBufferClass,
                        "asReadOnlyBuffer",
                        "()Ljava/nio/ByteBuffer;"
                    );
                    if (cached.byteBufferAllocateDirect == nullptr || cached.byteBufferAsReadOnly == nullptr) {
                        throw std::runtime_error("Failed to resolve ByteBuffer methods");
                    }

                    jclass rowViewLocal = env->FindClass("dev/swiftstorm/akkaradb/engine/RowView");
                    if (rowViewLocal == nullptr) { throw std::runtime_error("Failed to resolve RowView class"); }
                    cached.rowViewClass = static_cast<jclass>(env->NewGlobalRef(rowViewLocal));
                    env->DeleteLocalRef(rowViewLocal);
                    if (cached.rowViewClass == nullptr) { throw std::runtime_error("Failed to cache RowView class"); }
                    cached.rowViewCtor = env->GetMethodID(
                        cached.rowViewClass,
                        "<init>",
                        "(Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Lkotlin/jvm/internal/DefaultConstructorMarker;)V"
                    );
                    if (cached.rowViewCtor == nullptr) { throw std::runtime_error("Failed to resolve RowView constructor"); }

                    jclass versionEntryLocal = env->FindClass("dev/swiftstorm/akkaradb/engine/VersionEntry");
                    if (versionEntryLocal == nullptr) { throw std::runtime_error("Failed to resolve VersionEntry class"); }
                    cached.versionEntryClass = static_cast<jclass>(env->NewGlobalRef(versionEntryLocal));
                    env->DeleteLocalRef(versionEntryLocal);
                    if (cached.versionEntryClass == nullptr) { throw std::runtime_error("Failed to cache VersionEntry class"); }
                    cached.versionEntryCtor = env->GetMethodID(
                        cached.versionEntryClass,
                        "<init>",
                        "(JJJB[B)V"
                    );
                    if (cached.versionEntryCtor == nullptr) { throw std::runtime_error("Failed to resolve VersionEntry constructor"); }

                    jclass batchGetResultLocal = env->FindClass("dev/swiftstorm/akkaradb/engine/BatchGetResult");
                    if (batchGetResultLocal == nullptr) { throw std::runtime_error("Failed to resolve BatchGetResult class"); }
                    cached.batchGetResultClass = static_cast<jclass>(env->NewGlobalRef(batchGetResultLocal));
                    env->DeleteLocalRef(batchGetResultLocal);
                    if (cached.batchGetResultClass == nullptr) { throw std::runtime_error("Failed to cache BatchGetResult class"); }
                    cached.batchGetResultCtor = env->GetMethodID(
                        cached.batchGetResultClass,
                        "<init>",
                        "()V"
                    );
                    if (cached.batchGetResultCtor == nullptr) { throw std::runtime_error("Failed to resolve BatchGetResult constructor"); }
                }
                catch (...) { initError = std::current_exception(); }
            }
        );

        if (initError != nullptr) { std::rethrow_exception(initError); }
        return cached;
    }

    [[nodiscard]] AkkaraDB* dbFrom(jlong handle) noexcept { return reinterpret_cast<AkkaraDB*>(static_cast<std::uintptr_t>(handle)); }

    [[nodiscard]] jlong toHandle(void* ptr) noexcept { return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(ptr)); }

    void throwRuntime(JNIEnv* env, const std::exception& ex) {
        env->ThrowNew(bindings(env).runtimeExceptionClass, ex.what());
    }

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
        const auto& ids = bindings(env);
        jobject out = env->CallStaticObjectMethod(ids.byteBufferClass, ids.byteBufferAllocateDirect, static_cast<jint>(bytes.size()));
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
        const auto& ids = bindings(env);
        jobject direct = env->NewDirectByteBuffer(const_cast<uint8_t*>(bytes.data()), static_cast<jlong>(bytes.size()));
        if (direct == nullptr) { return nullptr; }
        return env->CallObjectMethod(direct, ids.byteBufferAsReadOnly);
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

    [[nodiscard]] akkaradb::engine::AkkEngineOptions::ApiBackend apiBackendFromOrdinal(jint value) {
        switch (value) {
            case 0: return akkaradb::engine::AkkEngineOptions::ApiBackend::HTTP;
            case 1: return akkaradb::engine::AkkEngineOptions::ApiBackend::TCP;
            case 2: return akkaradb::engine::AkkEngineOptions::ApiBackend::GRPC;
            default: throw std::runtime_error("Invalid AkkaraDB ApiBackend ordinal");
        }
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions::ApiTransportMode apiTransportModeFromOrdinal(jint value) {
        switch (value) {
            case 0: return akkaradb::engine::AkkEngineOptions::ApiTransportMode::TLS;
            case 1: return akkaradb::engine::AkkEngineOptions::ApiTransportMode::PLAIN;
            default: throw std::runtime_error("Invalid AkkaraDB ApiTransportMode ordinal");
        }
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions::ApiIoBackend apiIoBackendFromOrdinal(jint value) {
        switch (value) {
            case 0: return akkaradb::engine::AkkEngineOptions::ApiIoBackend::AUTO;
            case 1: return akkaradb::engine::AkkEngineOptions::ApiIoBackend::THREAD_POOL;
            default: throw std::runtime_error("Invalid AkkaraDB ApiIoBackend ordinal");
        }
    }

    [[nodiscard]] bool boolFromInt(jint value, const char* name) {
        switch (value) {
            case 0: return false;
            case 1: return true;
            default: throw std::runtime_error(std::string("Invalid boolean value for ") + name);
        }
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

    template <typename T>
    [[nodiscard]] T checkedCast(jlong value, const char* name) {
        if (value < 0) { throw std::runtime_error(std::string(name) + " must be >= 0"); }
        using LimitT = std::numeric_limits<T>;
        using UnsignedWide = unsigned long long;
        const auto raw = static_cast<UnsignedWide>(value);
        if (raw > static_cast<UnsignedWide>(LimitT::max())) {
            throw std::runtime_error(std::string(name) + " is out of range");
        }
        return static_cast<T>(value);
    }

    class BytesReader {
        public:
            explicit BytesReader(std::span<const uint8_t> bytes) : bytes_(bytes) {}

            [[nodiscard]] bool eof() const noexcept { return pos_ == bytes_.size(); }
            [[nodiscard]] size_t position() const noexcept { return pos_; }
            [[nodiscard]] std::span<const uint8_t> slice(size_t start, size_t len) const {
                if (start > bytes_.size() || len > bytes_.size() - start) {
                    throw std::runtime_error("AkkaraDB query payload slice is out of bounds");
                }
                return bytes_.subspan(start, len);
            }

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

            [[nodiscard]] int32_t i32() {
                return static_cast<int32_t>(u32());
            }

            [[nodiscard]] uint64_t u64() {
                const uint64_t lo = u32();
                const uint64_t hi = u32();
                return lo | (hi << 32);
            }

            [[nodiscard]] int64_t i64() {
                return static_cast<int64_t>(u64());
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
            friend std::string readUtf8String(BytesReader& in);
            friend std::optional<std::string> readOptionalUtf8String(BytesReader& in);
            friend std::optional<std::vector<uint8_t>> readOptionalBytes(BytesReader& in);

            void ensure(size_t len) const {
                if (len > bytes_.size() - pos_) { throw std::runtime_error("Truncated AkkaraDB query payload"); }
            }

            std::span<const uint8_t> bytes_;
            size_t pos_ = 0;
    };

    [[nodiscard]] std::string readUtf8String(BytesReader& in) {
        const auto len = in.i32();
        if (len < 0) { throw std::runtime_error("Negative string length in AkkaraDB options payload"); }
        if (len == 0) { return {}; }
        in.ensure(static_cast<size_t>(len));
        std::string out(reinterpret_cast<const char*>(in.bytes_.data() + in.pos_), static_cast<size_t>(len));
        in.skip(static_cast<size_t>(len));
        return out;
    }

    [[nodiscard]] std::optional<std::string> readOptionalUtf8String(BytesReader& in) {
        const auto len = in.i32();
        if (len < 0) { return std::nullopt; }
        if (len == 0) { return std::string{}; }
        in.ensure(static_cast<size_t>(len));
        std::string out(reinterpret_cast<const char*>(in.bytes_.data() + in.pos_), static_cast<size_t>(len));
        in.skip(static_cast<size_t>(len));
        return out;
    }

    [[nodiscard]] std::optional<std::vector<uint8_t>> readOptionalBytes(BytesReader& in) {
        const auto len = in.i32();
        if (len < 0) { return std::nullopt; }
        std::vector<uint8_t> out(static_cast<size_t>(len));
        if (len == 0) { return out; }
        in.ensure(static_cast<size_t>(len));
        std::memcpy(out.data(), in.bytes_.data() + in.pos_, static_cast<size_t>(len));
        in.skip(static_cast<size_t>(len));
        return out;
    }

    struct TypeDesc;
    using TypePtr = std::shared_ptr<TypeDesc>;

    struct FieldDesc {
        std::string name;
        TypePtr type;
    };

    struct ColumnRef {
        std::string name;
        std::vector<uint8_t> fieldPath;
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

    [[nodiscard]] const TypeDesc& unwrapNullableStruct(const TypeDesc& type, const std::string& name) {
        if (type.kind == TypeDesc::NULLABLE) { return unwrapNullableStruct(*type.elem, name); }
        if (type.kind != TypeDesc::STRUCT) { throw std::runtime_error("AkkaraDB query column path descends into a non-struct field: " + name); }
        return type;
    }

    [[nodiscard]] uint8_t resolveFieldIndex(const TypeDesc& type, std::string_view fieldName, const std::string& name) {
        if (type.fields.size() > static_cast<size_t>(std::numeric_limits<uint8_t>::max())) {
            throw std::runtime_error("AkkaraDB schema has too many fields for JNI query resolution");
        }
        for (size_t i = 0; i < type.fields.size(); ++i) {
            if (type.fields[i].name == fieldName) { return static_cast<uint8_t>(i); }
        }
        throw std::runtime_error("AkkaraDB query column not found in schema: " + name);
    }

    [[nodiscard]] ColumnRef resolveColumn(const TypeDesc& schema, std::string name) {
        const auto parts = splitColumnPath(name);
        if (parts.empty() || parts[0].empty()) { throw std::runtime_error("AkkaraDB query column name is empty"); }

        ColumnRef out;
        out.name = std::move(name);
        out.fieldPath.reserve(parts.size());

        const TypeDesc* current = &schema;
        for (size_t i = 0; i < parts.size(); ++i) {
            const TypeDesc& structType = unwrapNullableStruct(*current, out.name);
            const uint8_t fieldIndex = resolveFieldIndex(structType, parts[i], out.name);
            out.fieldPath.push_back(fieldIndex);
            current = structType.fields[fieldIndex].type.get();
        }
        return out;
    }

    [[nodiscard]] Value extractFromStruct(BytesReader& in, const TypeDesc& type, const ColumnRef& column, size_t part);

    [[nodiscard]] Value extractNested(BytesReader& in, const TypeDesc& type, const ColumnRef& column, size_t part) {
        if (type.kind == TypeDesc::NULLABLE) {
            if (in.u8() == 0) { return Value::null(); }
            return extractNested(in, *type.elem, column, part);
        }
        if (type.kind != TypeDesc::STRUCT) {
            throw std::runtime_error("AkkaraDB query column path descends into a non-struct field: " + column.name);
        }
        return extractFromStruct(in, type, column, part);
    }

    [[nodiscard]] Value extractFromStruct(BytesReader& in, const TypeDesc& type, const ColumnRef& column, size_t part) {
        if (type.kind != TypeDesc::STRUCT) { throw std::runtime_error("AkkaraDB query root schema is not a struct"); }
        const size_t fieldIndex = column.fieldPath[part];
        if (fieldIndex >= type.fields.size()) {
            throw std::runtime_error("AkkaraDB query column field index is out of range: " + column.name);
        }
        for (size_t i = 0; i < type.fields.size(); ++i) {
            const auto& field = type.fields[i];
            if (i == fieldIndex) {
                if (part + 1 == column.fieldPath.size()) { return readBinpackValue(in, *field.type); }
                return extractNested(in, *field.type, column, part + 1);
            }
            skipBinpackValue(in, *field.type);
        }
        throw std::runtime_error("AkkaraDB query column not found in schema: " + column.name);
    }

    [[nodiscard]] Value readColumn(std::span<const uint8_t> rowValue, const TypeDesc& schema, const ColumnRef& column) {
        BytesReader in(rowValue);
        return extractFromStruct(in, schema, column, 0);
    }

    struct Expr {
        enum Tag : uint8_t {
            BIN = 0x01, UN = 0x02, LIT = 0x03, COL = 0x04, CAP = 0x05
        };

        Tag tag = LIT;
        uint8_t op = 0;
        Value literal;
        ColumnRef column;
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

    [[nodiscard]] Expr parseExpr(BytesReader& in, const TypeDesc& schema) {
        Expr expr;
        expr.tag = static_cast<Expr::Tag>(in.u8());
        switch (expr.tag) {
            case Expr::BIN: expr.op = in.u8();
                expr.lhs = std::make_unique<Expr>(parseExpr(in, schema));
                expr.rhs = std::make_unique<Expr>(parseExpr(in, schema));
                break;
            case Expr::UN: expr.op = in.u8();
                expr.lhs = std::make_unique<Expr>(parseExpr(in, schema));
                break;
            case Expr::LIT: expr.literal = parseLiteral(in);
                break;
            case Expr::COL: expr.column = resolveColumn(schema, in.strU16());
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
        program.schema = parseRootSchema(schemaBytes);
        const auto captureCount = in.u32();
        if (captureCount > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
            throw std::runtime_error("Too many AkkaraDB query captures");
        }
        program.captures.reserve(static_cast<size_t>(captureCount));
        for (uint32_t i = 0; i < captureCount; ++i) { program.captures.push_back(parseLiteral(in)); }
        program.where = parseExpr(in, program.schema);
        if (!in.eof()) { throw std::runtime_error("Trailing bytes in AkkaraDB query payload"); }
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
        BufferArena arena;
        ArenaGenerator<AkkEngine::ScanRecordView> rows;
        ArenaGenerator<AkkEngine::ScanRecordView>::iterator it;
        std::unique_ptr<QueryProgram> query;
        struct IndexLookup {
            AkkaraDB* db = nullptr;
            std::vector<uint8_t> tablePrefix;
            size_t searchPrefixSize = 0;
        };
        std::unique_ptr<IndexLookup> indexLookup;
    };

    [[nodiscard]] ScanCursor* cursorFrom(jlong handle) noexcept {
        return reinterpret_cast<ScanCursor*>(static_cast<std::uintptr_t>(handle));
    }

    struct NativeIndexDef {
        std::string fieldName;
        std::vector<uint8_t> prefix;
        ColumnRef column;
    };

    struct NativeTable {
        AkkaraDB* db = nullptr;
        std::vector<uint8_t> tablePrefix;
        TypeDesc schema{TypeDesc::STRUCT};
        std::vector<NativeIndexDef> indexes;
    };

    [[nodiscard]] NativeTable* tableFrom(jlong handle) noexcept {
        return reinterpret_cast<NativeTable*>(static_cast<std::uintptr_t>(handle));
    }

    [[nodiscard]] std::string readJavaString(JNIEnv* env, jstring value) {
        if (value == nullptr) { throw std::runtime_error("AkkaraDB JNI string argument is null"); }
        const char* chars = env->GetStringUTFChars(value, nullptr);
        if (chars == nullptr || env->ExceptionCheck()) { throw std::runtime_error("AkkaraDB JNI failed to read string argument"); }
        std::string out{chars};
        env->ReleaseStringUTFChars(value, chars);
        return out;
    }

    [[nodiscard]] std::vector<uint8_t> concatBytes(std::span<const uint8_t> first, std::span<const uint8_t> second) {
        std::vector<uint8_t> out;
        out.reserve(first.size() + second.size());
        out.insert(out.end(), first.begin(), first.end());
        out.insert(out.end(), second.begin(), second.end());
        return out;
    }

    void appendU32Le(std::vector<uint8_t>& out, uint32_t value) {
        out.push_back(static_cast<uint8_t>(value & 0xFF));
        out.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
        out.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
        out.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
    }

    [[nodiscard]] bool incrementLexicographic(std::vector<uint8_t>& bytes) noexcept {
        for (size_t i = bytes.size(); i-- > 0;) {
            const uint16_t next = static_cast<uint16_t>(bytes[i]) + 1;
            bytes[i] = static_cast<uint8_t>(next);
            if (next <= 0xFF) { return true; }
        }
        return false;
    }

    void buildIndexSearchPrefix(std::span<const uint8_t> prefix, std::span<const uint8_t> fieldBytes, std::vector<uint8_t>& out) {
        if (fieldBytes.size() > static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
            throw std::runtime_error("AkkaraDB JNI index field is too large");
        }
        out.clear();
        out.reserve(prefix.size() + 4 + fieldBytes.size());
        out.insert(out.end(), prefix.begin(), prefix.end());
        appendU32Le(out, static_cast<uint32_t>(fieldBytes.size()));
        out.insert(out.end(), fieldBytes.begin(), fieldBytes.end());
    }

    [[nodiscard]] const NativeIndexDef& requireIndex(const NativeTable& table, std::string_view fieldName) {
        for (const auto& index : table.indexes) {
            if (index.fieldName == fieldName) { return index; }
        }
        throw std::runtime_error("AkkaraDB secondary index is not registered for field: " + std::string(fieldName));
    }

    [[nodiscard]] const TypeDesc& resolveColumnLeafType(const TypeDesc& schema, const ColumnRef& column) {
        const TypeDesc* current = &schema;
        for (uint8_t part : column.fieldPath) {
            while (current->kind == TypeDesc::NULLABLE) { current = current->elem.get(); }
            if (current->kind != TypeDesc::STRUCT || part >= current->fields.size()) {
                throw std::runtime_error("AkkaraDB index column path is invalid: " + column.name);
            }
            current = current->fields[part].type.get();
        }
        return *current;
    }

    [[nodiscard]] std::span<const uint8_t> extractEncodedFieldFromStruct(BytesReader& in, const TypeDesc& type, const ColumnRef& column, size_t part);

    [[nodiscard]] std::span<const uint8_t> extractEncodedFieldNested(BytesReader& in, const TypeDesc& type, const ColumnRef& column, size_t part) {
        if (type.kind == TypeDesc::NULLABLE) {
            const size_t start = in.position();
            if (in.u8() == 0) { return in.slice(start, in.position() - start); }
            return extractEncodedFieldNested(in, *type.elem, column, part);
        }
        if (type.kind != TypeDesc::STRUCT) {
            throw std::runtime_error("AkkaraDB index column path descends into a non-struct field: " + column.name);
        }
        return extractEncodedFieldFromStruct(in, type, column, part);
    }

    [[nodiscard]] std::span<const uint8_t> extractEncodedFieldFromStruct(BytesReader& in, const TypeDesc& type, const ColumnRef& column, size_t part) {
        if (type.kind != TypeDesc::STRUCT) { throw std::runtime_error("AkkaraDB index root schema is not a struct"); }
        const size_t fieldIndex = column.fieldPath[part];
        if (fieldIndex >= type.fields.size()) {
            throw std::runtime_error("AkkaraDB index column field index is out of range: " + column.name);
        }
        for (size_t i = 0; i < type.fields.size(); ++i) {
            const auto& field = type.fields[i];
            if (i == fieldIndex) {
                if (part + 1 == column.fieldPath.size()) {
                    const size_t start = in.position();
                    skipBinpackValue(in, *field.type);
                    return in.slice(start, in.position() - start);
                }
                return extractEncodedFieldNested(in, *field.type, column, part + 1);
            }
            skipBinpackValue(in, *field.type);
        }
        throw std::runtime_error("AkkaraDB index column not found in schema: " + column.name);
    }

    [[nodiscard]] std::span<const uint8_t> readEncodedField(std::span<const uint8_t> rowValue, const TypeDesc& schema, const ColumnRef& column) {
        BytesReader in(rowValue);
        return extractEncodedFieldFromStruct(in, schema, column, 0);
    }

    [[nodiscard]] uint16_t readU16Le(std::span<const uint8_t> bytes) {
        return static_cast<uint16_t>(bytes[0]) | (static_cast<uint16_t>(bytes[1]) << 8);
    }

    [[nodiscard]] uint32_t readU32Le(std::span<const uint8_t> bytes) {
        return static_cast<uint32_t>(bytes[0])
            | (static_cast<uint32_t>(bytes[1]) << 8)
            | (static_cast<uint32_t>(bytes[2]) << 16)
            | (static_cast<uint32_t>(bytes[3]) << 24);
    }

    [[nodiscard]] uint64_t readU64Le(std::span<const uint8_t> bytes) {
        return static_cast<uint64_t>(readU32Le(bytes))
            | (static_cast<uint64_t>(readU32Le(bytes.subspan(4, 4))) << 32);
    }

    void appendSortableInt(std::vector<uint8_t>& out, uint64_t value, size_t size) {
        out.resize(size);
        for (size_t i = 0; i < size; ++i) {
            out[i] = static_cast<uint8_t>(value >> ((size - 1 - i) * 8));
        }
    }

    void encodeIndexFieldFromRow(
        std::span<const uint8_t> rowValue,
        const TypeDesc& schema,
        const ColumnRef& column,
        std::vector<uint8_t>& out
    ) {
        const auto encoded = readEncodedField(rowValue, schema, column);
        const TypeDesc& leafType = resolveColumnLeafType(schema, column);
        out.clear();
        switch (leafType.kind) {
            case TypeDesc::INT8:
                out.push_back(static_cast<uint8_t>(static_cast<int8_t>(encoded[0]) ^ static_cast<int8_t>(0x80)));
                return;
            case TypeDesc::INT16: {
                const auto value = static_cast<uint16_t>(static_cast<int16_t>(readU16Le(encoded)) ^ static_cast<int16_t>(0x8000));
                appendSortableInt(out, value, 2);
                return;
            }
            case TypeDesc::INT32: {
                const auto value = static_cast<uint32_t>(static_cast<int32_t>(readU32Le(encoded)) ^ std::numeric_limits<int32_t>::min());
                appendSortableInt(out, value, 4);
                return;
            }
            case TypeDesc::INT64: {
                const auto value = readU64Le(encoded) ^ (1ull << 63);
                appendSortableInt(out, value, 8);
                return;
            }
            case TypeDesc::FLOAT: {
                const uint32_t raw = readU32Le(encoded);
                const uint32_t sortable = (raw & 0x80000000u) != 0u ? ~raw : (raw ^ 0x80000000u);
                appendSortableInt(out, sortable, 4);
                return;
            }
            case TypeDesc::DOUBLE: {
                const uint64_t raw = readU64Le(encoded);
                const uint64_t sortable = (raw & (1ull << 63)) != 0ull ? ~raw : (raw ^ (1ull << 63));
                appendSortableInt(out, sortable, 8);
                return;
            }
            default:
                out.insert(out.end(), encoded.begin(), encoded.end());
                return;
        }
    }

    void updateIndexEntries(
        NativeTable& table,
        std::span<const uint8_t> pkBytes,
        std::span<const uint8_t> rowValue,
        bool remove
    ) {
        std::vector<uint8_t> fieldBytes;
        std::vector<uint8_t> key;
        for (const auto& index : table.indexes) {
            encodeIndexFieldFromRow(rowValue, table.schema, index.column, fieldBytes);
            buildIndexSearchPrefix(index.prefix, fieldBytes, key);
            key.insert(key.end(), pkBytes.begin(), pkBytes.end());
            if (remove) { table.db->engine().remove(key); }
            else { table.db->engine().put(key, std::span<const uint8_t>{}); }
        }
    }

    [[nodiscard]] jobject makeRow(JNIEnv* env, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        const auto& ids = bindings(env);
        jobject keyBuffer = makeDirectView(env, key);
        if (keyBuffer == nullptr || env->ExceptionCheck()) { return nullptr; }
        jobject valueBuffer = makeDirectView(env, value);
        if (valueBuffer == nullptr || env->ExceptionCheck()) { return nullptr; }
        return env->NewObject(ids.rowViewClass, ids.rowViewCtor, keyBuffer, valueBuffer, nullptr);
    }

    [[nodiscard]] jbyteArray makeByteArray(JNIEnv* env, std::span<const uint8_t> bytes) {
        if (bytes.size() > static_cast<size_t>(std::numeric_limits<jsize>::max())) {
            throw std::runtime_error("AkkaraDB JNI byte array is too large");
        }
        jbyteArray out = env->NewByteArray(static_cast<jsize>(bytes.size()));
        if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
        if (!bytes.empty()) {
            env->SetByteArrayRegion(
                out,
                0,
                static_cast<jsize>(bytes.size()),
                reinterpret_cast<const jbyte*>(bytes.data())
            );
            if (env->ExceptionCheck()) { return nullptr; }
        }
        return out;
    }

    [[nodiscard]] jobject makeVersionEntry(JNIEnv* env, const akkaradb::engine::VersionEntry& entry) {
        const auto& ids = bindings(env);
        jbyteArray rawValue = makeByteArray(env, entry.value);
        if (rawValue == nullptr && env->ExceptionCheck()) { return nullptr; }
        return env->NewObject(
            ids.versionEntryClass,
            ids.versionEntryCtor,
            static_cast<jlong>(entry.seq),
            static_cast<jlong>(entry.sourceNodeId),
            static_cast<jlong>(entry.timestampNs),
            static_cast<jbyte>(entry.flags),
            rawValue
        );
    }

    jfieldID requireField(JNIEnv* env, jclass cls, const char* name, const char* sig) {
        jfieldID field = env->GetFieldID(cls, name, sig);
        if (field == nullptr || env->ExceptionCheck()) {
            throw std::runtime_error(std::string("Failed to resolve field: ") + name);
        }
        return field;
    }

    void setBooleanField(JNIEnv* env, jobject target, jclass cls, const char* name, bool value) {
        env->SetBooleanField(target, requireField(env, cls, name, "Z"), value ? JNI_TRUE : JNI_FALSE);
    }

    void setIntField(JNIEnv* env, jobject target, jclass cls, const char* name, jint value) {
        env->SetIntField(target, requireField(env, cls, name, "I"), value);
    }

    void setLongField(JNIEnv* env, jobject target, jclass cls, const char* name, jlong value) {
        env->SetLongField(target, requireField(env, cls, name, "J"), value);
    }

    void setObjectField(JNIEnv* env, jobject target, jclass cls, const char* name, const char* sig, jobject value) {
        env->SetObjectField(target, requireField(env, cls, name, sig), value);
    }

    void readDirectBufferArray(JNIEnv* env, jobjectArray array, std::vector<std::span<const uint8_t>>& out) {
        if (array == nullptr) { throw std::runtime_error("JNI buffer array must not be null"); }
        const jsize size = env->GetArrayLength(array);
        out.clear();
        out.reserve(static_cast<size_t>(size));
        for (jsize i = 0; i < size; ++i) {
            jobject buffer = env->GetObjectArrayElement(array, i);
            if (buffer == nullptr) { throw std::runtime_error("JNI buffer array contains null entry"); }
            out.push_back(readDirectBuffer(env, buffer));
            env->DeleteLocalRef(buffer);
        }
    }

    [[nodiscard]] jobject makeBatchGetResult(JNIEnv* env, const AkkEngine::BatchGetResult& result) {
        const auto& ids = bindings(env);
        jobject out = env->NewObject(ids.batchGetResultClass, ids.batchGetResultCtor);
        if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
        jbyteArray value = makeByteArray(env, result.value);
        if (value == nullptr && env->ExceptionCheck()) { return nullptr; }
        setBooleanField(env, out, ids.batchGetResultClass, "found", result.found);
        setObjectField(env, out, ids.batchGetResultClass, "valueBytes", "[B", value);
        return out;
    }

    [[nodiscard]] jobject makeLevelStats(JNIEnv* env, const akkaradb::engine::LevelStats& stats) {
        jclass cls = env->FindClass("dev/swiftstorm/akkaradb/engine/LevelStats");
        if (cls == nullptr) { return nullptr; }
        jmethodID ctor = env->GetMethodID(cls, "<init>", "()V");
        if (ctor == nullptr) { return nullptr; }
        jobject out = env->NewObject(cls, ctor);
        if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
        setIntField(env, out, cls, "level", static_cast<jint>(stats.level));
        setLongField(env, out, cls, "fileCount", static_cast<jlong>(stats.fileCount));
        setLongField(env, out, cls, "bytes", static_cast<jlong>(stats.bytes));
        setLongField(env, out, cls, "budgetBytes", static_cast<jlong>(stats.budgetBytes));
        return out;
    }

    [[nodiscard]] jobject makeApiStats(JNIEnv* env, const akkaradb::engine::EngineStats::ApiStats& stats) {
        jclass cls = env->FindClass("dev/swiftstorm/akkaradb/engine/ApiStats");
        if (cls == nullptr) { return nullptr; }
        jmethodID ctor = env->GetMethodID(cls, "<init>", "()V");
        if (ctor == nullptr) { return nullptr; }
        jobject out = env->NewObject(cls, ctor);
        if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
        setBooleanField(env, out, cls, "enabled", stats.enabled);
        setBooleanField(env, out, cls, "httpEnabled", stats.httpEnabled);
        setBooleanField(env, out, cls, "httpTlsEnabled", stats.httpTlsEnabled);
        setIntField(env, out, cls, "httpPort", static_cast<jint>(stats.httpPort));
        setLongField(env, out, cls, "httpMaxBatchItems", static_cast<jlong>(stats.httpMaxBatchItems));
        setLongField(env, out, cls, "httpMaxScanItems", static_cast<jlong>(stats.httpMaxScanItems));
        setLongField(env, out, cls, "httpMaxHistoryEntries", static_cast<jlong>(stats.httpMaxHistoryEntries));
        setLongField(env, out, cls, "httpMaxContentLength", static_cast<jlong>(stats.httpMaxContentLength));
        setLongField(env, out, cls, "httpConnectionsAcceptedTotal", static_cast<jlong>(stats.httpConnectionsAcceptedTotal));
        setLongField(env, out, cls, "httpConnectionsClosedTotal", static_cast<jlong>(stats.httpConnectionsClosedTotal));
        setLongField(env, out, cls, "httpConnectionsActive", static_cast<jlong>(stats.httpConnectionsActive));
        setLongField(env, out, cls, "httpRequestsTotal", static_cast<jlong>(stats.httpRequestsTotal));
        setLongField(env, out, cls, "httpResponsesTotal", static_cast<jlong>(stats.httpResponsesTotal));
        setLongField(env, out, cls, "httpBytesReceivedTotal", static_cast<jlong>(stats.httpBytesReceivedTotal));
        setLongField(env, out, cls, "httpBytesSentTotal", static_cast<jlong>(stats.httpBytesSentTotal));
        setLongField(env, out, cls, "httpProtocolErrorsTotal", static_cast<jlong>(stats.httpProtocolErrorsTotal));
        setLongField(env, out, cls, "httpErrorsTotal", static_cast<jlong>(stats.httpErrorsTotal));
        setLongField(env, out, cls, "httpBatchPutItemsTotal", static_cast<jlong>(stats.httpBatchPutItemsTotal));
        setLongField(env, out, cls, "httpBatchGetItemsTotal", static_cast<jlong>(stats.httpBatchGetItemsTotal));
        setBooleanField(env, out, cls, "tcpEnabled", stats.tcpEnabled);
        setBooleanField(env, out, cls, "tcpTlsEnabled", stats.tcpTlsEnabled);
        setIntField(env, out, cls, "tcpIoBackend", static_cast<jint>(stats.tcpIoBackend));
        setLongField(env, out, cls, "tcpWorkerThreads", static_cast<jlong>(stats.tcpWorkerThreads));
        setLongField(env, out, cls, "tcpAcceptQueueLimit", static_cast<jlong>(stats.tcpAcceptQueueLimit));
        setLongField(env, out, cls, "tcpAcceptQueueTimeoutMs", static_cast<jlong>(stats.tcpAcceptQueueTimeoutMs));
        setLongField(env, out, cls, "tcpListenBacklog", static_cast<jlong>(stats.tcpListenBacklog));
        setLongField(env, out, cls, "tcpReadTimeoutMs", static_cast<jlong>(stats.tcpReadTimeoutMs));
        setLongField(env, out, cls, "tcpWriteTimeoutMs", static_cast<jlong>(stats.tcpWriteTimeoutMs));
        setLongField(env, out, cls, "tcpConnectionsAcceptedTotal", static_cast<jlong>(stats.tcpConnectionsAcceptedTotal));
        setLongField(env, out, cls, "tcpConnectionsClosedTotal", static_cast<jlong>(stats.tcpConnectionsClosedTotal));
        setLongField(env, out, cls, "tcpConnectionsActive", static_cast<jlong>(stats.tcpConnectionsActive));
        setLongField(env, out, cls, "tcpAcceptQueueDepth", static_cast<jlong>(stats.tcpAcceptQueueDepth));
        setLongField(env, out, cls, "tcpAcceptQueuePeakDepth", static_cast<jlong>(stats.tcpAcceptQueuePeakDepth));
        setLongField(env, out, cls, "tcpAcceptQueueRejectedTotal", static_cast<jlong>(stats.tcpAcceptQueueRejectedTotal));
        setLongField(env, out, cls, "tcpAcceptQueueExpiredTotal", static_cast<jlong>(stats.tcpAcceptQueueExpiredTotal));
        setLongField(env, out, cls, "tcpRequestsTotal", static_cast<jlong>(stats.tcpRequestsTotal));
        setLongField(env, out, cls, "tcpResponsesTotal", static_cast<jlong>(stats.tcpResponsesTotal));
        setLongField(env, out, cls, "tcpBytesReceivedTotal", static_cast<jlong>(stats.tcpBytesReceivedTotal));
        setLongField(env, out, cls, "tcpBytesSentTotal", static_cast<jlong>(stats.tcpBytesSentTotal));
        setLongField(env, out, cls, "tcpProtocolErrorsTotal", static_cast<jlong>(stats.tcpProtocolErrorsTotal));
        setLongField(env, out, cls, "tcpCrcErrorsTotal", static_cast<jlong>(stats.tcpCrcErrorsTotal));
        setLongField(env, out, cls, "tcpPipelineBatchesTotal", static_cast<jlong>(stats.tcpPipelineBatchesTotal));
        setLongField(env, out, cls, "tcpBackpressureFlushesTotal", static_cast<jlong>(stats.tcpBackpressureFlushesTotal));
        setLongField(env, out, cls, "tcpBackpressureDisconnectsTotal", static_cast<jlong>(stats.tcpBackpressureDisconnectsTotal));
        setLongField(env, out, cls, "tcpBatchPutItemsTotal", static_cast<jlong>(stats.tcpBatchPutItemsTotal));
        setLongField(env, out, cls, "tcpBatchGetItemsTotal", static_cast<jlong>(stats.tcpBatchGetItemsTotal));
        setBooleanField(env, out, cls, "grpcEnabled", stats.grpcEnabled);
        setBooleanField(env, out, cls, "grpcTlsEnabled", stats.grpcTlsEnabled);
        setIntField(env, out, cls, "grpcPort", static_cast<jint>(stats.grpcPort));
        setLongField(env, out, cls, "grpcWorkerThreads", static_cast<jlong>(stats.grpcWorkerThreads));
        setLongField(env, out, cls, "grpcCompletionQueues", static_cast<jlong>(stats.grpcCompletionQueues));
        setLongField(env, out, cls, "grpcMinPollers", static_cast<jlong>(stats.grpcMinPollers));
        setLongField(env, out, cls, "grpcMaxPollers", static_cast<jlong>(stats.grpcMaxPollers));
        setLongField(env, out, cls, "grpcMaxConcurrentStreams", static_cast<jlong>(stats.grpcMaxConcurrentStreams));
        setLongField(env, out, cls, "grpcResourceQuotaBytes", static_cast<jlong>(stats.grpcResourceQuotaBytes));
        setLongField(env, out, cls, "grpcMaxBatchItems", static_cast<jlong>(stats.grpcMaxBatchItems));
        setLongField(env, out, cls, "grpcMaxScanItems", static_cast<jlong>(stats.grpcMaxScanItems));
        setLongField(env, out, cls, "grpcMaxHistoryEntries", static_cast<jlong>(stats.grpcMaxHistoryEntries));
        setLongField(env, out, cls, "grpcRequestsTotal", static_cast<jlong>(stats.grpcRequestsTotal));
        setLongField(env, out, cls, "grpcResponsesTotal", static_cast<jlong>(stats.grpcResponsesTotal));
        setLongField(env, out, cls, "grpcActiveRequests", static_cast<jlong>(stats.grpcActiveRequests));
        setLongField(env, out, cls, "grpcErrorsTotal", static_cast<jlong>(stats.grpcErrorsTotal));
        setLongField(env, out, cls, "grpcBatchPutItemsTotal", static_cast<jlong>(stats.grpcBatchPutItemsTotal));
        setLongField(env, out, cls, "grpcBatchGetItemsTotal", static_cast<jlong>(stats.grpcBatchGetItemsTotal));
        return out;
    }

    template <typename TStats>
    [[nodiscard]] jobject makePlainStats(JNIEnv* env, const char* className, const TStats& /*stats*/, std::initializer_list<std::pair<const char*, jlong>> longs, std::initializer_list<std::pair<const char*, bool>> bools = {}) {
        jclass cls = env->FindClass(className);
        if (cls == nullptr) { return nullptr; }
        jmethodID ctor = env->GetMethodID(cls, "<init>", "()V");
        if (ctor == nullptr) { return nullptr; }
        jobject out = env->NewObject(cls, ctor);
        if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
        for (const auto& [name, value] : bools) { setBooleanField(env, out, cls, name, value); }
        for (const auto& [name, value] : longs) { setLongField(env, out, cls, name, value); }
        return out;
    }

    [[nodiscard]] jobject makeEngineStats(JNIEnv* env, const akkaradb::engine::EngineStats& stats) {
        jclass cls = env->FindClass("dev/swiftstorm/akkaradb/engine/EngineStats");
        if (cls == nullptr) { return nullptr; }
        jmethodID ctor = env->GetMethodID(cls, "<init>", "()V");
        if (ctor == nullptr) { return nullptr; }
        jobject out = env->NewObject(cls, ctor);
        if (out == nullptr || env->ExceptionCheck()) { return nullptr; }

        jobject api = makeApiStats(env, stats.api);
        jobject memtable = makePlainStats(
            env,
            "dev/swiftstorm/akkaradb/engine/MemTableStats",
            stats.memtable,
            {
                {"shardCount", static_cast<jlong>(stats.memtable.shardCount)},
                {"thresholdBytesPerShard", static_cast<jlong>(stats.memtable.thresholdBytesPerShard)},
                {"approxBytes", static_cast<jlong>(stats.memtable.approxBytes)},
                {"putsApplied", static_cast<jlong>(stats.memtable.putsApplied)},
                {"removesApplied", static_cast<jlong>(stats.memtable.removesApplied)},
                {"flushesCompleted", static_cast<jlong>(stats.memtable.flushesCompleted)},
                {"bytesFlushed", static_cast<jlong>(stats.memtable.bytesFlushed)}
            }
        );
        jobject wal = makePlainStats(
            env,
            "dev/swiftstorm/akkaradb/engine/WalStats",
            stats.wal,
            {
                {"shardCount", static_cast<jlong>(stats.wal.shardCount)},
                {"entriesWritten", static_cast<jlong>(stats.wal.entriesWritten)},
                {"bytesWritten", static_cast<jlong>(stats.wal.bytesWritten)},
                {"batchesFlushed", static_cast<jlong>(stats.wal.batchesFlushed)},
                {"syncsExecuted", static_cast<jlong>(stats.wal.syncsExecuted)},
                {"segmentRotations", static_cast<jlong>(stats.wal.segmentRotations)}
            },
            {{"enabled", stats.wal.enabled}}
        );
        jobject blob = makePlainStats(
            env,
            "dev/swiftstorm/akkaradb/engine/BlobStats",
            stats.blob,
            {
                {"thresholdBytes", static_cast<jlong>(stats.blob.thresholdBytes)},
                {"blobsWritten", static_cast<jlong>(stats.blob.blobsWritten)},
                {"bytesUncompressed", static_cast<jlong>(stats.blob.bytesUncompressed)},
                {"bytesOnDisk", static_cast<jlong>(stats.blob.bytesOnDisk)},
                {"blobsDeleted", static_cast<jlong>(stats.blob.blobsDeleted)},
                {"gcCycles", static_cast<jlong>(stats.blob.gcCycles)}
            },
            {{"enabled", stats.blob.enabled}}
        );
        jobject vlog = makePlainStats(
            env,
            "dev/swiftstorm/akkaradb/engine/VLogStats",
            stats.vlog,
            {
                {"syncMode", static_cast<jlong>(stats.vlog.syncMode)},
                {"groupN", static_cast<jlong>(stats.vlog.groupN)},
                {"groupMicros", static_cast<jlong>(stats.vlog.groupMicros)},
                {"groupBytes", static_cast<jlong>(stats.vlog.groupBytes)},
                {"asyncMaxPendingBytes", static_cast<jlong>(stats.vlog.asyncMaxPendingBytes)},
                {"indexedKeys", static_cast<jlong>(stats.vlog.indexedKeys)},
                {"indexedEntries", static_cast<jlong>(stats.vlog.indexedEntries)},
                {"rollbackEntries", static_cast<jlong>(stats.vlog.rollbackEntries)},
                {"pendingWrites", static_cast<jlong>(stats.vlog.pendingWrites)},
                {"pendingBytes", static_cast<jlong>(stats.vlog.pendingBytes)},
                {"durableBytes", static_cast<jlong>(stats.vlog.durableBytes)}
            },
            {
                {"enabled", stats.vlog.enabled},
                {"flushThreadRunning", stats.vlog.flushThreadRunning}
            }
        );

        jclass sstCls = env->FindClass("dev/swiftstorm/akkaradb/engine/SstStats");
        if (sstCls == nullptr) { return nullptr; }
        jmethodID sstCtor = env->GetMethodID(sstCls, "<init>", "()V");
        if (sstCtor == nullptr) { return nullptr; }
        jobject sst = env->NewObject(sstCls, sstCtor);
        if (sst == nullptr || env->ExceptionCheck()) { return nullptr; }
        jclass levelCls = env->FindClass("dev/swiftstorm/akkaradb/engine/LevelStats");
        if (levelCls == nullptr) { return nullptr; }
        jobjectArray levels = env->NewObjectArray(static_cast<jsize>(stats.sst.levels.size()), levelCls, nullptr);
        if (levels == nullptr || env->ExceptionCheck()) { return nullptr; }
        for (jsize i = 0; i < static_cast<jsize>(stats.sst.levels.size()); ++i) {
            jobject level = makeLevelStats(env, stats.sst.levels[static_cast<size_t>(i)]);
            if (level == nullptr || env->ExceptionCheck()) { return nullptr; }
            env->SetObjectArrayElement(levels, i, level);
            if (env->ExceptionCheck()) { return nullptr; }
            env->DeleteLocalRef(level);
        }
        setBooleanField(env, sst, sstCls, "enabled", stats.sst.enabled);
        setObjectField(env, sst, sstCls, "levels", "[Ldev/swiftstorm/akkaradb/engine/LevelStats;", levels);
        setLongField(env, sst, sstCls, "fileCount", static_cast<jlong>(stats.sst.fileCount));
        setLongField(env, sst, sstCls, "bytes", static_cast<jlong>(stats.sst.bytes));
        setLongField(env, sst, sstCls, "l0FileCount", static_cast<jlong>(stats.sst.l0FileCount));
        setBooleanField(env, sst, sstCls, "compactionPending", stats.sst.compactionPending);
        setLongField(env, sst, sstCls, "compactionsCompleted", static_cast<jlong>(stats.sst.compactionsCompleted));
        setLongField(env, sst, sstCls, "filesCompacted", static_cast<jlong>(stats.sst.filesCompacted));
        setLongField(env, sst, sstCls, "bytesCompactedIn", static_cast<jlong>(stats.sst.bytesCompactedIn));
        setLongField(env, sst, sstCls, "bytesCompactedOut", static_cast<jlong>(stats.sst.bytesCompactedOut));
        setLongField(env, sst, sstCls, "l0Stalls", static_cast<jlong>(stats.sst.l0Stalls));

        setLongField(env, out, cls, "currentSeq", static_cast<jlong>(stats.currentSeq));
        setLongField(env, out, cls, "nodeId", static_cast<jlong>(stats.nodeId));
        setLongField(env, out, cls, "putsTotal", static_cast<jlong>(stats.putsTotal));
        setLongField(env, out, cls, "removesTotal", static_cast<jlong>(stats.removesTotal));
        setLongField(env, out, cls, "getsTotal", static_cast<jlong>(stats.getsTotal));
        setLongField(env, out, cls, "getsMemtableHit", static_cast<jlong>(stats.getsMemtableHit));
        setLongField(env, out, cls, "getsSstHit", static_cast<jlong>(stats.getsSstHit));
        setLongField(env, out, cls, "getsMiss", static_cast<jlong>(stats.getsMiss));
        setLongField(env, out, cls, "existsTotal", static_cast<jlong>(stats.existsTotal));
        setLongField(env, out, cls, "scansTotal", static_cast<jlong>(stats.scansTotal));
        setLongField(env, out, cls, "blobPutsTotal", static_cast<jlong>(stats.blobPutsTotal));
        setObjectField(env, out, cls, "api", "Ldev/swiftstorm/akkaradb/engine/ApiStats;", api);
        setObjectField(env, out, cls, "memtable", "Ldev/swiftstorm/akkaradb/engine/MemTableStats;", memtable);
        setObjectField(env, out, cls, "wal", "Ldev/swiftstorm/akkaradb/engine/WalStats;", wal);
        setObjectField(env, out, cls, "blob", "Ldev/swiftstorm/akkaradb/engine/BlobStats;", blob);
        setObjectField(env, out, cls, "sst", "Ldev/swiftstorm/akkaradb/engine/SstStats;", sst);
        setObjectField(env, out, cls, "vlog", "Ldev/swiftstorm/akkaradb/engine/VLogStats;", vlog);
        return out;
    }

    [[nodiscard]] AkkaraDB::Options readOptions(JNIEnv* env, jobject optionsBuffer) {
        BytesReader in(readDirectBuffer(env, optionsBuffer));
        const auto version = in.u8();
        if (version != 1 && version != 2 && version != 3 && version != 4) { throw std::runtime_error("Unsupported AkkaraDB options payload version"); }

        AkkaraDB::Options options;
        options.mode = startupModeFromOrdinal(static_cast<jint>(in.i32()));
        options.dataDir = readUtf8String(in);

        const auto memtableThresholdPerShard = static_cast<jlong>(in.i64());
        if (memtableThresholdPerShard >= 0) {
            options.overrides.memtableThresholdPerShard = checkedCast<size_t>(memtableThresholdPerShard, "memtableThresholdPerShard");
        }

        assignOptionalBool(options.overrides.versionLogEnabled, static_cast<jint>(in.i32()), "versionLogEnabled");

        const auto sstCodec = static_cast<jint>(in.i32());
        if (sstCodec >= 0) { options.overrides.sstCodec = codecFromOrdinal(sstCodec); }

        const auto blobCodec = static_cast<jint>(in.i32());
        if (blobCodec >= 0) { options.overrides.blobCodec = codecFromOrdinal(blobCodec); }

        const auto blobThresholdBytes = static_cast<jlong>(in.i64());
        if (blobThresholdBytes >= 0) {
            options.overrides.blobThresholdBytes = checkedCast<uint64_t>(blobThresholdBytes, "blobThresholdBytes");
        }

        assignOptionalBool(options.overrides.sstPromoteReads, static_cast<jint>(in.i32()), "sstPromoteReads");

        const auto sstBloomBitsPerKey = static_cast<jlong>(in.i64());
        if (sstBloomBitsPerKey >= 0) {
            options.overrides.sstBloomBitsPerKey = checkedCast<size_t>(sstBloomBitsPerKey, "sstBloomBitsPerKey");
        }

        const auto maxL0SstFiles = static_cast<jlong>(in.i64());
        if (maxL0SstFiles >= 0) {
            options.overrides.maxL0SstFiles = checkedCast<size_t>(maxL0SstFiles, "maxL0SstFiles");
        }

        if (version >= 2) {
            const auto hasApi = static_cast<jint>(in.i32());
            if (hasApi != 0) {
                AkkaraDB::Options::ApiOptions api;
                const auto backendCount = static_cast<jint>(in.i32());
                if (backendCount < 0) { throw std::runtime_error("api.backends count must be >= 0"); }
                api.backends.reserve(static_cast<size_t>(backendCount));
                for (jint i = 0; i < backendCount; ++i) {
                    api.backends.push_back(apiBackendFromOrdinal(static_cast<jint>(in.i32())));
                }
                api.bindHost = readUtf8String(in);
                api.httpPort = checkedCast<uint16_t>(static_cast<jlong>(in.i32()), "api.httpPort");
                api.tcpPort = checkedCast<uint16_t>(static_cast<jlong>(in.i32()), "api.tcpPort");
                api.grpcPort = checkedCast<uint16_t>(static_cast<jlong>(in.i32()), "api.grpcPort");
                api.transportMode = apiTransportModeFromOrdinal(static_cast<jint>(in.i32()));
                if (const auto value = readOptionalUtf8String(in)) { api.serverBackendPath = *value; }
                if (const auto value = readOptionalUtf8String(in)) { api.transportBackendPath = *value; }
                if (const auto value = readOptionalUtf8String(in)) { api.httpBackendPath = *value; }
                if (const auto value = readOptionalUtf8String(in)) { api.tcpBackendPath = *value; }
                if (const auto value = readOptionalUtf8String(in)) { api.grpcBackendPath = *value; }
                if (version >= 3) {
                    api.httpMaxBatchItems = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.httpMaxBatchItems");
                    api.httpMaxScanItems = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.httpMaxScanItems");
                    api.httpMaxHistoryEntries = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.httpMaxHistoryEntries");
                    api.httpMaxContentLength = checkedCast<uint64_t>(static_cast<jlong>(in.i64()), "api.httpMaxContentLength");
                    api.grpcWorkerThreads = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcWorkerThreads");
                    api.grpcCompletionQueues = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcCompletionQueues");
                    api.grpcMinPollers = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcMinPollers");
                    api.grpcMaxPollers = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcMaxPollers");
                    api.grpcMaxConcurrentStreams = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcMaxConcurrentStreams");
                    api.grpcResourceQuotaBytes = checkedCast<uint64_t>(static_cast<jlong>(in.i64()), "api.grpcResourceQuotaBytes");
                    api.grpcMaxBatchItems = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcMaxBatchItems");
                    api.grpcMaxScanItems = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcMaxScanItems");
                    api.grpcMaxHistoryEntries = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcMaxHistoryEntries");
                    api.tcpIoBackend = apiIoBackendFromOrdinal(static_cast<jint>(in.i32()));
                    api.tcpWorkerThreads = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpWorkerThreads");
                    api.tcpAcceptQueueLimit = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpAcceptQueueLimit");
                    api.tcpAcceptQueueTimeoutMs = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpAcceptQueueTimeoutMs");
                    api.tcpListenBacklog = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpListenBacklog");
                    api.tcpRecvBufferBytes = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpRecvBufferBytes");
                    api.tcpSendBufferBytes = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpSendBufferBytes");
                    api.tcpPipelineBatchLimit = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpPipelineBatchLimit");
                    api.tcpMaxBatchItems = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpMaxBatchItems");
                    api.tcpMaxPendingResponseBytes = checkedCast<uint64_t>(static_cast<jlong>(in.i64()), "api.tcpMaxPendingResponseBytes");
                    api.tcpReadTimeoutMs = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpReadTimeoutMs");
                    api.tcpWriteTimeoutMs = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpWriteTimeoutMs");
                    api.tcpNoDelay = boolFromInt(static_cast<jint>(in.i32()), "api.tcpNoDelay");
                    api.tcpKeepAlive = boolFromInt(static_cast<jint>(in.i32()), "api.tcpKeepAlive");
                    if (const auto value = readOptionalUtf8String(in)) { api.tls.certPath = *value; }
                    if (const auto value = readOptionalUtf8String(in)) { api.tls.keyPath = *value; }
                    if (const auto value = readOptionalUtf8String(in)) { api.tls.caPath = *value; }
                    if (version >= 4) {
                        if (const auto value = readOptionalBytes(in)) { api.tls.psk = std::move(*value); }
                    }
                    if (const auto value = readOptionalUtf8String(in)) { api.tls.pskIdentity = *value; }
                    api.tls.verifyPeer = boolFromInt(static_cast<jint>(in.i32()), "api.tls.verifyPeer");
                }
                options.api = std::move(api);
            }
        }

        if (!in.eof()) { throw std::runtime_error("Trailing bytes in AkkaraDB options payload"); }
        if (options.dataDir.empty() && options.mode != StartupMode::ULTRA_FAST) {
            throw std::runtime_error("dataDir must not be blank unless mode is ULTRA_FAST");
        }
        return options;
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
        jobject optionsBuffer
    ) {
        return guard(
            env,
            [&]() -> jlong {
                auto options = readOptions(env, optionsBuffer);
                auto db = AkkaraDB::open(std::move(options));
                return toHandle(db.release());
            }
        );
    }

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativeOpenTable(
        JNIEnv* env,
        jobject,
        jlong engineHandle,
        jobject tablePrefix,
        jobject schemaBytes
    ) {
        return guard(
            env,
            [&]() -> jlong {
                auto* db = dbFrom(engineHandle);
                if (db == nullptr) { throw std::runtime_error("JNIPackedTable engine handle is null"); }
                auto table = std::make_unique<NativeTable>();
                table->db = db;
                const auto prefix = readDirectBuffer(env, tablePrefix);
                table->tablePrefix.assign(prefix.begin(), prefix.end());
                table->schema = parseRootSchema(readDirectBuffer(env, schemaBytes));
                return toHandle(table.release());
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativeRegisterIndex(
        JNIEnv* env,
        jobject,
        jlong tableHandle,
        jstring fieldName,
        jobject indexPrefix
    ) {
        guard(
            env,
            [&]() {
                auto* table = tableFrom(tableHandle);
                if (table == nullptr) { throw std::runtime_error("JNIPackedTable handle is null"); }
                const std::string name = readJavaString(env, fieldName);
                for (auto& index : table->indexes) {
                    if (index.fieldName == name) { return 0; }
                }
                NativeIndexDef index;
                index.fieldName = name;
                const auto prefix = readDirectBuffer(env, indexPrefix);
                index.prefix.assign(prefix.begin(), prefix.end());
                index.column = resolveColumn(table->schema, name);
                table->indexes.push_back(std::move(index));
                return 0;
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativePutIndexed(
        JNIEnv* env,
        jobject,
        jlong tableHandle,
        jobject pkBytes,
        jobject value
    ) {
        guard(
            env,
            [&]() {
                auto* table = tableFrom(tableHandle);
                if (table == nullptr) { throw std::runtime_error("JNIPackedTable handle is null"); }
                const auto pk = readDirectBuffer(env, pkBytes);
                const auto rowValue = readDirectBuffer(env, value);
                const auto pkKey = concatBytes(table->tablePrefix, pk);
                auto oldValue = table->db->engine().get(pkKey);
                if (oldValue) { updateIndexEntries(*table, pk, *oldValue, true); }
                table->db->engine().put(pkKey, rowValue);
                updateIndexEntries(*table, pk, rowValue, false);
                return 0;
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativeRemoveIndexed(
        JNIEnv* env,
        jobject,
        jlong tableHandle,
        jobject pkBytes
    ) {
        guard(
            env,
            [&]() {
                auto* table = tableFrom(tableHandle);
                if (table == nullptr) { throw std::runtime_error("JNIPackedTable handle is null"); }
                const auto pk = readDirectBuffer(env, pkBytes);
                const auto pkKey = concatBytes(table->tablePrefix, pk);
                auto oldValue = table->db->engine().get(pkKey);
                if (oldValue) { updateIndexEntries(*table, pk, *oldValue, true); }
                table->db->engine().remove(pkKey);
                return 0;
            }
        );
    }

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativeOpenIndexScan(
        JNIEnv* env,
        jobject,
        jlong tableHandle,
        jstring fieldName,
        jobject fieldBytes
    ) {
        return guard(
            env,
            [&]() -> jlong {
                auto* table = tableFrom(tableHandle);
                if (table == nullptr) { throw std::runtime_error("JNIPackedTable handle is null"); }
                const auto& index = requireIndex(*table, readJavaString(env, fieldName));
                const auto field = readDirectBuffer(env, fieldBytes);
                std::vector<uint8_t> startKey;
                buildIndexSearchPrefix(index.prefix, field, startKey);
                std::vector<uint8_t> endKey = startKey;
                if (!incrementLexicographic(endKey)) { endKey.clear(); }

                auto cursor = std::make_unique<ScanCursor>();
                cursor->indexLookup = std::make_unique<ScanCursor::IndexLookup>();
                cursor->indexLookup->db = table->db;
                cursor->indexLookup->tablePrefix = table->tablePrefix;
                cursor->indexLookup->searchPrefixSize = startKey.size();
                cursor->rows = table->db->engine().scan(cursor->arena, startKey, endKey);
                cursor->it = cursor->rows.begin();
                return toHandle(cursor.release());
            }
        );
    }

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativeOpenQueryIndexScan(
        JNIEnv* env,
        jobject,
        jlong tableHandle,
        jstring fieldName,
        jobject fieldBytes,
        jobject queryBytes,
        jobject schemaBytes
    ) {
        return guard(
            env,
            [&]() -> jlong {
                auto* table = tableFrom(tableHandle);
                if (table == nullptr) { throw std::runtime_error("JNIPackedTable handle is null"); }
                const auto& index = requireIndex(*table, readJavaString(env, fieldName));
                const auto field = readDirectBuffer(env, fieldBytes);
                std::vector<uint8_t> startKey;
                buildIndexSearchPrefix(index.prefix, field, startKey);
                std::vector<uint8_t> endKey = startKey;
                if (!incrementLexicographic(endKey)) { endKey.clear(); }

                auto cursor = std::make_unique<ScanCursor>();
                cursor->indexLookup = std::make_unique<ScanCursor::IndexLookup>();
                cursor->indexLookup->db = table->db;
                cursor->indexLookup->tablePrefix = table->tablePrefix;
                cursor->indexLookup->searchPrefixSize = startKey.size();
                cursor->query = std::make_unique<QueryProgram>(
                    parseQuery(readDirectBuffer(env, queryBytes), readDirectBuffer(env, schemaBytes))
                );
                cursor->rows = table->db->engine().scan(cursor->arena, startKey, endKey);
                cursor->it = cursor->rows.begin();
                return toHandle(cursor.release());
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativeCloseTable(
        JNIEnv* env,
        jobject,
        jlong tableHandle
    ) {
        guard(
            env,
            [&]() {
                std::unique_ptr<NativeTable> table{tableFrom(tableHandle)};
                return 0;
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

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativePutHinted(
        JNIEnv* env,
        jobject,
        jlong handle,
        jobject key,
        jobject value,
        jlong fp64,
        jlong miniKey
    ) {
        guard(
            env,
            [&]() {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                db->engine().putHinted(
                    readDirectBuffer(env, key),
                    readDirectBuffer(env, value),
                    static_cast<uint64_t>(fp64),
                    static_cast<uint64_t>(miniKey)
                );
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

    JNIEXPORT jobjectArray JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeGetBatch(
        JNIEnv* env,
        jobject,
        jlong handle,
        jobjectArray keys
    ) {
        return guard(
            env,
            [&]() -> jobjectArray {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                std::vector<std::span<const uint8_t>> keySpans;
                readDirectBufferArray(env, keys, keySpans);
                const auto results = db->engine().getBatch(keySpans);
                const auto& ids = bindings(env);
                jobjectArray out = env->NewObjectArray(
                    static_cast<jsize>(results.size()),
                    ids.batchGetResultClass,
                    nullptr
                );
                if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
                for (jsize i = 0; i < static_cast<jsize>(results.size()); ++i) {
                    jobject result = makeBatchGetResult(env, results[static_cast<size_t>(i)]);
                    if (result == nullptr || env->ExceptionCheck()) { return nullptr; }
                    env->SetObjectArrayElement(out, i, result);
                    if (env->ExceptionCheck()) { return nullptr; }
                    env->DeleteLocalRef(result);
                }
                return out;
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativePutBatch(
        JNIEnv* env,
        jobject,
        jlong handle,
        jobjectArray keys,
        jobjectArray values
    ) {
        guard(
            env,
            [&]() {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const jsize keyCount = env->GetArrayLength(keys);
                const jsize valueCount = env->GetArrayLength(values);
                if (keyCount != valueCount) { throw std::runtime_error("putBatch keys and values length mismatch"); }
                std::vector<std::span<const uint8_t>> keySpans;
                std::vector<std::span<const uint8_t>> valueSpans;
                readDirectBufferArray(env, keys, keySpans);
                readDirectBufferArray(env, values, valueSpans);
                std::vector<AkkEngine::BatchPutEntry> entries;
                entries.reserve(static_cast<size_t>(keyCount));
                for (jsize i = 0; i < keyCount; ++i) {
                    entries.push_back({keySpans[static_cast<size_t>(i)], valueSpans[static_cast<size_t>(i)]});
                }
                db->engine().putBatch(entries);
                return 0;
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

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeRemoveHinted(
        JNIEnv* env,
        jobject,
        jlong handle,
        jobject key,
        jlong fp64,
        jlong miniKey
    ) {
        guard(
            env,
            [&]() {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                db->engine().removeHinted(
                    readDirectBuffer(env, key),
                    static_cast<uint64_t>(fp64),
                    static_cast<uint64_t>(miniKey)
                );
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
                cursor->query = std::make_unique<QueryProgram>(parseQuery(query, schema));
                cursor->rows = db->engine().scan(cursor->arena, start, end);
                cursor->it = cursor->rows.begin();
                return toHandle(cursor.release());
            }
        );
    }

    JNIEXPORT jobject JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeGetAt(
        JNIEnv* env,
        jobject,
        jlong handle,
        jobject key,
        jlong targetSeq
    ) {
        return guard(
            env,
            [&]() -> jobject {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto k = readDirectBuffer(env, key);
                auto value = db->engine().getAt(k, static_cast<uint64_t>(targetSeq));
                if (!value) { return nullptr; }
                return makeDirectBuffer(env, *value);
            }
        );
    }

    JNIEXPORT jobjectArray JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeHistory(
        JNIEnv* env,
        jobject,
        jlong handle,
        jobject key
    ) {
        return guard(
            env,
            [&]() -> jobjectArray {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto k = readDirectBuffer(env, key);
                const auto history = db->engine().history(k);
                const auto& ids = bindings(env);
                if (history.size() > static_cast<size_t>(std::numeric_limits<jsize>::max())) {
                    throw std::runtime_error("AkkaraDB JNI history result is too large");
                }
                jobjectArray out = env->NewObjectArray(
                    static_cast<jsize>(history.size()),
                    ids.versionEntryClass,
                    nullptr
                );
                if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
                for (jsize i = 0; i < static_cast<jsize>(history.size()); ++i) {
                    jobject entry = makeVersionEntry(env, history[static_cast<size_t>(i)]);
                    if (entry == nullptr || env->ExceptionCheck()) { return nullptr; }
                    env->SetObjectArrayElement(out, i, entry);
                    if (env->ExceptionCheck()) { return nullptr; }
                    env->DeleteLocalRef(entry);
                }
                return out;
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

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeRollbackKey(
        JNIEnv* env,
        jobject,
        jlong handle,
        jobject key,
        jlong targetSeq
    ) {
        guard(
            env,
            [&]() {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                db->engine().rollbackKey(readDirectBuffer(env, key), static_cast<uint64_t>(targetSeq));
                return 0;
            }
        );
    }

    JNIEXPORT jobject JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeStats(JNIEnv* env, jobject, jlong handle) {
        return guard(
            env,
            [&]() -> jobject {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                return makeEngineStats(env, db->engine().stats());
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeForceSync(JNIEnv* env, jobject, jlong handle) {
        guard(
            env,
            [&]() {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                db->engine().forceSync();
                return 0;
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeForceFlush(JNIEnv* env, jobject, jlong handle) {
        guard(
            env,
            [&]() {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                db->engine().forceFlush();
                return 0;
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeRunBlobGc(JNIEnv* env, jobject, jlong handle) {
        guard(
            env,
            [&]() {
                auto* db = dbFrom(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                db->engine().runBlobGc();
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
                    if (cursor->indexLookup != nullptr) {
                        const auto& state = *cursor->indexLookup;
                        const auto key = row.key;
                        ++cursor->it;
                        if (key.size() <= state.searchPrefixSize) { continue; }
                        const std::span<const uint8_t> pkBytes{key.data() + state.searchPrefixSize, key.size() - state.searchPrefixSize};
                        const auto pkKey = concatBytes(state.tablePrefix, pkBytes);
                        auto value = state.db->engine().get(pkKey);
                        if (!value) { continue; }
                        if (cursor->query != nullptr && !matchesQuery(*cursor->query, *value)) { continue; }
                        return makeRow(env, pkKey, *value);
                    }
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
