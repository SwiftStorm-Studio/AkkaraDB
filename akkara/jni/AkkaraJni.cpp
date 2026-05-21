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

namespace {
    using akkaradb::AkkaraDB;
    using akkaradb::Codec;
    using akkaradb::StartupMode;
    using akkaradb::core::ArenaGenerator;
    using akkaradb::core::BufferArena;
    using akkaradb::engine::AkkEngine;

    [[nodiscard]] AkkaraDB* db_from(jlong handle) noexcept { return reinterpret_cast<AkkaraDB*>(static_cast<std::uintptr_t>(handle)); }

    [[nodiscard]] jlong to_handle(void* ptr) noexcept { return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(ptr)); }

    void throw_java(JNIEnv* env, const char* class_name, const char* message) {
        jclass cls = env->FindClass(class_name);
        if (cls != nullptr) { env->ThrowNew(cls, message); }
    }

    void throw_runtime(JNIEnv* env, const std::exception& ex) { throw_java(env, "java/lang/RuntimeException", ex.what()); }

    [[nodiscard]] std::vector<uint8_t> read_bytes(JNIEnv* env, jbyteArray array) {
        if (array == nullptr) { return {}; }
        const jsize len = env->GetArrayLength(array);
        std::vector<uint8_t> out(static_cast<size_t>(len));
        if (len > 0) { env->GetByteArrayRegion(array, 0, len, reinterpret_cast<jbyte*>(out.data())); }
        return out;
    }

    [[nodiscard]] jbyteArray make_bytes(JNIEnv* env, std::span<const uint8_t> bytes) {
        auto out = env->NewByteArray(static_cast<jsize>(bytes.size()));
        if (out != nullptr && !bytes.empty()) {
            env->SetByteArrayRegion(out, 0, static_cast<jsize>(bytes.size()), reinterpret_cast<const jbyte*>(bytes.data()));
        }
        return out;
    }

    [[nodiscard]] std::string read_string(JNIEnv* env, jstring value) {
        if (value == nullptr) { return {}; }
        const char* raw = env->GetStringUTFChars(value, nullptr);
        if (raw == nullptr) { return {}; }
        std::string out{raw};
        env->ReleaseStringUTFChars(value, raw);
        return out;
    }

    [[nodiscard]] StartupMode startup_mode_from_ordinal(jint value) {
        switch (value) {
            case 0: return StartupMode::ULTRA_FAST;
            case 1: return StartupMode::FAST;
            case 2: return StartupMode::NORMAL;
            case 3: return StartupMode::DURABLE;
            default: throw std::runtime_error("Invalid AkkaraDB StartupMode ordinal");
        }
    }

    [[nodiscard]] Codec codec_from_ordinal(jint value) {
        switch (value) {
            case 0: return Codec::None;
            case 1: return Codec::Zstd;
            default: throw std::runtime_error("Invalid AkkaraDB Codec ordinal");
        }
    }

    template <typename T>
    void assign_optional_non_negative(std::optional<T>& target, jlong value) {
        if (value < 0) { return; }
        target = static_cast<T>(value);
    }

    void assign_optional_bool(std::optional<bool>& target, jint value, const char* name) {
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
                const uint16_t out = static_cast<uint16_t>(bytes_[pos_]) | static_cast<uint16_t>(static_cast<uint16_t>(bytes_[pos_ + 1]) << 8);
                pos_ += 2;
                return out;
            }

            [[nodiscard]] uint32_t u32() {
                ensure(4);
                const uint32_t out = static_cast<uint32_t>(bytes_[pos_]) | (static_cast<uint32_t>(bytes_[pos_ + 1]) << 8) | (static_cast<uint32_t>(
                    bytes_[pos_ + 2]) << 16) | (static_cast<uint32_t>(bytes_[pos_ + 3]) << 24);
                pos_ += 4;
                return out;
            }

            [[nodiscard]] uint64_t u64() {
                const uint64_t lo = u32();
                const uint64_t hi = u32();
                return lo | (hi << 32);
            }

            [[nodiscard]] std::string str_u16() {
                const auto len = static_cast<size_t>(u16());
                ensure(len);
                std::string out(reinterpret_cast<const char*>(bytes_.data() + pos_), len);
                pos_ += len;
                return out;
            }

            [[nodiscard]] std::string str_i32() {
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
            void ensure(size_t len) const { if (len > bytes_.size() - pos_) { throw std::runtime_error("Truncated AkkaraDB query payload"); } }

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
            Bool = 0x01,
            Int8 = 0x02,
            Int16 = 0x03,
            Int32 = 0x04,
            Int64 = 0x05,
            Float = 0x06,
            Double = 0x07,
            String = 0x08,
            List = 0x0A,
            Map = 0x0B,
            Struct = 0x0C,
            Nullable = 0x0D, };

        Kind kind;
        std::vector<FieldDesc> fields;
        TypePtr elem;
        TypePtr key;
        TypePtr value;
    };

    [[nodiscard]] TypePtr parse_type(BytesReader& in);

    void parse_struct_fields(BytesReader& in, TypeDesc& out) {
        const auto count = in.u8();
        out.fields.reserve(count);
        for (uint8_t i = 0; i < count; ++i) { out.fields.push_back(FieldDesc{in.str_u16(), parse_type(in)}); }
    }

    [[nodiscard]] TypePtr parse_type(BytesReader& in) {
        auto out = std::make_shared<TypeDesc>();
        out->kind = static_cast<TypeDesc::Kind>(in.u8());
        switch (out->kind) {
            case TypeDesc::Bool:
            case TypeDesc::Int8:
            case TypeDesc::Int16:
            case TypeDesc::Int32:
            case TypeDesc::Int64:
            case TypeDesc::Float:
            case TypeDesc::Double:
            case TypeDesc::String: break;
            case TypeDesc::List: out->elem = parse_type(in);
                break;
            case TypeDesc::Map: out->key = parse_type(in);
                out->value = parse_type(in);
                break;
            case TypeDesc::Struct: parse_struct_fields(in, *out);
                break;
            case TypeDesc::Nullable: out->elem = parse_type(in);
                break;
            default: throw std::runtime_error("Unknown AkkaraDB schema kind");
        }
        return out;
    }

    [[nodiscard]] TypeDesc parse_root_schema(std::span<const uint8_t> schema_bytes) {
        BytesReader in(schema_bytes);
        TypeDesc root{TypeDesc::Struct};
        parse_struct_fields(in, root);
        if (!in.eof()) { throw std::runtime_error("Trailing bytes in AkkaraDB schema payload"); }
        return root;
    }

    struct Value {
        enum Kind {
            Missing, Null, Bool, Int, Double, String, List, Map
        } kind = Missing;

        bool b = false;
        int64_t i = 0;
        double d = 0.0;
        std::string s;
        std::vector<Value> list;
        std::vector<std::pair<Value, Value>> map;

        [[nodiscard]] static Value missing() { return {}; }

        [[nodiscard]] static Value null() {
            Value v;
            v.kind = Null;
            return v;
        }

        [[nodiscard]] static Value boolean(bool value) {
            Value v;
            v.kind = Bool;
            v.b = value;
            return v;
        }

        [[nodiscard]] static Value integer(int64_t value) {
            Value v;
            v.kind = Int;
            v.i = value;
            return v;
        }

        [[nodiscard]] static Value floating(double value) {
            Value v;
            v.kind = Double;
            v.d = value;
            return v;
        }

        [[nodiscard]] static Value string(std::string value) {
            Value v;
            v.kind = String;
            v.s = std::move(value);
            return v;
        }

        [[nodiscard]] static Value list_value(std::vector<Value> values) {
            Value v;
            v.kind = List;
            v.list = std::move(values);
            return v;
        }

        [[nodiscard]] static Value map_value(std::vector<std::pair<Value, Value>> values) {
            Value v;
            v.kind = Map;
            v.map = std::move(values);
            return v;
        }

        [[nodiscard]] bool is_numeric() const noexcept { return kind == Int || kind == Double; }
        [[nodiscard]] double as_double() const noexcept { return kind == Int ? static_cast<double>(i) : d; }
    };

    [[nodiscard]] Value read_binpack_value(BytesReader& in, const TypeDesc& type);

    void skip_binpack_value(BytesReader& in, const TypeDesc& type) {
        switch (type.kind) {
            case TypeDesc::Bool:
            case TypeDesc::Int8: in.skip(1);
                break;
            case TypeDesc::Int16: in.skip(2);
                break;
            case TypeDesc::Int32:
            case TypeDesc::Float: in.skip(4);
                break;
            case TypeDesc::Int64:
            case TypeDesc::Double: in.skip(8);
                break;
            case TypeDesc::String: in.skip(static_cast<size_t>(in.u32()));
                break;
            case TypeDesc::Nullable: if (in.u8() != 0) { skip_binpack_value(in, *type.elem); }
                break;
            case TypeDesc::Struct: for (const auto& field : type.fields) { skip_binpack_value(in, *field.type); }
                break;
            case TypeDesc::List: {
                const auto count = in.u32();
                for (uint32_t i = 0; i < count; ++i) { skip_binpack_value(in, *type.elem); }
                break;
            }
            case TypeDesc::Map: {
                const auto count = in.u32();
                for (uint32_t i = 0; i < count; ++i) {
                    skip_binpack_value(in, *type.key);
                    skip_binpack_value(in, *type.value);
                }
                break;
            }
            default: throw std::runtime_error("Unknown AkkaraDB schema kind while skipping value");
        }
    }

    [[nodiscard]] float bits_to_float(uint32_t bits) {
        float out;
        std::memcpy(&out, &bits, sizeof(out));
        return out;
    }

    [[nodiscard]] double bits_to_double(uint64_t bits) {
        double out;
        std::memcpy(&out, &bits, sizeof(out));
        return out;
    }

    [[nodiscard]] Value read_binpack_value(BytesReader& in, const TypeDesc& type) {
        switch (type.kind) {
            case TypeDesc::Bool: return Value::boolean(in.u8() != 0);
            case TypeDesc::Int8: return Value::integer(static_cast<int8_t>(in.u8()));
            case TypeDesc::Int16: return Value::integer(static_cast<int16_t>(in.u16()));
            case TypeDesc::Int32: return Value::integer(static_cast<int32_t>(in.u32()));
            case TypeDesc::Int64: return Value::integer(static_cast<int64_t>(in.u64()));
            case TypeDesc::Float: return Value::floating(static_cast<double>(bits_to_float(in.u32())));
            case TypeDesc::Double: return Value::floating(bits_to_double(in.u64()));
            case TypeDesc::String: return Value::string(in.str_i32());
            case TypeDesc::Nullable: return in.u8() == 0 ? Value::null() : read_binpack_value(in, *type.elem);
            case TypeDesc::List: {
                const auto count = in.u32();
                std::vector<Value> values;
                values.reserve(count);
                for (uint32_t i = 0; i < count; ++i) { values.push_back(read_binpack_value(in, *type.elem)); }
                return Value::list_value(std::move(values));
            }
            case TypeDesc::Map: {
                const auto count = in.u32();
                std::vector<std::pair<Value, Value>> values;
                values.reserve(count);
                for (uint32_t i = 0; i < count; ++i) {
                    Value key = read_binpack_value(in, *type.key);
                    Value value = read_binpack_value(in, *type.value);
                    values.emplace_back(std::move(key), std::move(value));
                }
                return Value::map_value(std::move(values));
            }
            case TypeDesc::Struct: skip_binpack_value(in, type);
                return Value::missing();
            default: throw std::runtime_error("Unknown AkkaraDB schema kind while reading value");
        }
    }

    [[nodiscard]] std::vector<std::string> split_column_path(const std::string& path) {
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

    [[nodiscard]] Value extract_from_struct(BytesReader& in, const TypeDesc& type, const std::vector<std::string>& path, size_t part);

    [[nodiscard]] Value extract_nested(BytesReader& in, const TypeDesc& type, const std::vector<std::string>& path, size_t part) {
        if (type.kind == TypeDesc::Nullable) {
            if (in.u8() == 0) { return Value::null(); }
            return extract_nested(in, *type.elem, path, part);
        }
        if (type.kind != TypeDesc::Struct) { throw std::runtime_error("AkkaraDB query column path descends into a non-struct field"); }
        return extract_from_struct(in, type, path, part);
    }

    [[nodiscard]] Value extract_from_struct(BytesReader& in, const TypeDesc& type, const std::vector<std::string>& path, size_t part) {
        if (type.kind != TypeDesc::Struct) { throw std::runtime_error("AkkaraDB query root schema is not a struct"); }
        for (const auto& field : type.fields) {
            if (field.name == path[part]) {
                if (part + 1 == path.size()) { return read_binpack_value(in, *field.type); }
                return extract_nested(in, *field.type, path, part + 1);
            }
            skip_binpack_value(in, *field.type);
        }
        throw std::runtime_error("AkkaraDB query column not found in schema: " + path[part]);
    }

    [[nodiscard]] Value read_column(std::span<const uint8_t> row_value, const TypeDesc& schema, const std::string& name) {
        const auto path = split_column_path(name);
        if (path.empty() || path[0].empty()) { throw std::runtime_error("AkkaraDB query column name is empty"); }
        BytesReader in(row_value);
        return extract_from_struct(in, schema, path, 0);
    }

    struct Expr {
        enum Tag : uint8_t {
            Bin = 0x01, Un = 0x02, Lit = 0x03, Col = 0x04, Cap = 0x05
        };

        Tag tag = Lit;
        uint8_t op = 0;
        Value literal;
        std::string column;
        uint32_t capture = 0;
        std::unique_ptr<Expr> lhs;
        std::unique_ptr<Expr> rhs;
    };

    struct QueryProgram {
        std::vector<Value> captures;
        TypeDesc schema{TypeDesc::Struct};
        Expr where;
    };

    [[nodiscard]] Value parse_literal(BytesReader& in) {
        switch (in.u8()) {
            case 0x01: return Value::boolean(in.u8() != 0);
            case 0x02: return Value::integer(static_cast<int8_t>(in.u8()));
            case 0x03: return Value::integer(static_cast<int16_t>(in.u16()));
            case 0x04: return Value::integer(static_cast<int32_t>(in.u32()));
            case 0x05: return Value::integer(static_cast<int64_t>(in.u64()));
            case 0x06: return Value::floating(static_cast<double>(bits_to_float(in.u32())));
            case 0x07: return Value::floating(bits_to_double(in.u64()));
            case 0x08: return Value::string(in.str_i32());
            case 0x09: return Value::null();
            case 0x0A: {
                const auto count = in.u32();
                std::vector<Value> values;
                values.reserve(count);
                for (uint32_t i = 0; i < count; ++i) { values.push_back(parse_literal(in)); }
                return Value::list_value(std::move(values));
            }
            case 0x0B: {
                const auto count = in.u32();
                std::vector<std::pair<Value, Value>> values;
                values.reserve(count);
                for (uint32_t i = 0; i < count; ++i) {
                    Value key = parse_literal(in);
                    Value value = parse_literal(in);
                    values.emplace_back(std::move(key), std::move(value));
                }
                return Value::map_value(std::move(values));
            }
            default: throw std::runtime_error("Unknown AkkaraDB query literal tag");
        }
    }

    [[nodiscard]] Expr parse_expr(BytesReader& in) {
        Expr expr;
        expr.tag = static_cast<Expr::Tag>(in.u8());
        switch (expr.tag) {
            case Expr::Bin: expr.op = in.u8();
                expr.lhs = std::make_unique<Expr>(parse_expr(in));
                expr.rhs = std::make_unique<Expr>(parse_expr(in));
                break;
            case Expr::Un: expr.op = in.u8();
                expr.lhs = std::make_unique<Expr>(parse_expr(in));
                break;
            case Expr::Lit: expr.literal = parse_literal(in);
                break;
            case Expr::Col: expr.column = in.str_u16();
                break;
            case Expr::Cap: expr.capture = in.u32();
                break;
            default: throw std::runtime_error("Unknown AkkaraDB query expression tag");
        }
        return expr;
    }

    [[nodiscard]] QueryProgram parse_query(std::span<const uint8_t> query_bytes, std::span<const uint8_t> schema_bytes) {
        BytesReader in(query_bytes);
        QueryProgram program;
        const auto capture_count = in.u32();
        if (capture_count > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) { throw std::runtime_error("Too many AkkaraDB query captures"); }
        program.captures.reserve(static_cast<size_t>(capture_count));
        for (uint32_t i = 0; i < capture_count; ++i) { program.captures.push_back(parse_literal(in)); }
        program.where = parse_expr(in);
        if (!in.eof()) { throw std::runtime_error("Trailing bytes in AkkaraDB query payload"); }
        program.schema = parse_root_schema(schema_bytes);
        return program;
    }

    [[nodiscard]] bool value_is_true(const Value& value) noexcept { return value.kind == Value::Bool && value.b; }

    [[nodiscard]] int compare_values(const Value& lhs, const Value& rhs) {
        if (lhs.is_numeric() && rhs.is_numeric()) {
            const double a = lhs.as_double();
            const double b = rhs.as_double();
            return (a > b) - (a < b);
        }
        if (lhs.kind == Value::String && rhs.kind == Value::String) { return (lhs.s > rhs.s) - (lhs.s < rhs.s); }
        if (lhs.kind == Value::Bool && rhs.kind == Value::Bool) { return (lhs.b > rhs.b) - (lhs.b < rhs.b); }
        throw std::runtime_error("AkkaraDB query comparison between incompatible values");
    }

    [[nodiscard]] bool values_equal(const Value& lhs, const Value& rhs) {
        if (lhs.kind == Value::Null || rhs.kind == Value::Null) { return lhs.kind == rhs.kind; }
        if (lhs.kind == Value::Missing || rhs.kind == Value::Missing) { return false; }
        if (lhs.is_numeric() && rhs.is_numeric()) { return compare_values(lhs, rhs) == 0; }
        if (lhs.kind != rhs.kind) { return false; }
        switch (lhs.kind) {
            case Value::Bool: return lhs.b == rhs.b;
            case Value::Int: return lhs.i == rhs.i;
            case Value::Double: return lhs.d == rhs.d;
            case Value::String: return lhs.s == rhs.s;
            case Value::List:
                if (lhs.list.size() != rhs.list.size()) { return false; }
                for (size_t i = 0; i < lhs.list.size(); ++i) {
                    if (!values_equal(lhs.list[i], rhs.list[i])) { return false; }
                }
                return true;
            case Value::Map:
                if (lhs.map.size() != rhs.map.size()) { return false; }
                for (const auto& [lhs_key, lhs_value] : lhs.map) {
                    bool found = false;
                    for (const auto& [rhs_key, rhs_value] : rhs.map) {
                        if (values_equal(lhs_key, rhs_key) && values_equal(lhs_value, rhs_value)) {
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

    [[nodiscard]] bool value_in_list(const Value& needle, const Value& haystack) {
        if (haystack.kind != Value::List) { return false; }
        for (const Value& value : haystack.list) {
            if (values_equal(needle, value)) { return true; }
        }
        return false;
    }

    [[nodiscard]] Value map_get_value(const Value& map, const Value& key) {
        if (map.kind != Value::Map) { return Value::null(); }
        for (const auto& [candidate_key, value] : map.map) {
            if (values_equal(candidate_key, key)) { return value; }
        }
        return Value::null();
    }

    [[nodiscard]] bool string_starts_with(const Value& value, const Value& prefix) {
        return value.kind == Value::String && prefix.kind == Value::String && value.s.starts_with(prefix.s);
    }

    [[nodiscard]] bool string_contains(const Value& value, const Value& needle) {
        return value.kind == Value::String && needle.kind == Value::String && value.s.find(needle.s) != std::string::npos;
    }

    [[nodiscard]] bool like_match(std::string_view value, size_t vi, std::string_view pattern, size_t pi) {
        while (pi < pattern.size()) {
            if (pattern[pi] == '%') {
                while (pi + 1 < pattern.size() && pattern[pi + 1] == '%') { ++pi; }
                if (pi + 1 == pattern.size()) { return true; }
                for (size_t next = vi; next <= value.size(); ++next) {
                    if (like_match(value, next, pattern, pi + 1)) { return true; }
                }
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

    [[nodiscard]] bool string_like(const Value& value, const Value& pattern) {
        return value.kind == Value::String && pattern.kind == Value::String && like_match(value.s, 0, pattern.s, 0);
    }

    [[nodiscard]] Value eval_expr(const QueryProgram& program, const Expr& expr, std::span<const uint8_t> row_value) {
        switch (expr.tag) {
            case Expr::Lit: return expr.literal;
            case Expr::Cap: if (expr.capture >= program.captures.size()) { throw std::runtime_error("AkkaraDB query capture index out of range"); }
                return program.captures[expr.capture];
            case Expr::Col: return read_column(row_value, program.schema, expr.column);
            case Expr::Un: {
                const Value x = eval_expr(program, *expr.lhs, row_value);
                switch (expr.op) {
                    case 9: return Value::boolean(!value_is_true(x));
                    case 12: return Value::boolean(x.kind == Value::Null);
                    case 13: return Value::boolean(x.kind != Value::Null);
                    default: throw std::runtime_error("Unsupported AkkaraDB unary query operator");
                }
            }
            case Expr::Bin: {
                if (expr.op == 7) {
                    const Value lhs = eval_expr(program, *expr.lhs, row_value);
                    return Value::boolean(value_is_true(lhs) && value_is_true(eval_expr(program, *expr.rhs, row_value)));
                }
                if (expr.op == 8) {
                    const Value lhs = eval_expr(program, *expr.lhs, row_value);
                    return Value::boolean(value_is_true(lhs) || value_is_true(eval_expr(program, *expr.rhs, row_value)));
                }

                const Value lhs = eval_expr(program, *expr.lhs, row_value);
                const Value rhs = eval_expr(program, *expr.rhs, row_value);
                switch (expr.op) {
                    case 1: return Value::boolean(compare_values(lhs, rhs) > 0);
                    case 2: return Value::boolean(compare_values(lhs, rhs) >= 0);
                    case 3: return Value::boolean(compare_values(lhs, rhs) < 0);
                    case 4: return Value::boolean(compare_values(lhs, rhs) <= 0);
                    case 5: return Value::boolean(values_equal(lhs, rhs));
                    case 6: return Value::boolean(!values_equal(lhs, rhs));
                    case 9: return Value::boolean(!value_is_true(lhs));
                    case 10: return Value::boolean(value_in_list(lhs, rhs));
                    case 11: return Value::boolean(!value_in_list(lhs, rhs));
                    case 12: return Value::boolean(lhs.kind == Value::Null);
                    case 13: return Value::boolean(lhs.kind != Value::Null);
                    case 14: return map_get_value(lhs, rhs);
                    case 15: return Value::boolean(string_starts_with(lhs, rhs));
                    case 16: return Value::boolean(string_contains(lhs, rhs));
                    case 17: return Value::boolean(string_like(lhs, rhs));
                    default: throw std::runtime_error("Unsupported AkkaraDB binary query operator");
                }
            }
            default: throw std::runtime_error("Unknown AkkaraDB query expression tag");
        }
    }

    [[nodiscard]] bool matches_query(const QueryProgram& program, std::span<const uint8_t> row_value) {
        return value_is_true(eval_expr(program, program.where, row_value));
    }

    struct ScanCursor {
        AkkaraDB* db = nullptr;
        BufferArena arena;
        ArenaGenerator<AkkEngine::ScanRecordView> rows;
        ArenaGenerator<AkkEngine::ScanRecordView>::iterator it;
        std::unique_ptr<QueryProgram> query;
    };

    [[nodiscard]] ScanCursor* cursor_from(jlong handle) noexcept { return reinterpret_cast<ScanCursor*>(static_cast<std::uintptr_t>(handle)); }

    [[nodiscard]] jobject make_row(JNIEnv* env, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        jclass byte_buffer_cls = env->FindClass("java/nio/ByteBuffer");
        if (byte_buffer_cls == nullptr) { return nullptr; }
        jmethodID wrap = env->GetStaticMethodID(byte_buffer_cls, "wrap", "([B)Ljava/nio/ByteBuffer;");
        if (wrap == nullptr) { return nullptr; }

        jclass row_cls = env->FindClass("dev/swiftstorm/akkaradb/engine/RowView");
        if (row_cls == nullptr) { return nullptr; }
        jmethodID ctor = env->GetMethodID(row_cls, "<init>", "(Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Lkotlin/jvm/internal/DefaultConstructorMarker;)V");
        if (ctor == nullptr) { return nullptr; }
        jbyteArray key_array = make_bytes(env, key);
        if (key_array == nullptr) { return nullptr; }
        jbyteArray value_array = make_bytes(env, value);
        if (value_array == nullptr) { return nullptr; }
        jobject key_buffer = env->CallStaticObjectMethod(byte_buffer_cls, wrap, key_array);
        jobject value_buffer = env->CallStaticObjectMethod(byte_buffer_cls, wrap, value_array);
        return env->NewObject(row_cls, ctor, key_buffer, value_buffer, nullptr);
    }

    template <typename F>
    auto guard(JNIEnv* env, F&& f) -> decltype(f()) {
        try { return f(); }
        catch (const std::exception& ex) {
            throw_runtime(env, ex);
            using R = decltype(f());
            if constexpr (std::is_pointer_v<R>) { return nullptr; }
            else if constexpr (std::is_same_v<R, jboolean>) { return JNI_FALSE; }
            else if constexpr (std::is_integral_v<R>) { return 0; }
            else { return R{}; }
        }
    }
}

extern "C" {
    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeOpen(
        JNIEnv* env,
        jclass,
        jstring path,
        jint mode,
        jlong memtable_threshold_per_shard,
        jint version_log_enabled,
        jint sst_codec,
        jint blob_codec,
        jlong blob_threshold_bytes,
        jint sst_promote_reads,
        jlong sst_bloom_bits_per_key,
        jlong max_l0_sst_files
    ) {
        return guard(
            env,
            [&]() -> jlong {
                AkkaraDB::Options options;
                options.data_dir = read_string(env, path);
                options.mode = startup_mode_from_ordinal(mode);

                assign_optional_non_negative(options.overrides.memtable_threshold_per_shard, memtable_threshold_per_shard);
                assign_optional_bool(options.overrides.version_log_enabled, version_log_enabled, "versionLogEnabled");
                if (sst_codec >= 0) { options.overrides.sst_codec = codec_from_ordinal(sst_codec); }
                if (blob_codec >= 0) { options.overrides.blob_codec = codec_from_ordinal(blob_codec); }
                assign_optional_non_negative(options.overrides.blob_threshold_bytes, blob_threshold_bytes);
                assign_optional_bool(options.overrides.sst_promote_reads, sst_promote_reads, "sstPromoteReads");
                assign_optional_non_negative(options.overrides.sst_bloom_bits_per_key, sst_bloom_bits_per_key);
                assign_optional_non_negative(options.overrides.max_l0_sst_files, max_l0_sst_files);

                auto db = AkkaraDB::open(std::move(options));
                return to_handle(db.release());
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativePut(JNIEnv* env, jobject, jlong handle, jbyteArray key, jbyteArray value) {
        guard(
            env,
            [&]() {
                auto* db = db_from(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto k = read_bytes(env, key);
                const auto v = read_bytes(env, value);
                db->engine().put(k, v);
                return 0;
            }
        );
    }

    JNIEXPORT jbyteArray JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeGet(JNIEnv* env, jobject, jlong handle, jbyteArray key) {
        return guard(
            env,
            [&]() -> jbyteArray {
                auto* db = db_from(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto k = read_bytes(env, key);
                auto value = db->engine().get(k);
                if (!value) { return nullptr; }
                return make_bytes(env, *value);
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeRemove(JNIEnv* env, jobject, jlong handle, jbyteArray key) {
        guard(
            env,
            [&]() {
                auto* db = db_from(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto k = read_bytes(env, key);
                db->engine().remove(k);
                return 0;
            }
        );
    }

    JNIEXPORT jboolean JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeExists(JNIEnv* env, jobject, jlong handle, jbyteArray key) {
        return guard(
            env,
            [&]() -> jboolean {
                auto* db = db_from(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto k = read_bytes(env, key);
                return db->engine().exists(k) ? JNI_TRUE : JNI_FALSE;
            }
        );
    }

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeCount(
        JNIEnv* env,
        jobject,
        jlong handle,
        jbyteArray start_key,
        jbyteArray end_key
    ) {
        return guard(
            env,
            [&]() -> jlong {
                auto* db = db_from(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto start = read_bytes(env, start_key);
                const auto end = read_bytes(env, end_key);
                return static_cast<jlong>(db->engine().count(start, end));
            }
        );
    }

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeOpenScan(
        JNIEnv* env,
        jobject,
        jlong handle,
        jbyteArray start_key,
        jbyteArray end_key
    ) {
        return guard(
            env,
            [&]() -> jlong {
                auto* db = db_from(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto start = read_bytes(env, start_key);
                const auto end = read_bytes(env, end_key);

                auto cursor = std::make_unique<ScanCursor>();
                cursor->db = db;
                cursor->rows = db->engine().scan(cursor->arena, start, end);
                cursor->it = cursor->rows.begin();
                return to_handle(cursor.release());
            }
        );
    }

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeOpenQueryScan(
        JNIEnv* env,
        jobject,
        jlong handle,
        jbyteArray start_key,
        jbyteArray end_key,
        jbyteArray query_bytes,
        jbyteArray schema_bytes
    ) {
        return guard(
            env,
            [&]() -> jlong {
                auto* db = db_from(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                const auto start = read_bytes(env, start_key);
                const auto end = read_bytes(env, end_key);
                const auto query = read_bytes(env, query_bytes);
                const auto schema = read_bytes(env, schema_bytes);

                auto cursor = std::make_unique<ScanCursor>();
                cursor->db = db;
                cursor->query = std::make_unique<QueryProgram>(parse_query(query, schema));
                cursor->rows = db->engine().scan(cursor->arena, start, end);
                cursor->it = cursor->rows.begin();
                return to_handle(cursor.release());
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeRollbackTo(JNIEnv* env, jobject, jlong handle, jlong target_seq) {
        guard(
            env,
            [&]() {
                auto* db = db_from(handle);
                if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
                db->engine().rollback_to(static_cast<uint64_t>(target_seq));
                return 0;
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeClose(JNIEnv* env, jobject, jlong handle) {
        guard(
            env,
            [&]() {
                std::unique_ptr<AkkaraDB> db{db_from(handle)};
                if (db) { db->close(); }
                return 0;
            }
        );
    }

    JNIEXPORT jobject JNICALL Java_dev_swiftstorm_akkaradb_engine_NativeScanCursor_nativeNext(JNIEnv* env, jobject, jlong handle) {
        return guard(
            env,
            [&]() -> jobject {
                auto* cursor = cursor_from(handle);
                if (cursor == nullptr) { throw std::runtime_error("NativeScanCursor handle is null"); }
                while (cursor->it != cursor->rows.end()) {
                    const auto& row = *cursor->it;
                    const bool matched = cursor->query == nullptr || matches_query(*cursor->query, row.value);
                    ++cursor->it;
                    if (matched) { return make_row(env, row.key, row.value); }
                }
                return nullptr;
            }
        );
    }

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_NativeScanCursor_nativeClose(JNIEnv* env, jobject, jlong handle) {
        guard(
            env,
            [&]() {
                std::unique_ptr<ScanCursor> cursor{cursor_from(handle)};
                return 0;
            }
        );
    }
}
