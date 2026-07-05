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
