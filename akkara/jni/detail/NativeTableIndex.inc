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
