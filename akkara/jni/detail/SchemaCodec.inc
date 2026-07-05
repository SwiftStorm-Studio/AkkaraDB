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
