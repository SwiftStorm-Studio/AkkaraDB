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
