    // Runtime counterpart to query::bytecode::PreparedQuery<Entity>.
    // The JVM has BinPack bytes rather than a C++ Entity, so LoadField resolves a
    // schema path at runtime; every other operation is evaluated from the shared
    // portable opcode stream.
    struct RuntimeBytecodeQuery {
        std::vector<Value> constants;
        TypeDesc schema = makeStructType();
        std::vector<ColumnRef> fields;
        std::vector<uint8_t> code;
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
                for (uint32_t i = 0; i < count; ++i) { values.emplace_back(parseLiteral(in), parseLiteral(in)); }
                return Value::mapValue(std::move(values));
            }
            default: throw std::runtime_error("Unknown AkkaraDB portable query literal tag");
        }
    }

    [[nodiscard]] RuntimeBytecodeQuery parseQuery(std::span<const uint8_t> queryBytes, std::span<const uint8_t> schemaBytes) {
        constexpr uint8_t bytecodeMagic[] = {'A', 'K', 'Q', 'B'};
        if (queryBytes.size() < 5 || std::memcmp(queryBytes.data(), bytecodeMagic, sizeof(bytecodeMagic)) != 0) {
            throw std::runtime_error("AkkaraDB legacy AST query payloads are no longer supported; use portable query bytecode");
        }

        BytesReader in(queryBytes.subspan(sizeof(bytecodeMagic)));
        if (in.u8() != 1) { throw std::runtime_error("Unsupported AkkaraDB portable query bytecode version"); }

        RuntimeBytecodeQuery query;
        query.schema = parseRootSchema(schemaBytes);
        const auto constantCount = in.u32();
        query.constants.reserve(constantCount);
        for (uint32_t i = 0; i < constantCount; ++i) { query.constants.push_back(parseLiteral(in)); }

        const auto fieldCount = in.u16();
        query.fields.reserve(fieldCount);
        for (uint16_t i = 0; i < fieldCount; ++i) { query.fields.push_back(resolveColumn(query.schema, in.strU16())); }

        const auto codeSize = static_cast<size_t>(in.u32());
        const size_t codeStart = in.position();
        const auto code = in.slice(codeStart, codeSize);
        in.skip(codeSize);
        if (!in.eof()) { throw std::runtime_error("Trailing bytes in AkkaraDB portable query bytecode"); }
        query.code.assign(code.begin(), code.end());
        return query;
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

    [[nodiscard]] bool matchesQuery(const RuntimeBytecodeQuery& query, std::span<const uint8_t> rowValue) {
        std::vector<Value> stack;
        stack.reserve(16);
        size_t pc = 0;
        auto readU16 = [&]() {
            if (pc + 2 > query.code.size()) { throw std::runtime_error("Truncated AkkaraDB portable query bytecode operand"); }
            const uint16_t value = static_cast<uint16_t>(query.code[pc]) | static_cast<uint16_t>(static_cast<uint16_t>(query.code[pc + 1])
                << 8);
            pc += 2;
            return value;
        };
        auto pop = [&]() {
            if (stack.empty()) { throw std::runtime_error("AkkaraDB portable query bytecode stack underflow"); }
            Value value = std::move(stack.back());
            stack.pop_back();
            return value;
        };

        while (pc < query.code.size()) {
            switch (query.code[pc++]) {
                case 0x01: { // PushConst
                    const auto index = readU16();
                    if (index >= query.constants.size()) {
                        throw std::runtime_error("AkkaraDB portable query constant index out of range");
                    }
                    stack.push_back(query.constants[index]);
                    break;
                }
                case 0x02: { // LoadField
                    const auto index = readU16();
                    if (index >= query.fields.size()) { throw std::runtime_error("AkkaraDB portable query field index out of range"); }
                    stack.push_back(readColumn(rowValue, query.schema, query.fields[index]));
                    break;
                }
                case 0x03: {
                    const auto rhs = pop();
                    const auto lhs = pop();
                    stack.push_back(Value::boolean(valuesEqual(lhs, rhs)));
                    break;
                }
                case 0x04: {
                    const auto rhs = pop();
                    const auto lhs = pop();
                    stack.push_back(Value::boolean(!valuesEqual(lhs, rhs)));
                    break;
                }
                case 0x05:
                case 0x06:
                case 0x07:
                case 0x08: {
                    const auto rhs = pop();
                    const auto lhs = pop();
                    const auto cmp = compareValues(lhs, rhs);
                    const uint8_t op = query.code[pc - 1];
                    stack.push_back(Value::boolean(op == 0x05 ? cmp < 0 : op == 0x06 ? cmp <= 0 : op == 0x07 ? cmp > 0 : cmp >= 0));
                    break;
                }
                case 0x09: stack.push_back(Value::boolean(!valueIsTrue(pop())));
                    break;
                case 0x0A: {
                    const auto rhs = pop();
                    const auto lhs = pop();
                    stack.push_back(Value::boolean(valueIsTrue(lhs) && valueIsTrue(rhs)));
                    break;
                }
                case 0x0B: {
                    const auto rhs = pop();
                    const auto lhs = pop();
                    stack.push_back(Value::boolean(valueIsTrue(lhs) || valueIsTrue(rhs)));
                    break;
                }
                case 0x12: {
                    const auto rhs = pop();
                    const auto lhs = pop();
                    stack.push_back(Value::boolean(stringStartsWith(lhs, rhs)));
                    break;
                }
                case 0x14: {
                    const auto rhs = pop();
                    const auto lhs = pop();
                    stack.push_back(Value::boolean(stringContains(lhs, rhs)));
                    break;
                }
                case 0x15: {
                    const auto rhs = pop();
                    const auto lhs = pop();
                    stack.push_back(Value::boolean(stringLike(lhs, rhs)));
                    break;
                }
                case 0x1C: {
                    const auto rhs = pop();
                    const auto lhs = pop();
                    stack.push_back(Value::boolean(valueInList(lhs, rhs)));
                    break;
                }
                case 0x1D: {
                    const auto rhs = pop();
                    const auto lhs = pop();
                    stack.push_back(Value::boolean(!valueInList(lhs, rhs)));
                    break;
                }
                case 0x1E: {
                    const auto key = pop();
                    const auto map = pop();
                    stack.push_back(mapGetValue(map, key));
                    break;
                }
                case 0x10: if (pc != query.code.size() || stack.size() != 1) {
                        throw std::runtime_error("Malformed AkkaraDB portable query bytecode return");
                    }
                    return valueIsTrue(stack.back());
                default: throw std::runtime_error("Unsupported AkkaraDB portable query opcode");
            }
        }
        throw std::runtime_error("AkkaraDB portable query bytecode is missing Return");
    }
