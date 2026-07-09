# Akkara Query Clang Plugin

This is the distributable skeleton for a future AkkaraDB Native C++ query rewriter.

The goal is to bring the Kotlin compiler-plugin model to C++:

- inspect query lambdas at compile time with Clang AST hooks,
- turn entity-field expressions into an Akkara query AST,
- evaluate row-independent outer expressions as captures,
- leave the runtime library with an explicit compiled query object instead of an opaque C++ lambda.

This plugin can now do diagnostic lowering and sidecar source rewriting. With
`-plugin-arg-akkara-query-dump`, it finds AkkaraDB `PackedTable::query(...)` and
`QueryView::where(...)` calls that take a lambda, lowers the returned expression
into a small diagnostic IR, and prints the index-source strategy it would choose
for compound predicates. With `rewrite` plus `rewrite-output=<path>`, it writes
a rewritten source file where supported `PackedTable::query(...)` lambdas are
replaced by `akkaradb::query::bytecode::CompiledQueryDescriptor<T>` objects.

Current compound predicate strategy:

- `A && B`: use the highest-scoring indexable side as the source scan, then keep
  the whole predicate as a residual filter.
- `A || B`: use an index union only when both sides are indexable; otherwise fall
  back to a table scan.
- Equality is scored above ordered ranges; prefix-like string operations are
  scored above range scans; non-sargable string operations such as `contains` and
  `ends_with` are treated as full-field-index candidates plus residual filtering.

## Build

Configure this directory as a standalone CMake project with LLVM and Clang package paths available.

On Windows, use an LLVM/Clang package that includes the development CMake files. The official LLVM installer provides
`clang++`, but may not include `LLVMConfig.cmake`, `ClangConfig.cmake`, and the Clang development libraries required to
build a plugin. The verified local setup is MSYS2 UCRT64:

- `C:/msys64/ucrt64/bin/clang++.exe`
- `C:/msys64/ucrt64/lib/cmake/llvm/LLVMConfig.cmake`
- `C:/msys64/ucrt64/lib/cmake/clang/ClangConfig.cmake`

```powershell
C:\msys64\usr\bin\bash.exe -lc "export PATH=/ucrt64/bin:/usr/bin:`$PATH; cd /c/Users/main/CLionProjects/akkaradb/akkaradb-native/akkara/akkquery-clang-plugin && cmake --preset windows-msys2-ucrt64-release && cmake --build --preset windows-msys2-ucrt64-release"
```

Portable shape:

```powershell
cmake -S akkara/akkquery-clang-plugin -B builds/akkquery-clang-plugin `
  -DCMAKE_CXX_COMPILER="C:/path/to/clang++.exe" `
  -DLLVM_DIR="C:/path/to/llvm/lib/cmake/llvm" `
  -DClang_DIR="C:/path/to/llvm/lib/cmake/clang"

cmake --build builds/akkquery-clang-plugin --config Release
```

## Load

Exact flags vary by compiler distribution. With Clang, the intended shape is:

```bash
clang++ -Xclang -load -Xclang /path/to/akkara-query.so \
        -Xclang -add-plugin -Xclang akkara-query \
        -Xclang -plugin-arg-akkara-query -Xclang dump \
        app.cpp
```

Sidecar rewrite:

```bash
clang++ -fsyntax-only \
        -DAKKARADB_QUERY_REWRITE_PASS \
        -Xclang -load -Xclang /path/to/akkara-query.so \
        -Xclang -add-plugin -Xclang akkara-query \
        -Xclang -plugin-arg-akkara-query -Xclang rewrite \
        -Xclang -plugin-arg-akkara-query -Xclang rewrite-output=app.akkara.rewritten.cpp \
        app.cpp
```

In copied work trees, `rewrite-in-place` can be used instead of
`rewrite-output=<path>` to overwrite every edited file, including included
project headers:

```bash
clang++ -fsyntax-only \
        -DAKKARADB_QUERY_REWRITE_PASS \
        -Xclang -load -Xclang /path/to/akkara-query.so \
        -Xclang -add-plugin -Xclang akkara-query \
        -Xclang -plugin-arg-akkara-query -Xclang rewrite \
        -Xclang -plugin-arg-akkara-query -Xclang rewrite-in-place \
        app.cpp
```

## Rewrite Build Runner

The repository also exposes a Windows build target that automates the sidecar
workflow for the full tree:

```powershell
cmake --preset windows-clang-cl-release
cmake --build --preset windows-clang-cl-query-rewrite-build
```

`akkaradb_query_rewrite_build` copies the repository to
`builds/akkara-query-rewrite/<preset>/source`, configures that copy, builds the
query plugin, rewrites copied `.cpp` translation units and any edited project
headers in place, and then builds the copied source tree. The original
repository files are not overwritten.

The runner is intentionally conservative at this stage:

- rewrite is driven from translation-unit source files (`.cpp`, `.cc`, `.cxx`);
  included project headers are rewritten in place when their calls are
  semantically resolvable in that translation unit,
- the actual build still uses the selected CMake preset, normally
  `windows-clang-cl-release`,
- the rewrite scan uses the standalone Clang plugin toolchain and includes from
  the configured copied tree,
- the rewrite scan defines `AKKARADB_QUERY_REWRITE_PASS` so typed entity lambdas
  can be parsed without instantiating the normal runtime proxy-lambda path.

## Rewrite Shape

Source code:

```cpp
auto result = profiles.query([](const Profile& profile) {
    return profile.age >= 18 && profile.name == "Bobby";
});
```

Rewritten source:

```cpp
auto result = profiles.query(
    ([&]() {
        using __AkkEntity = Profile;
        static constexpr ::std::array<::std::uint8_t, 16> __akk_code{/* stack VM bytes */};
        static constexpr ::std::array<::akkaradb::query::bytecode::Value, 2> __akk_constants{
            ::akkaradb::query::bytecode::Value::integer(18),
            ::akkaradb::query::bytecode::Value::string("Bobby")
        };
        static constexpr ::std::array<::akkaradb::query::bytecode::FieldBinding<__AkkEntity>, 2> __akk_fields{
            ::akkaradb::query::bytecode::makeTopLevelFieldBinding<&Profile::age, 3>("age"),
            ::akkaradb::query::bytecode::makeTopLevelFieldBinding<&Profile::name, 2>("name")
        };
        static constexpr ::std::array<::akkaradb::query::bytecode::PlanHint, 2> __akk_hints{/* equality hints first */};
        return ::akkaradb::query::bytecode::CompiledQueryDescriptor<__AkkEntity>{
            .code = __akk_code,
            .constants = __akk_constants,
            .fields = __akk_fields,
            .hostCalls = {},
            .planHints = __akk_hints,
            .captures = nullptr,
            .capturesOwner = {}
        };
    }())
);
```

The bytecode rewrite currently requires a typed non-generic lambda parameter.
Expressions rooted at the entity parameter become field loads and VM operations.
Row-independent values become descriptor constants; when those constants need
runtime storage, the generated descriptor owns a small capture block. Unsupported
row-dependent predicates fall back to a generated `HostCallBool` thunk that owns
the original lambda.

Example dump:

```text
[akkara-query] candidate call `query` at app.cpp:15:5
  ir: and(ge(col(age), capture(minAge)), or(eq(col(email), lit("a@example.test")), ends_with(col(email), lit(".test"))))
  plan: index-scan(age ge, score=80) + residual-filter
[akkara-query] candidate call `query` at app.cpp:18:5
  ir: or(eq(col(email), lit("a@example.test")), lt(col(age), lit(30)))
  plan: index-union(email eq | age lt, score=70) + residual-filter
```

Example rewrite:

```cpp
profiles.query([](const Profile& profile) {
    return profile.age >= 18 && profile.name == "Bobby";
});
```

becomes:

```cpp
profiles.query(
    ([&]() {
        /* static bytecode, constants, field bindings, and plan hints */
        return ::akkaradb::query::bytecode::CompiledQueryDescriptor<Profile>{...};
    }())
);
```

Current rewrite coverage:

- `PackedTable::query(lambda)` is rewritten to a bytecode descriptor,
- direct `PackedTable::query(lambda).where(lambda)` chains and variables that
  hold a rewritten query view, for example `auto view = profiles.query(...);
  view.where(...)`, are rewritten to descriptor calls and composed into one
  bytecode program at runtime,
- if a `where` lambda cannot be emitted as composable bytecode, it is left
  unchanged and runs as a decoded predicate filter,
- typed lambda parameters such as `[](const Profile& profile)`,
- top-level entity fields such as `profile.age` and `profile.email`, with raw
  row evaluation when every field has a raw reader,
- nested aggregate fields such as `profile.address.city`, evaluated after entity
  decode,
- `==`, `!=`, `<`, `<=`, `>`, `>=`,
- `&&`, `||`, `!`; `&&` and `||` are lowered with `JumpIfFalse` /
  `JumpIfTrue` short-circuit control flow,
- numeric `+`, `-`, `*`, `/`, and `%` arithmetic expressions,
- `starts_with`, `startsWith`, `ends_with`, `endsWith`, `contains`, and `like`
  as bytecode string operations,
- integer, floating, bool, character, and string literals,
- row-independent capture expressions as owned constants,
- whole-lambda host-call fallback for unsupported row-dependent expressions such
  as custom function calls or `Ref<T>` resolution in `query(...)`,
- user-registered stack opcodes through
  `akkaradb::query::bytecode::CustomOpcodeRegistry`,
- statically registered user opcode calls through `AKKARADB_QUERY_OPCODE(...)`
  are lowered to `CallCustom`,
- plan hints are emitted for value-vs-field comparisons and string prefixes, and
  sorted so equality/prefix hints are considered before ordered range hints.
  Hints are emitted only where using one side as the scan source is safe; `||`
  and `!` subexpressions keep residual filtering without unsafe index narrowing.

## User Opcodes

User opcodes are registered as stack functions. A custom opcode receives the
already-evaluated stack values declared by its arity and returns one
`akkaradb::query::bytecode::Value`.

The runtime contract is:

- user opcode ids must be in `0x8000..0xFFFF`,
- names must be non-empty and unique within the registry,
- arity must be `<= 16`,
- the thunk must be non-null,
- the thunk must return the declared `resultKind`.

`CustomOpcodeRegistry::registerOpcode(...)` rejects metadata that violates this
contract, writes a diagnostic to the supplied stream, and leaves the registry
unchanged so the process can continue. If a thunk later returns `false` or
returns a value with the wrong `resultKind`, query execution treats that as an
implementation contract violation.

For Clang Plugin lowering, declare a static opcode registration before the query
that calls the function:

Example:

```cpp
namespace bc = akkaradb::query::bytecode;

bool oddAge(std::span<const bc::Value> args, bc::Value& out, const void*) {
    if (args.size() != 1 || !args[0].isNumeric()) { return false; }
    out = bc::Value::boolean((args[0].u % 2U) == 1U);
    return true;
}

bc::CustomOpcodeRegistry registry;
registry.registerOpcode(
    bc::CustomOpcodeBinding{
        .opcode = bc::customOpcodeUserMin,
        .name = "odd_age",
        .arity = 1,
        .resultKind = bc::ValueKind::Bool,
        .thunk = &oddAge
    }
);
```

To make a normal C++ function lower to `CallCustom`, bind that function to the
same stack opcode metadata:

```cpp
bool customOddAge(uint32_t age) {
    return (age % 2U) == 1U;
}

AKKARADB_QUERY_OPCODE(
    customOddAge,
    bc::customOpcodeUserMin,
    "odd_age",
    1,
    bc::ValueKind::Bool,
    &oddAge
);

profiles.query([](const Profile& p) {
    return customOddAge(p.age);
});
```

The rewritten bytecode shape is:

```cpp
LoadField age
CallCustom odd_age
Return
```

The plugin only lowers calls whose target function has a visible
`AKKARADB_QUERY_OPCODE(...)` registration earlier in the translation unit. If no
registration is visible, or if the call arity does not match the registration,
the expression keeps the existing whole-lambda `HostCallBool` fallback.

`akkaradb::Ref<T>` is deliberately not traversed by the sidecar rewrite. A
predicate such as `post.author->name == "Alice"` crosses a table boundary and
needs join/resolve semantics instead of a local raw-row field path rewrite. For
`PackedTable::query(lambda)`, the plugin keeps the lambda as an owned
`HostCallBool` descriptor so normal C++ `Ref<T>` lazy resolution still runs. For
`where(lambda)`, a `Ref<T>` crossing is left as the existing decoded predicate
filter because `where` descriptors must be bytecode-composable. Explicit
`join(...).where([](const Left&, const Right&) { ... })` keeps the existing
two-entity predicate semantics and is not rewritten into local row bytecode.

## Next Steps

1. Add integration tests using `clang -cc1 -load`.
2. Add optional Ref-aware join bytecode only after the public join API exposes a
   stable bytecode descriptor shape for two-table predicates.
