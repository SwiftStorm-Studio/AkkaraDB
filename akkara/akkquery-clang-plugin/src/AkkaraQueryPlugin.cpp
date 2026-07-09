#include "clang/AST/ASTConsumer.h"
#include "clang/AST/DeclCXX.h"
#include "clang/AST/Expr.h"
#include "clang/AST/ExprCXX.h"
#include "clang/AST/OperationKinds.h"
#include "clang/AST/RecursiveASTVisitor.h"
#include "clang/Frontend/CompilerInstance.h"
#include "clang/Frontend/FrontendPluginRegistry.h"
#include "clang/Lex/Lexer.h"
#include "clang/Rewrite/Core/Rewriter.h"

#include "llvm/ADT/StringRef.h"
#include "llvm/Support/FileSystem.h"
#include "llvm/Support/raw_ostream.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

namespace akkara::query_plugin {
namespace {

enum class IrKind {
    Unsupported,
    Column,
    Literal,
    Capture,
    CustomCall,
    Arithmetic,
    Compare,
    Logical,
    Unary
};

struct CustomOpcodeDecl {
    const clang::FunctionDecl* function = nullptr;
    std::string bindingExpression;
    std::string opcodeExpression;
    uint8_t arity = 0;
};

struct IrNode {
    IrKind kind = IrKind::Unsupported;
    std::string op;
    std::string text;
    bool rowDependent = false;
    bool crossesRef = false;
    std::string customOpcodeBindingExpression;
    std::string customOpcodeExpression;
    uint8_t customOpcodeArity = 0;
    std::vector<std::string> fieldNames;
    std::vector<std::string> fieldPointers;
    std::vector<size_t> fieldIndices;
    std::vector<IrNode> children;
};

struct PlanSummary {
    enum class Kind {
        TableScan,
        IndexScan,
        IndexUnion
    };

    Kind kind = Kind::TableScan;
    int score = 0;
    std::string source;
    bool residual = true;

    [[nodiscard]] bool indexable() const noexcept {
        return kind == Kind::IndexScan || kind == Kind::IndexUnion;
    }
};

struct LambdaContext {
    clang::ASTContext& ast;
    std::vector<const clang::ParmVarDecl*> rowParams;
    const std::vector<CustomOpcodeDecl>* customOpcodes = nullptr;
};

struct RewriteResult {
    std::string replacementSource;
    std::string reason;
    bool ok = false;
};

struct FieldPathInfo {
    std::string display;
    bool crossesRef = false;
    std::vector<std::string> names;
    std::vector<std::string> pointers;
    std::vector<size_t> indices;
};

struct InitCaptureInfo {
    std::string name;
    std::string initializerSource;
};

[[nodiscard]] const clang::Expr* ignoreNoise(const clang::Expr* expr) {
    if (expr == nullptr) { return nullptr; }
    return expr->IgnoreParenImpCasts();
}

[[nodiscard]] std::string sourceText(const clang::ASTContext& context, const clang::Expr* expr) {
    if (expr == nullptr || expr->getBeginLoc().isInvalid() || expr->getEndLoc().isInvalid()) { return "<expr>"; }
    const auto range = clang::CharSourceRange::getTokenRange(expr->getSourceRange());
    const auto text = clang::Lexer::getSourceText(range, context.getSourceManager(), context.getLangOpts());
    if (text.empty()) { return "<expr>"; }
    return text.str();
}

[[nodiscard]] std::string sourceText(const clang::ASTContext& context, clang::SourceRange sourceRange) {
    if (sourceRange.getBegin().isInvalid() || sourceRange.getEnd().isInvalid()) { return {}; }
    const auto range = clang::CharSourceRange::getTokenRange(sourceRange);
    const auto text = clang::Lexer::getSourceText(range, context.getSourceManager(), context.getLangOpts());
    return text.str();
}

[[nodiscard]] bool isRowParam(const clang::Decl* decl, const LambdaContext& context) {
    const auto* parm = llvm::dyn_cast_or_null<clang::ParmVarDecl>(decl);
    if (parm == nullptr) { return false; }
    return std::find(context.rowParams.begin(), context.rowParams.end(), parm) != context.rowParams.end();
}

[[nodiscard]] bool typeLooksLikeAkkaraRef(clang::QualType type) {
    const std::string spelled = type.getCanonicalType().getUnqualifiedType().getAsString();
    return spelled.find("akkaradb::Ref<") != std::string::npos || spelled.find("class akkaradb::Ref<") != std::string::npos;
}

[[nodiscard]] std::string fieldPointerText(const clang::ValueDecl* decl) {
    if (decl == nullptr) { return {}; }
    const auto* record = llvm::dyn_cast_or_null<clang::RecordDecl>(decl->getDeclContext());
    if (record == nullptr) { return {}; }
    return record->getQualifiedNameAsString() + "::" + decl->getNameAsString();
}

[[nodiscard]] size_t fieldIndex(const clang::ValueDecl* decl) {
    if (decl == nullptr) { return std::numeric_limits<size_t>::max(); }
    const auto* field = llvm::dyn_cast_or_null<clang::FieldDecl>(decl);
    if (field == nullptr) { return std::numeric_limits<size_t>::max(); }
    const auto* record = llvm::dyn_cast_or_null<clang::RecordDecl>(field->getDeclContext());
    if (record == nullptr) { return std::numeric_limits<size_t>::max(); }

    size_t index = 0;
    for (const clang::FieldDecl* current : record->fields()) {
        if (current == field) { return index; }
        ++index;
    }
    return std::numeric_limits<size_t>::max();
}

[[nodiscard]] std::optional<FieldPathInfo> tryFieldPath(const clang::Expr* expr, const LambdaContext& context) {
    expr = ignoreNoise(expr);
    if (expr == nullptr) { return std::nullopt; }

    if (const auto* member = llvm::dyn_cast<clang::MemberExpr>(expr)) {
        const auto base = ignoreNoise(member->getBase());
        if (const auto* ref = llvm::dyn_cast_or_null<clang::DeclRefExpr>(base); ref != nullptr && isRowParam(ref->getDecl(), context)) {
            FieldPathInfo out;
            out.display = member->getMemberNameInfo().getAsString();
            out.names.push_back(out.display);
            out.pointers.push_back(fieldPointerText(member->getMemberDecl()));
            out.indices.push_back(fieldIndex(member->getMemberDecl()));
            return out;
        }
        if (auto parent = tryFieldPath(base, context)) {
            const std::string name = member->getMemberNameInfo().getAsString();
            const bool parentIsRef = typeLooksLikeAkkaraRef(base->getType());
            parent->display.append(".");
            parent->display.append(name);
            parent->names.push_back(name);
            parent->pointers.push_back(fieldPointerText(member->getMemberDecl()));
            parent->indices.push_back(fieldIndex(member->getMemberDecl()));
            parent->crossesRef = parent->crossesRef || parentIsRef;
            return parent;
        }
        return std::nullopt;
    }

    if (const auto* member = llvm::dyn_cast<clang::CXXDependentScopeMemberExpr>(expr)) {
        const auto base = ignoreNoise(member->getBase());
        if (const auto* ref = llvm::dyn_cast_or_null<clang::DeclRefExpr>(base); ref != nullptr && isRowParam(ref->getDecl(), context)) {
            FieldPathInfo out;
            out.display = member->getMemberNameInfo().getAsString();
            out.names.push_back(out.display);
            out.indices.push_back(std::numeric_limits<size_t>::max());
            return out;
        }
        if (auto parent = tryFieldPath(base, context)) {
            const std::string name = member->getMemberNameInfo().getAsString();
            parent->display.append(".");
            parent->display.append(name);
            parent->names.push_back(name);
            parent->pointers.emplace_back();
            parent->indices.push_back(std::numeric_limits<size_t>::max());
            return parent;
        }
    }

    return std::nullopt;
}

[[nodiscard]] bool dependsOnRow(const clang::Stmt* stmt, const LambdaContext& context) {
    if (stmt == nullptr) { return false; }
    if (const auto* expr = llvm::dyn_cast<clang::Expr>(stmt)) {
        if (tryFieldPath(expr, context).has_value()) { return true; }
    }
    for (const clang::Stmt* child : stmt->children()) {
        if (dependsOnRow(child, context)) { return true; }
    }
    return false;
}

[[nodiscard]] bool isLiteralExpr(const clang::Expr* expr) {
    expr = ignoreNoise(expr);
    return llvm::isa_and_nonnull<clang::IntegerLiteral>(expr)
        || llvm::isa_and_nonnull<clang::FloatingLiteral>(expr)
        || llvm::isa_and_nonnull<clang::StringLiteral>(expr)
        || llvm::isa_and_nonnull<clang::CXXBoolLiteralExpr>(expr)
        || llvm::isa_and_nonnull<clang::CharacterLiteral>(expr)
        || llvm::isa_and_nonnull<clang::CXXNullPtrLiteralExpr>(expr);
}

[[nodiscard]] std::optional<std::string> binaryCompareOp(clang::BinaryOperatorKind op) {
    switch (op) {
        case clang::BO_EQ: return "eq";
        case clang::BO_NE: return "ne";
        case clang::BO_GT: return "gt";
        case clang::BO_GE: return "ge";
        case clang::BO_LT: return "lt";
        case clang::BO_LE: return "le";
        default: return std::nullopt;
    }
}

[[nodiscard]] std::optional<std::string> binaryArithmeticOp(clang::BinaryOperatorKind op) {
    switch (op) {
        case clang::BO_Add: return "add";
        case clang::BO_Sub: return "sub";
        case clang::BO_Mul: return "mul";
        case clang::BO_Div: return "div";
        case clang::BO_Rem: return "mod";
        default: return std::nullopt;
    }
}

[[nodiscard]] std::optional<std::string> overloadedCompareOp(clang::OverloadedOperatorKind op) {
    switch (op) {
        case clang::OO_EqualEqual: return "eq";
        case clang::OO_ExclaimEqual: return "ne";
        case clang::OO_Greater: return "gt";
        case clang::OO_GreaterEqual: return "ge";
        case clang::OO_Less: return "lt";
        case clang::OO_LessEqual: return "le";
        default: return std::nullopt;
    }
}

[[nodiscard]] std::optional<std::string> overloadedArithmeticOp(clang::OverloadedOperatorKind op) {
    switch (op) {
        case clang::OO_Plus: return "add";
        case clang::OO_Minus: return "sub";
        case clang::OO_Star: return "mul";
        case clang::OO_Slash: return "div";
        case clang::OO_Percent: return "mod";
        default: return std::nullopt;
    }
}

[[nodiscard]] std::optional<std::string> methodQueryOp(llvm::StringRef name) {
    if (name == "startsWith" || name == "starts_with") { return "starts_with"; }
    if (name == "contains") { return "contains"; }
    if (name == "like") { return "like"; }
    if (name == "endsWith" || name == "ends_with") { return "ends_with"; }
    return std::nullopt;
}

[[nodiscard]] const clang::FunctionDecl* canonicalFunctionDecl(const clang::FunctionDecl* function) {
    return function == nullptr ? nullptr : function->getCanonicalDecl();
}

[[nodiscard]] const clang::FunctionDecl* functionDeclFromTemplateArgExpr(const clang::Expr* expr) {
    expr = ignoreNoise(expr);
    if (const auto* unary = llvm::dyn_cast_or_null<clang::UnaryOperator>(expr)) {
        if (unary->getOpcode() == clang::UO_AddrOf) { expr = ignoreNoise(unary->getSubExpr()); }
    }
    if (const auto* ref = llvm::dyn_cast_or_null<clang::DeclRefExpr>(expr)) {
        return canonicalFunctionDecl(llvm::dyn_cast_or_null<clang::FunctionDecl>(ref->getDecl()));
    }
    return nullptr;
}

[[nodiscard]] const clang::FunctionDecl* functionDeclFromTemplateArg(const clang::TemplateArgument& arg) {
    if (arg.getKind() == clang::TemplateArgument::Declaration) {
        return canonicalFunctionDecl(llvm::dyn_cast_or_null<clang::FunctionDecl>(arg.getAsDecl()));
    }
    if (arg.getKind() == clang::TemplateArgument::Expression) {
        return functionDeclFromTemplateArgExpr(arg.getAsExpr());
    }
    return nullptr;
}

[[nodiscard]] std::optional<uint64_t> unsignedIntegralTemplateArg(const clang::TemplateArgument& arg) {
    if (arg.getKind() != clang::TemplateArgument::Integral) { return std::nullopt; }
    const llvm::APSInt& value = arg.getAsIntegral();
    if (value.isSigned() && value.isNegative()) { return std::nullopt; }
    if (value.getActiveBits() > 64) { return std::nullopt; }
    return value.getZExtValue();
}

[[nodiscard]] const clang::TemplateSpecializationType* templateSpecializationType(clang::QualType type) {
    if (type.isNull()) { return nullptr; }
    const clang::Type* desugared = type.getTypePtr()->getUnqualifiedDesugaredType();
    return llvm::dyn_cast_or_null<clang::TemplateSpecializationType>(desugared);
}

[[nodiscard]] std::optional<CustomOpcodeDecl> customOpcodeDeclFromVar(const clang::VarDecl* var) {
    if (var == nullptr) { return std::nullopt; }

    llvm::ArrayRef<clang::TemplateArgument> args;
    std::string templateName;
    if (const auto* spec = templateSpecializationType(var->getType())) {
        const clang::TemplateDecl* templ = spec->getTemplateName().getAsTemplateDecl();
        if (templ == nullptr) { return std::nullopt; }
        templateName = templ->getQualifiedNameAsString();
        args = spec->template_arguments();
    }
    else if (const auto* record = var->getType()->getAsCXXRecordDecl()) {
        const auto* classSpec = llvm::dyn_cast<clang::ClassTemplateSpecializationDecl>(record);
        if (classSpec == nullptr || classSpec->getSpecializedTemplate() == nullptr) { return std::nullopt; }
        templateName = classSpec->getSpecializedTemplate()->getQualifiedNameAsString();
        args = classSpec->getTemplateArgs().asArray();
    }
    else { return std::nullopt; }

    if (templateName.find("akkaradb::query::bytecode::StaticCustomOpcodeRegistration") == std::string::npos) {
        return std::nullopt;
    }
    if (args.size() < 3) { return std::nullopt; }

    const clang::FunctionDecl* function = functionDeclFromTemplateArg(args[0]);
    auto opcode = unsignedIntegralTemplateArg(args[1]);
    auto arity = unsignedIntegralTemplateArg(args[2]);
    if (function == nullptr || !opcode.has_value() || !arity.has_value() || *opcode > std::numeric_limits<uint16_t>::max()
        || *arity > std::numeric_limits<uint8_t>::max()) {
        return std::nullopt;
    }

    std::string name = var->getQualifiedNameAsString();
    if (name.empty() || name.find("(anonymous namespace)") != std::string::npos) { name = var->getNameAsString(); }

    return CustomOpcodeDecl{
        function,
        name + ".binding()",
        name + ".opcode",
        static_cast<uint8_t>(*arity)
    };
}

class LambdaAnalyzer {
public:
    explicit LambdaAnalyzer(LambdaContext context)
        : context_{std::move(context)} {}

    [[nodiscard]] IrNode analyze(const clang::LambdaExpr* lambda) const {
        if (lambda == nullptr) { return unsupported("<missing lambda>"); }

        const clang::Stmt* body = lambda->getBody();
        if (const auto* compound = llvm::dyn_cast_or_null<clang::CompoundStmt>(body)) {
            for (const clang::Stmt* stmt : compound->body()) {
                if (const auto* ret = llvm::dyn_cast_or_null<clang::ReturnStmt>(stmt)) {
                    return parseExpr(ret->getRetValue());
                }
            }
            return unsupported("<lambda without return>");
        }

        if (const auto* expr = llvm::dyn_cast_or_null<clang::Expr>(body)) { return parseExpr(expr); }
        return unsupported("<unsupported lambda body>");
    }

    [[nodiscard]] PlanSummary plan(const IrNode& node) const { return planNode(node); }

    [[nodiscard]] RewriteResult rewrite(const clang::LambdaExpr* lambda, const IrNode& node, bool allowHostFallback = true) const {
        RewriteResult result;
        if (lambda == nullptr) {
            result.reason = "missing lambda";
            return result;
        }

        auto expression = renderBytecodeDescriptor(lambda, node, result.reason, allowHostFallback);
        if (!expression.has_value()) {
            if (result.reason.empty()) {
                result.reason = containsRefCrossing(node)
                                    ? "field path crosses akkaradb::Ref<T>; use an explicit join or resolved field"
                                    : "lambda cannot be rendered as an Akkara bytecode descriptor";
            }
            return result;
        }

        auto initCaptures = collectInitCaptures(lambda);
        if (!initCaptures.has_value()) {
            result.reason = "lambda init-capture source could not be recovered";
            return result;
        }

        result.replacementSource = wrapWithInitCaptures(*expression, *initCaptures);
        result.ok = true;
        return result;
    }

    void print(const IrNode& node, const PlanSummary& plan) const {
        llvm::errs() << "  ir: ";
        printNode(node);
        llvm::errs() << '\n';

        llvm::errs() << "  plan: ";
        switch (plan.kind) {
            case PlanSummary::Kind::TableScan:
                llvm::errs() << "table-scan";
                break;
            case PlanSummary::Kind::IndexScan:
                llvm::errs() << "index-scan(" << plan.source << ", score=" << plan.score << ')';
                break;
            case PlanSummary::Kind::IndexUnion:
                llvm::errs() << "index-union(" << plan.source << ", score=" << plan.score << ')';
                break;
        }
        if (plan.residual) { llvm::errs() << " + residual-filter"; }
        llvm::errs() << '\n';
    }

private:
    [[nodiscard]] const CustomOpcodeDecl* findCustomOpcode(const clang::FunctionDecl* function) const {
        function = canonicalFunctionDecl(function);
        if (function == nullptr || context_.customOpcodes == nullptr) { return nullptr; }
        for (const auto& opcode : *context_.customOpcodes) {
            if (opcode.function == function) { return &opcode; }
        }
        return nullptr;
    }

    [[nodiscard]] std::optional<std::vector<InitCaptureInfo>> collectInitCaptures(const clang::LambdaExpr* lambda) const {
        std::vector<InitCaptureInfo> out;
        for (const clang::LambdaCapture& capture : lambda->captures()) {
            if (!capture.capturesVariable()) { continue; }
            const auto* captured = llvm::dyn_cast_or_null<clang::VarDecl>(capture.getCapturedVar());
            if (captured == nullptr || !captured->isInitCapture()) { continue; }
            if (captured->getIdentifier() == nullptr || captured->getInit() == nullptr) { return std::nullopt; }
            std::string initializer = sourceText(context_.ast, captured->getInit());
            if (initializer.empty() || initializer == "<expr>") { return std::nullopt; }
            out.push_back(InitCaptureInfo{captured->getNameAsString(), std::move(initializer)});
        }
        return out;
    }

    [[nodiscard]] static std::string wrapWithInitCaptures(std::string expression, const std::vector<InitCaptureInfo>& initCaptures) {
        if (initCaptures.empty()) { return expression; }

        std::string out = "([&]() { ";
        for (const auto& capture : initCaptures) {
            out.append("auto ");
            out.append(capture.name);
            out.append(" = ");
            out.append(capture.initializerSource);
            out.append("; ");
        }
        out.append("return ");
        out.append(expression);
        out.append("; }())");
        return out;
    }

    struct BytecodeField {
        std::string name;
        std::string pointer;
        std::string pathExpression;
        size_t index = std::numeric_limits<size_t>::max();
        bool topLevel = false;
    };

    struct BytecodeConstant {
        std::string valueExpression;
        std::string sourceExpression;
        bool dynamic = false;
    };

    struct BytecodePlanHint {
        std::string op;
        uint16_t field = 0;
        uint16_t constant = 0;
        int score = 0;
    };

    struct BytecodeCustomOpcode {
        std::string bindingExpression;
    };

    struct BytecodeBuild {
        std::vector<std::string> code;
        std::vector<BytecodeConstant> constants;
        std::vector<BytecodeField> fields;
        std::vector<BytecodeCustomOpcode> customOpcodes;
        std::vector<BytecodePlanHint> hints;
        std::string reason;

        [[nodiscard]] bool ok() const noexcept { return reason.empty(); }

        [[nodiscard]] bool hasDynamicConstants() const noexcept {
            return std::any_of(constants.begin(), constants.end(), [](const BytecodeConstant& constant) {
                return constant.dynamic;
            });
        }

        void fail(std::string message) {
            if (reason.empty()) { reason = std::move(message); }
        }

        void emitByte(uint16_t value) {
            code.push_back(std::to_string(value & 0xFFU));
        }

        void emitOp(std::string_view op) {
            code.emplace_back("static_cast<::std::uint8_t>(::akkaradb::query::bytecode::Opcode::");
            code.back().append(op);
            code.back().append(")");
        }

        void emitU16(uint16_t value) {
            emitByte(value);
            emitByte(static_cast<uint16_t>(value >> 8));
        }

        [[nodiscard]] size_t currentOffset() const noexcept { return code.size(); }

        void patchU16(size_t offset, size_t value) {
            if (offset + 1 >= code.size()) {
                fail("bytecode jump patch offset is out of range");
                return;
            }
            if (value > std::numeric_limits<uint16_t>::max()) {
                fail("bytecode jump target is too large");
                return;
            }
            code[offset] = std::to_string(value & 0xFFU);
            code[offset + 1] = std::to_string((value >> 8) & 0xFFU);
        }

        [[nodiscard]] size_t emitJump(std::string_view op) {
            emitOp(op);
            const size_t offset = currentOffset();
            emitU16(0);
            return offset;
        }

        void emitU16Expr(std::string_view value) {
            code.emplace_back("static_cast<::std::uint8_t>((");
            code.back().append(value);
            code.back().append(") & 0xFFU)");
            code.emplace_back("static_cast<::std::uint8_t>(((");
            code.back().append(value);
            code.back().append(") >> 8) & 0xFFU)");
        }

        [[nodiscard]] std::optional<uint16_t> addField(const IrNode& node) {
            if (node.kind != IrKind::Column) {
                fail("bytecode field operand is not a column");
                return std::nullopt;
            }
            if (node.crossesRef) {
                fail("bytecode field path crosses akkaradb::Ref<T>; use a host call or an explicit join");
                return std::nullopt;
            }
            if (node.fieldNames.empty() || node.fieldPointers.empty() || node.fieldIndices.empty()) {
                fail("bytecode field path could not be resolved");
                return std::nullopt;
            }
            if (node.fieldPointers.front().empty()) {
                fail("bytecode field pointer/index could not be resolved; use a typed lambda parameter");
                return std::nullopt;
            }

            std::string path;
            for (size_t i = 0; i < node.fieldNames.size(); ++i) {
                path.append(".");
                path.append(node.fieldNames[i]);
            }

            const auto existing = std::find_if(fields.begin(), fields.end(), [&](const BytecodeField& field) {
                return field.pathExpression == path;
            });
            if (existing != fields.end()) { return static_cast<uint16_t>(std::distance(fields.begin(), existing)); }
            if (fields.size() >= std::numeric_limits<uint16_t>::max()) {
                fail("bytecode query has too many fields");
                return std::nullopt;
            }
            const bool topLevel = node.fieldNames.size() == 1 && node.fieldIndices.front() != std::numeric_limits<size_t>::max();
            fields.push_back(BytecodeField{node.text, node.fieldPointers.front(), std::move(path), node.fieldIndices.front(), topLevel});
            return static_cast<uint16_t>(fields.size() - 1);
        }

        [[nodiscard]] static std::optional<BytecodeConstant> renderValue(const IrNode& node) {
            const std::string_view text{node.text};

            if (node.kind == IrKind::Capture) {
                return BytecodeConstant{
                    "::akkaradb::query::bytecode::valueFrom(__value_${index})",
                    node.text,
                    true
                };
            }

            if (node.kind != IrKind::Literal || text == "nullptr") { return std::nullopt; }
            if (text == "true") {
                return BytecodeConstant{"::akkaradb::query::bytecode::Value::boolean(true)", node.text, false};
            }
            if (text == "false") {
                return BytecodeConstant{"::akkaradb::query::bytecode::Value::boolean(false)", node.text, false};
            }
            if (!text.empty() && (text.front() == '"' || text.starts_with("R\"") || text.starts_with("u8\"") || text.starts_with("L\"") || text.starts_with("u\"") || text.starts_with("U\""))) {
                return BytecodeConstant{"::akkaradb::query::bytecode::Value::string(" + node.text + ")", node.text, false};
            }
            if (text.find('.') != std::string_view::npos || text.find('e') != std::string_view::npos || text.find('E') != std::string_view::npos) {
                return BytecodeConstant{"::akkaradb::query::bytecode::Value::floating(" + node.text + ")", node.text, false};
            }
            const bool unsignedLiteral = text.find('u') != std::string_view::npos || text.find('U') != std::string_view::npos;
            if (unsignedLiteral) {
                return BytecodeConstant{"::akkaradb::query::bytecode::Value::uinteger(" + node.text + ")", node.text, false};
            }
            return BytecodeConstant{"::akkaradb::query::bytecode::Value::integer(" + node.text + ")", node.text, false};
        }

        [[nodiscard]] std::optional<uint16_t> addConstant(const IrNode& node) {
            auto value = renderValue(node);
            if (!value.has_value()) {
                fail("bytecode literal/capture is not supported yet");
                return std::nullopt;
            }

            const auto existing = std::find_if(constants.begin(), constants.end(), [&](const BytecodeConstant& constant) {
                return constant.sourceExpression == value->sourceExpression
                    && constant.valueExpression == value->valueExpression
                    && constant.dynamic == value->dynamic;
            });
            if (existing != constants.end()) { return static_cast<uint16_t>(std::distance(constants.begin(), existing)); }
            if (constants.size() >= std::numeric_limits<uint16_t>::max()) {
                fail("bytecode query has too many constants");
                return std::nullopt;
            }
            constants.push_back(std::move(*value));
            return static_cast<uint16_t>(constants.size() - 1);
        }

        [[nodiscard]] std::optional<uint16_t> addBooleanConstant(bool value) {
            IrNode node{.kind = IrKind::Literal, .text = value ? "true" : "false", .rowDependent = false};
            return addConstant(node);
        }

        bool addCustomOpcode(const IrNode& node) {
            if (node.customOpcodeBindingExpression.empty() || node.customOpcodeExpression.empty()) {
                fail("custom opcode registration metadata is incomplete");
                return false;
            }
            const auto existing = std::find_if(customOpcodes.begin(), customOpcodes.end(), [&](const BytecodeCustomOpcode& binding) {
                return binding.bindingExpression == node.customOpcodeBindingExpression;
            });
            if (existing != customOpcodes.end()) { return true; }
            if (customOpcodes.size() >= std::numeric_limits<uint16_t>::max()) {
                fail("bytecode query has too many custom opcode bindings");
                return false;
            }
            customOpcodes.push_back(BytecodeCustomOpcode{node.customOpcodeBindingExpression});
            return true;
        }

        [[nodiscard]] static std::optional<std::string> opcodeForCompare(std::string_view op) {
            if (op == "eq") { return "Eq"; }
            if (op == "ne") { return "Ne"; }
            if (op == "lt") { return "Lt"; }
            if (op == "le") { return "Le"; }
            if (op == "gt") { return "Gt"; }
            if (op == "ge") { return "Ge"; }
            return std::nullopt;
        }

        [[nodiscard]] static std::optional<std::string> opcodeForArithmetic(std::string_view op) {
            if (op == "add") { return "Add"; }
            if (op == "sub") { return "Sub"; }
            if (op == "mul") { return "Mul"; }
            if (op == "div") { return "Div"; }
            if (op == "mod") { return "Mod"; }
            return std::nullopt;
        }

        [[nodiscard]] static std::optional<std::string> hintOpForCompare(std::string_view op, bool reversed) {
            if (op == "eq") { return "Eq"; }
            if (op == "starts_with" && !reversed) { return "StartsWith"; }
            if (op == "ne") { return std::nullopt; }
            if (!reversed) {
                if (op == "lt") { return "Lt"; }
                if (op == "le") { return "Le"; }
                if (op == "gt") { return "Gt"; }
                if (op == "ge") { return "Ge"; }
            }
            else {
                if (op == "lt") { return "Gt"; }
                if (op == "le") { return "Ge"; }
                if (op == "gt") { return "Lt"; }
                if (op == "ge") { return "Le"; }
            }
            return std::nullopt;
        }

        [[nodiscard]] static int hintScore(std::string_view op) {
            if (op == "Eq") { return 100; }
            if (op == "StartsWith") { return 95; }
            if (op == "Gt" || op == "Ge" || op == "Lt" || op == "Le") { return 80; }
            return 0;
        }

        void maybeAddPlanHint(const IrNode& node) {
            if (node.kind != IrKind::Compare || node.children.size() != 2) { return; }
            const IrNode* column = nullptr;
            const IrNode* value = nullptr;
            bool reversed = false;
            if (node.children[0].kind == IrKind::Column && (node.children[1].kind == IrKind::Literal || node.children[1].kind == IrKind::Capture)) {
                column = &node.children[0];
                value = &node.children[1];
            }
            else if ((node.children[0].kind == IrKind::Literal || node.children[0].kind == IrKind::Capture) && node.children[1].kind == IrKind::Column) {
                column = &node.children[1];
                value = &node.children[0];
                reversed = true;
            }
            else { return; }

            auto hintOp = hintOpForCompare(node.op, reversed);
            if (!hintOp.has_value()) { return; }
            auto field = addField(*column);
            auto constant = addConstant(*value);
            if (!field.has_value() || !constant.has_value()) { return; }
            hints.push_back(BytecodePlanHint{*hintOp, *field, *constant, hintScore(*hintOp)});
        }

        bool compile(const IrNode& node, bool allowPlanHints = true) {
            if (!ok()) { return false; }

            switch (node.kind) {
                case IrKind::Column: {
                    auto field = addField(node);
                    if (!field.has_value()) { return false; }
                    emitOp("LoadField");
                    emitU16(*field);
                    return true;
                }
                case IrKind::Capture:
                    [[fallthrough]];
                case IrKind::Literal: {
                    auto constant = addConstant(node);
                    if (!constant.has_value()) { return false; }
                    emitOp("PushConst");
                    emitU16(*constant);
                    return true;
                }
                case IrKind::CustomCall: {
                    if (node.children.size() != node.customOpcodeArity) {
                        fail("custom opcode call arity does not match its registration: " + node.text);
                        return false;
                    }
                    if (!addCustomOpcode(node)) { return false; }
                    for (const auto& child : node.children) {
                        if (!compile(child, allowPlanHints)) { return false; }
                    }
                    emitOp("CallCustom");
                    emitU16Expr(node.customOpcodeExpression);
                    return true;
                }
                case IrKind::Unsupported:
                    fail("unsupported row-dependent expression cannot be emitted as bytecode yet: " + node.text);
                    return false;
                case IrKind::Unary:
                    if (node.op != "not" || node.children.size() != 1) {
                        fail("unsupported unary bytecode operation");
                        return false;
                    }
                    if (!compile(node.children[0], false)) { return false; }
                    emitOp("Not");
                    return true;
                case IrKind::Arithmetic: {
                    if (node.children.size() != 2) {
                        fail("malformed bytecode arithmetic expression");
                        return false;
                    }
                    auto op = opcodeForArithmetic(node.op);
                    if (!op.has_value()) {
                        fail("unsupported arithmetic bytecode operation");
                        return false;
                    }
                    if (!compile(node.children[0], allowPlanHints) || !compile(node.children[1], allowPlanHints)) { return false; }
                    emitOp(*op);
                    return true;
                }
                case IrKind::Logical: {
                    if (node.children.size() != 2 || (node.op != "and" && node.op != "or")) {
                        fail("unsupported logical bytecode operation");
                        return false;
                    }
                    const bool isAnd = node.op == "and";
                    const bool childHintsAllowed = allowPlanHints && isAnd;
                    if (!compile(node.children[0], childHintsAllowed)) { return false; }
                    const size_t jumpToConstant = emitJump(isAnd ? "JumpIfFalse" : "JumpIfTrue");
                    if (!compile(node.children[1], childHintsAllowed)) { return false; }
                    const size_t jumpToEnd = emitJump("Jump");
                    const size_t constantOffset = currentOffset();
                    auto constant = addBooleanConstant(!isAnd);
                    if (!constant.has_value()) { return false; }
                    emitOp("PushConst");
                    emitU16(*constant);
                    const size_t endOffset = currentOffset();
                    patchU16(jumpToConstant, constantOffset);
                    patchU16(jumpToEnd, endOffset);
                    if (!ok()) { return false; }
                    return true;
                }
                case IrKind::Compare: {
                    if (node.children.size() != 2) {
                        fail("malformed bytecode comparison");
                        return false;
                    }
                    auto op = opcodeForCompare(node.op);
                    if (!op.has_value()) {
                        if (node.op == "starts_with") { op = "StartsWith"; }
                        else if (node.op == "ends_with") { op = "EndsWith"; }
                        else if (node.op == "contains") { op = "Contains"; }
                        else if (node.op == "like") { op = "Like"; }
                    }
                    if (!op.has_value()) {
                        fail("bytecode method comparison lowering is not implemented yet: " + node.op);
                        return false;
                    }
                    if (allowPlanHints) { maybeAddPlanHint(node); }
                    if (!ok()) { return false; }
                    if (!compile(node.children[0], allowPlanHints) || !compile(node.children[1], allowPlanHints)) { return false; }
                    emitOp(*op);
                    return true;
                }
            }
            fail("unknown bytecode IR node");
            return false;
        }
    };

    [[nodiscard]] static std::optional<std::string> entityTypeText(const clang::LambdaExpr* lambda) {
        if (lambda == nullptr || lambda->getCallOperator() == nullptr || lambda->getCallOperator()->getNumParams() != 1) {
            return std::nullopt;
        }
        clang::QualType type = lambda->getCallOperator()->getParamDecl(0)->getType().getNonReferenceType().getUnqualifiedType();
        if (type.isNull() || type->isDependentType() || type->isUndeducedAutoType()) { return std::nullopt; }
        if (const auto* record = type->getAsCXXRecordDecl()) {
            std::string name = record->getQualifiedNameAsString();
            if (!name.empty()) { return name; }
        }
        std::string name = type.getAsString();
        if (name.empty() || name.find("auto") != std::string::npos) { return std::nullopt; }
        return name;
    }

    [[nodiscard]] static std::string joinComma(const std::vector<std::string>& values) {
        std::string out;
        for (size_t i = 0; i < values.size(); ++i) {
            if (i != 0) { out.append(", "); }
            out.append(values[i]);
        }
        return out;
    }

    [[nodiscard]] std::optional<std::string> renderBytecodeDescriptor(
        const clang::LambdaExpr* lambda,
        const IrNode& node,
        std::string& reason,
        bool allowHostFallback
    ) const {
        auto entity = entityTypeText(lambda);
        if (!entity.has_value()) {
            reason = "bytecode rewrite requires a non-generic typed lambda parameter";
            return std::nullopt;
        }

        BytecodeBuild build;
        if (!build.compile(node) || !build.ok()) {
            if (!allowHostFallback) {
                reason = build.reason.empty() ? "lambda cannot be emitted as composable bytecode" : build.reason;
                return std::nullopt;
            }
            return renderHostCallDescriptor(lambda, *entity, reason);
        }
        build.emitOp("Return");
        std::stable_sort(build.hints.begin(), build.hints.end(), [](const BytecodePlanHint& lhs, const BytecodePlanHint& rhs) {
            return lhs.score > rhs.score;
        });

        std::ostringstream out;
        out << "([&]() { ";
        out << "using __AkkEntity = " << *entity << "; ";

        out << "static constexpr ::std::array<::akkaradb::query::bytecode::CustomOpcodeBinding, " << build.customOpcodes.size() << "> __akk_custom_opcodes{";
        for (size_t i = 0; i < build.customOpcodes.size(); ++i) {
            if (i != 0) { out << ", "; }
            out << build.customOpcodes[i].bindingExpression;
        }
        out << "}; ";
        out << "static constexpr ::std::array<const void*, " << build.customOpcodes.size() << "> __akk_custom_opcode_captures{";
        for (size_t i = 0; i < build.customOpcodes.size(); ++i) {
            if (i != 0) { out << ", "; }
            out << "nullptr";
        }
        out << "}; ";

        out << "static constexpr ::std::array<::std::uint8_t, " << build.code.size() << "> __akk_code{";
        out << joinComma(build.code);
        out << "}; ";

        const bool dynamicConstants = build.hasDynamicConstants();
        if (!dynamicConstants) {
            out << "static constexpr ::std::array<::akkaradb::query::bytecode::Value, " << build.constants.size() << "> __akk_constants{";
            for (size_t i = 0; i < build.constants.size(); ++i) {
                if (i != 0) { out << ", "; }
                out << build.constants[i].valueExpression;
            }
            out << "}; ";
        }

        out << "static constexpr ::std::array<::akkaradb::query::bytecode::FieldBinding<__AkkEntity>, " << build.fields.size() << "> __akk_fields{";
        for (size_t i = 0; i < build.fields.size(); ++i) {
            if (i != 0) { out << ", "; }
            const auto& field = build.fields[i];
            if (field.topLevel) {
                out << "::akkaradb::query::bytecode::makeTopLevelFieldBinding<&" << field.pointer << ", " << field.index << ">(\"" << field.name << "\")";
            }
            else {
                out << "::akkaradb::query::bytecode::FieldBinding<__AkkEntity>{";
                out << "+[](const __AkkEntity& __akk_entity) -> ::akkaradb::query::bytecode::Value { return ::akkaradb::query::bytecode::valueFrom(__akk_entity" << field.pathExpression << "); }, ";
                out << "nullptr, \"" << field.name << "\", nullptr, false}";
            }
        }
        out << "}; ";

        out << "static constexpr ::std::array<::akkaradb::query::bytecode::PlanHint, " << build.hints.size() << "> __akk_hints{";
        for (size_t i = 0; i < build.hints.size(); ++i) {
            if (i != 0) { out << ", "; }
            const auto& hint = build.hints[i];
            out << "::akkaradb::query::bytecode::PlanHint{::akkaradb::query::bytecode::PlanHintOp::" << hint.op << ", "
                << hint.field << ", " << hint.constant << "}";
        }
        out << "}; ";

        if (dynamicConstants) {
            out << "struct __AkkCaptures { ";
            for (size_t i = 0; i < build.constants.size(); ++i) {
                const auto& constant = build.constants[i];
                if (!constant.dynamic) { continue; }
                out << "::std::remove_cvref_t<decltype(" << constant.sourceExpression << ")> __value_" << i << "; ";
            }
            out << "::std::array<::akkaradb::query::bytecode::Value, " << build.constants.size() << "> constants; ";
            out << "__AkkCaptures(";
            bool first = true;
            for (size_t i = 0; i < build.constants.size(); ++i) {
                const auto& constant = build.constants[i];
                if (!constant.dynamic) { continue; }
                if (!first) { out << ", "; }
                first = false;
                out << "::std::remove_cvref_t<decltype(" << constant.sourceExpression << ")> __arg_" << i;
            }
            out << ") : ";
            first = true;
            for (size_t i = 0; i < build.constants.size(); ++i) {
                const auto& constant = build.constants[i];
                if (!constant.dynamic) { continue; }
                if (!first) { out << ", "; }
                first = false;
                out << "__value_" << i << "(::std::move(__arg_" << i << "))";
            }
            if (!first) { out << ", "; }
            out << "constants{";
            for (size_t i = 0; i < build.constants.size(); ++i) {
                if (i != 0) { out << ", "; }
                const auto& constant = build.constants[i];
                if (constant.dynamic) { out << "::akkaradb::query::bytecode::valueFrom(__value_" << i << ")"; }
                else { out << constant.valueExpression; }
            }
            out << "} {} }; ";
            out << "auto __akk_captures = ::std::make_shared<__AkkCaptures>(";
            first = true;
            for (size_t i = 0; i < build.constants.size(); ++i) {
                const auto& constant = build.constants[i];
                if (!constant.dynamic) { continue; }
                if (!first) { out << ", "; }
                first = false;
                out << constant.sourceExpression;
            }
            out << "); ";
        }

        out << "return ::akkaradb::query::bytecode::CompiledQueryDescriptor<__AkkEntity>{";
        out << ".code = __akk_code, ";
        out << ".constants = " << (dynamicConstants ? "__akk_captures->constants" : "__akk_constants") << ", ";
        out << ".fields = __akk_fields, ";
        out << ".hostCalls = {}, ";
        out << ".customOpcodes = __akk_custom_opcodes, ";
        out << ".planHints = __akk_hints, ";
        out << ".customOpcodeCaptures = __akk_custom_opcode_captures";
        if (dynamicConstants) { out << ", .captures = __akk_captures.get(), .capturesOwner = __akk_captures"; }
        else { out << ", .captures = nullptr, .capturesOwner = {}"; }
        out << "}; ";
        out << "}())";
        return out.str();
    }

    [[nodiscard]] std::optional<std::string> renderHostCallDescriptor(const clang::LambdaExpr* lambda, const std::string& entity, std::string& reason) const {
        const std::string lambdaSource = sourceText(context_.ast, lambda == nullptr ? clang::SourceRange{} : lambda->getSourceRange());
        if (lambdaSource.empty()) {
            reason = "bytecode host-call fallback could not recover lambda source";
            return std::nullopt;
        }

        std::ostringstream out;
        out << "([&]() { ";
        out << "using __AkkEntity = " << entity << "; ";
        out << "auto __akk_lambda = " << lambdaSource << "; ";
        out << "struct __AkkCaptures { ::std::remove_cvref_t<decltype(__akk_lambda)> lambda; explicit __AkkCaptures(::std::remove_cvref_t<decltype(__akk_lambda)> value) : lambda(::std::move(value)) {} }; ";
        out << "auto __akk_captures = ::std::make_shared<__AkkCaptures>(::std::move(__akk_lambda)); ";
        out << "static constexpr ::std::array<::std::uint8_t, 4> __akk_code{";
        out << "static_cast<::std::uint8_t>(::akkaradb::query::bytecode::Opcode::HostCallBool), 0, 0, ";
        out << "static_cast<::std::uint8_t>(::akkaradb::query::bytecode::Opcode::Return)}; ";
        out << "static constexpr ::std::array<::akkaradb::query::bytecode::HostCallThunk<__AkkEntity>, 1> __akk_host_calls{";
        out << "+[](const __AkkEntity& __akk_entity, const void* __akk_raw) -> bool { return static_cast<const __AkkCaptures*>(__akk_raw)->lambda(__akk_entity); }}; ";
        out << "return ::akkaradb::query::bytecode::CompiledQueryDescriptor<__AkkEntity>{";
        out << ".code = __akk_code, .constants = {}, .fields = {}, .hostCalls = __akk_host_calls, .customOpcodes = {}, .planHints = {}, ";
        out << ".captures = __akk_captures.get(), .capturesOwner = __akk_captures}; ";
        out << "}())";
        return out.str();
    }

    [[nodiscard]] IrNode parseExpr(const clang::Expr* expr) const {
        expr = ignoreNoise(expr);
        if (expr == nullptr) { return unsupported("<null>"); }

        if (auto field = tryFieldPath(expr, context_)) {
            IrNode out{.kind = IrKind::Column, .text = std::move(field->display), .rowDependent = true, .crossesRef = field->crossesRef};
            out.fieldNames = std::move(field->names);
            out.fieldPointers = std::move(field->pointers);
            out.fieldIndices = std::move(field->indices);
            return out;
        }

        if (const auto* bin = llvm::dyn_cast<clang::BinaryOperator>(expr)) {
            if (bin->getOpcode() == clang::BO_LAnd || bin->getOpcode() == clang::BO_LOr) {
                return logical(bin->getOpcode() == clang::BO_LAnd ? "and" : "or", bin->getLHS(), bin->getRHS());
            }
            if (auto op = binaryCompareOp(bin->getOpcode())) { return compare(*op, bin->getLHS(), bin->getRHS()); }
            if (auto op = binaryArithmeticOp(bin->getOpcode())) { return arithmetic(*op, bin->getLHS(), bin->getRHS()); }
        }

        if (const auto* opCall = llvm::dyn_cast<clang::CXXOperatorCallExpr>(expr)) {
            const auto op = opCall->getOperator();
            if ((op == clang::OO_AmpAmp || op == clang::OO_PipePipe) && opCall->getNumArgs() == 2) {
                return logical(op == clang::OO_AmpAmp ? "and" : "or", opCall->getArg(0), opCall->getArg(1));
            }
            if (auto compareOp = overloadedCompareOp(op); compareOp && opCall->getNumArgs() == 2) {
                return compare(*compareOp, opCall->getArg(0), opCall->getArg(1));
            }
            if (auto arithmeticOp = overloadedArithmeticOp(op); arithmeticOp && opCall->getNumArgs() == 2) {
                return arithmetic(*arithmeticOp, opCall->getArg(0), opCall->getArg(1));
            }
            if (op == clang::OO_Exclaim && opCall->getNumArgs() == 1) {
                IrNode out{.kind = IrKind::Unary, .op = "not", .rowDependent = dependsOnRow(expr, context_)};
                out.children.push_back(parseExpr(opCall->getArg(0)));
                return out;
            }
        }

        if (const auto* unary = llvm::dyn_cast<clang::UnaryOperator>(expr)) {
            if (unary->getOpcode() == clang::UO_LNot) {
                IrNode out{.kind = IrKind::Unary, .op = "not", .rowDependent = dependsOnRow(expr, context_)};
                out.children.push_back(parseExpr(unary->getSubExpr()));
                return out;
            }
        }

        if (const auto* call = llvm::dyn_cast<clang::CXXMemberCallExpr>(expr)) {
            if (const auto* callee = call->getMethodDecl()) {
                if (auto op = methodQueryOp(callee->getName())) {
                    IrNode out{.kind = IrKind::Compare, .op = *op, .rowDependent = dependsOnRow(expr, context_)};
                    out.children.push_back(parseExpr(call->getImplicitObjectArgument()));
                    if (call->getNumArgs() == 1) { out.children.push_back(parseExpr(call->getArg(0))); }
                    else { out.children.push_back(unsupported("<missing argument>")); }
                    return out;
                }
            }
        }

        if (const auto* call = llvm::dyn_cast<clang::CallExpr>(expr)) {
            if (const auto* callee = ignoreNoise(call->getCallee())) {
                if (const auto* member = llvm::dyn_cast<clang::MemberExpr>(callee)) {
                    if (auto op = methodQueryOp(member->getMemberNameInfo().getAsString())) {
                        IrNode out{.kind = IrKind::Compare, .op = *op, .rowDependent = dependsOnRow(expr, context_)};
                        out.children.push_back(parseExpr(member->getBase()));
                        if (call->getNumArgs() == 1) { out.children.push_back(parseExpr(call->getArg(0))); }
                        else { out.children.push_back(unsupported("<missing argument>")); }
                        return out;
                    }
                }
                if (const auto* member = llvm::dyn_cast<clang::CXXDependentScopeMemberExpr>(callee)) {
                    if (auto op = methodQueryOp(member->getMemberNameInfo().getAsString())) {
                        IrNode out{.kind = IrKind::Compare, .op = *op, .rowDependent = dependsOnRow(expr, context_)};
                        out.children.push_back(parseExpr(member->getBase()));
                        if (call->getNumArgs() == 1) { out.children.push_back(parseExpr(call->getArg(0))); }
                        else { out.children.push_back(unsupported("<missing argument>")); }
                        return out;
                    }
                }
            }
            if (const auto* opcode = findCustomOpcode(call->getDirectCallee())) {
                IrNode out{.kind = IrKind::CustomCall, .op = "custom", .text = sourceText(context_.ast, expr), .rowDependent = dependsOnRow(expr, context_)};
                out.customOpcodeBindingExpression = opcode->bindingExpression;
                out.customOpcodeExpression = opcode->opcodeExpression;
                out.customOpcodeArity = opcode->arity;
                if (call->getNumArgs() != opcode->arity) {
                    out.kind = IrKind::Unsupported;
                    out.crossesRef = false;
                    return out;
                }
                for (const clang::Expr* arg : call->arguments()) { out.children.push_back(parseExpr(arg)); }
                return out;
            }
        }

        if (isLiteralExpr(expr)) {
            return IrNode{.kind = IrKind::Literal, .text = sourceText(context_.ast, expr), .rowDependent = false};
        }

        if (!dependsOnRow(expr, context_)) {
            return IrNode{.kind = IrKind::Capture, .text = sourceText(context_.ast, expr), .rowDependent = false};
        }

        std::string text = sourceText(context_.ast, expr);
        IrNode out = unsupported(text);
        out.crossesRef = text.find("->") != std::string::npos;
        return out;
    }

    [[nodiscard]] IrNode logical(std::string op, const clang::Expr* lhs, const clang::Expr* rhs) const {
        IrNode out{.kind = IrKind::Logical, .op = std::move(op), .rowDependent = true};
        out.children.push_back(parseExpr(lhs));
        out.children.push_back(parseExpr(rhs));
        out.rowDependent = out.children[0].rowDependent || out.children[1].rowDependent;
        return out;
    }

    [[nodiscard]] IrNode compare(std::string op, const clang::Expr* lhs, const clang::Expr* rhs) const {
        IrNode out{.kind = IrKind::Compare, .op = std::move(op), .rowDependent = true};
        out.children.push_back(parseExpr(lhs));
        out.children.push_back(parseExpr(rhs));
        out.rowDependent = out.children[0].rowDependent || out.children[1].rowDependent;
        return out;
    }

    [[nodiscard]] IrNode arithmetic(std::string op, const clang::Expr* lhs, const clang::Expr* rhs) const {
        IrNode out{.kind = IrKind::Arithmetic, .op = std::move(op), .rowDependent = true};
        out.children.push_back(parseExpr(lhs));
        out.children.push_back(parseExpr(rhs));
        out.rowDependent = out.children[0].rowDependent || out.children[1].rowDependent;
        return out;
    }

    [[nodiscard]] static IrNode unsupported(std::string text) {
        return IrNode{.kind = IrKind::Unsupported, .text = std::move(text), .rowDependent = true};
    }

    [[nodiscard]] static bool isColumn(const IrNode& node) noexcept { return node.kind == IrKind::Column; }

    [[nodiscard]] static bool isValue(const IrNode& node) noexcept {
        return node.kind == IrKind::Literal || node.kind == IrKind::Capture;
    }

    [[nodiscard]] static int compareScore(std::string_view op) noexcept {
        if (op == "eq") { return 100; }
        if (op == "starts_with" || op == "like") { return 95; }
        if (op == "gt" || op == "ge" || op == "lt" || op == "le") { return 80; }
        if (op == "ne" || op == "contains" || op == "ends_with") { return 10; }
        return 0;
    }

    [[nodiscard]] PlanSummary planNode(const IrNode& node) const {
        if (node.kind == IrKind::Logical && node.children.size() == 2) {
            const PlanSummary lhs = planNode(node.children[0]);
            const PlanSummary rhs = planNode(node.children[1]);
            if (node.op == "and") {
                if (!lhs.indexable()) { return rhs.indexable() ? PlanSummary{rhs.kind, rhs.score, rhs.source, true} : tablePlan(); }
                if (!rhs.indexable()) { return PlanSummary{lhs.kind, lhs.score, lhs.source, true}; }
                const PlanSummary& best = lhs.score >= rhs.score ? lhs : rhs;
                return PlanSummary{best.kind, best.score, best.source, true};
            }
            if (node.op == "or") {
                if (!lhs.indexable() || !rhs.indexable()) { return tablePlan(); }
                const int score = std::min({lhs.score, rhs.score, 70});
                return PlanSummary{PlanSummary::Kind::IndexUnion, score, lhs.source + " | " + rhs.source, true};
            }
        }

        if (node.kind == IrKind::Compare && node.children.size() == 2) {
            const IrNode& lhs = node.children[0];
            const IrNode& rhs = node.children[1];
            const IrNode* column = nullptr;
            if (isColumn(lhs) && isValue(rhs)) { column = &lhs; }
            else if (isValue(lhs) && isColumn(rhs)) { column = &rhs; }
            if (column == nullptr) { return tablePlan(); }

            const int score = compareScore(node.op);
            if (score <= 0) { return tablePlan(); }
            return PlanSummary{PlanSummary::Kind::IndexScan, score, column->text + " " + node.op, node.op != "eq"};
        }

        if (node.kind == IrKind::Unary && node.op == "not" && node.children.size() == 1) {
            PlanSummary child = planNode(node.children[0]);
            if (!child.indexable()) { return tablePlan(); }
            child.score = std::min(child.score, 10);
            child.residual = true;
            return child;
        }

        return tablePlan();
    }

    [[nodiscard]] static PlanSummary tablePlan() { return PlanSummary{}; }

    [[nodiscard]] static bool containsRefCrossing(const IrNode& node) {
        if (node.crossesRef) { return true; }
        for (const auto& child : node.children) {
            if (containsRefCrossing(child)) { return true; }
        }
        return false;
    }

    [[nodiscard]] static std::optional<std::string> renderQueryExpression(const IrNode& node) {
        switch (node.kind) {
            case IrKind::Column:
                return renderQueryColumn(node);
            case IrKind::Literal:
            case IrKind::Capture:
                return node.text;
            case IrKind::CustomCall:
                return std::nullopt;
            case IrKind::Unsupported:
                return std::nullopt;
            case IrKind::Arithmetic: {
                if (node.children.size() != 2) { return std::nullopt; }
                auto lhs = renderQueryExpression(node.children[0]);
                auto rhs = renderQueryExpression(node.children[1]);
                if (!lhs.has_value() || !rhs.has_value()) { return std::nullopt; }
                if (node.op == "add") { return "(" + *lhs + " + " + *rhs + ")"; }
                if (node.op == "sub") { return "(" + *lhs + " - " + *rhs + ")"; }
                if (node.op == "mul") { return "(" + *lhs + " * " + *rhs + ")"; }
                if (node.op == "div") { return "(" + *lhs + " / " + *rhs + ")"; }
                if (node.op == "mod") { return "(" + *lhs + " % " + *rhs + ")"; }
                return std::nullopt;
            }
            case IrKind::Unary: {
                if (node.op != "not" || node.children.size() != 1) { return std::nullopt; }
                auto child = renderQueryExpression(node.children[0]);
                if (!child.has_value()) { return std::nullopt; }
                return "!(" + *child + ")";
            }
            case IrKind::Logical: {
                if (node.children.size() != 2) { return std::nullopt; }
                auto lhs = renderQueryExpression(node.children[0]);
                auto rhs = renderQueryExpression(node.children[1]);
                if (!lhs.has_value() || !rhs.has_value()) { return std::nullopt; }
                const char* op = node.op == "and" ? " && " : node.op == "or" ? " || " : nullptr;
                if (op == nullptr) { return std::nullopt; }
                return "(" + *lhs + op + *rhs + ")";
            }
            case IrKind::Compare: {
                if (node.children.size() != 2) { return std::nullopt; }
                auto lhs = renderQueryExpression(node.children[0]);
                auto rhs = renderQueryExpression(node.children[1]);
                if (!lhs.has_value() || !rhs.has_value()) { return std::nullopt; }

                if (node.op == "eq") { return "(" + *lhs + " == " + *rhs + ")"; }
                if (node.op == "ne") { return "(" + *lhs + " != " + *rhs + ")"; }
                if (node.op == "gt") { return "(" + *lhs + " > " + *rhs + ")"; }
                if (node.op == "ge") { return "(" + *lhs + " >= " + *rhs + ")"; }
                if (node.op == "lt") { return "(" + *lhs + " < " + *rhs + ")"; }
                if (node.op == "le") { return "(" + *lhs + " <= " + *rhs + ")"; }
                if (node.op == "starts_with") { return "::akkaradb::query::startsWith(" + *lhs + ", " + *rhs + ")"; }
                if (node.op == "ends_with") { return "::akkaradb::query::endsWith(" + *lhs + ", " + *rhs + ")"; }
                if (node.op == "contains") { return "::akkaradb::query::contains(" + *lhs + ", " + *rhs + ")"; }
                if (node.op == "like") { return "::akkaradb::query::like(" + *lhs + ", " + *rhs + ")"; }
                return std::nullopt;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] static std::optional<std::string> renderQueryColumn(const IrNode& node) {
        if (node.fieldNames.empty()) { return std::nullopt; }
        if (node.crossesRef) { return std::nullopt; }
        if (node.fieldPointers.empty() || node.fieldPointers.front().empty()) { return std::nullopt; }

        std::string out = "::akkaradb::query::Column<&" + node.fieldPointers.front() + ">{}";
        if (node.fieldNames.size() == 1) { return out; }
        if (node.fieldPointers.size() != node.fieldNames.size()) { return std::nullopt; }

        for (size_t i = 1; i < node.fieldNames.size(); ++i) {
            if (node.fieldPointers[i].empty()) { return std::nullopt; }
            out = "::akkaradb::query::field<&" + node.fieldPointers[i] + ">(" + out + ")";
        }
        return out;
    }

    static void printNode(const IrNode& node) {
        switch (node.kind) {
            case IrKind::Column:
                llvm::errs() << "col(" << node.text << ')';
                return;
            case IrKind::Literal:
                llvm::errs() << "lit(" << node.text << ')';
                return;
            case IrKind::Capture:
                llvm::errs() << "capture(" << node.text << ')';
                return;
            case IrKind::CustomCall:
                llvm::errs() << "custom(" << node.text << ')';
                return;
            case IrKind::Unsupported:
                llvm::errs() << "unsupported(" << node.text << ')';
                return;
            case IrKind::Arithmetic:
                llvm::errs() << node.op << '(';
                for (size_t i = 0; i < node.children.size(); ++i) {
                    if (i != 0) { llvm::errs() << ", "; }
                    printNode(node.children[i]);
                }
                llvm::errs() << ')';
                return;
            case IrKind::Unary:
                llvm::errs() << node.op << '(';
                if (!node.children.empty()) { printNode(node.children[0]); }
                llvm::errs() << ')';
                return;
            case IrKind::Compare:
            case IrKind::Logical:
                llvm::errs() << node.op << '(';
                for (size_t i = 0; i < node.children.size(); ++i) {
                    if (i != 0) { llvm::errs() << ", "; }
                    printNode(node.children[i]);
                }
                llvm::errs() << ')';
                return;
        }
    }

    LambdaContext context_;
};

[[nodiscard]] const clang::LambdaExpr* findLambdaArgument(const clang::CallExpr* expr) {
    if (expr == nullptr) { return nullptr; }
    for (const clang::Expr* arg : expr->arguments()) {
        arg = ignoreNoise(arg);
        if (const auto* lambda = llvm::dyn_cast_or_null<clang::LambdaExpr>(arg)) { return lambda; }
    }
    return nullptr;
}

[[nodiscard]] bool lambdaHasTypedSingleParam(const clang::LambdaExpr* lambda) {
    if (lambda == nullptr) { return false; }
    const clang::CXXMethodDecl* call = lambda->getCallOperator();
    if (call == nullptr || call->getNumParams() != 1) { return false; }
    const clang::QualType type = call->getParamDecl(0)->getType();
    if (type.isNull()) { return false; }
    if (type->isDependentType()) { return false; }
    return type.getAsString().find("auto") == std::string::npos;
}

[[nodiscard]] bool containsAkkaraPackedTable(std::string_view text) {
    return text.find("akkaradb::PackedTable") != std::string_view::npos;
}

[[nodiscard]] bool containsAkkaraQueryView(std::string_view text) {
    return containsAkkaraPackedTable(text) && text.find("QueryView") != std::string_view::npos;
}

[[nodiscard]] std::string canonicalTypeName(clang::QualType type) {
    if (type.isNull()) { return {}; }
    return type.getCanonicalType().getUnqualifiedType().getAsString();
}

[[nodiscard]] bool isAkkaraQueryApiCall(const clang::CallExpr* expr, const clang::FunctionDecl* callee) {
    if (callee == nullptr) { return false; }

    const llvm::StringRef name = callee->getName();
    if (name != "query" && name != "where") { return false; }

    if (const auto* method = llvm::dyn_cast<clang::CXXMethodDecl>(callee)) {
        const std::string parentName = method->getParent()->getQualifiedNameAsString();
        if (name == "query" && containsAkkaraPackedTable(parentName)) { return true; }
        if (name == "where" && containsAkkaraQueryView(parentName)) { return true; }
    }

    if (const auto* memberCall = llvm::dyn_cast_or_null<clang::CXXMemberCallExpr>(expr)) {
        const clang::Expr* object = memberCall->getImplicitObjectArgument();
        const std::string objectType = object == nullptr ? std::string{} : canonicalTypeName(object->getType());
        if (name == "query" && containsAkkaraPackedTable(objectType)) { return true; }
        if (name == "where" && containsAkkaraQueryView(objectType)) { return true; }
    }

    return false;
}

[[nodiscard]] bool containsAkkaraBytecodeQueryView(std::string_view text) {
    return text.find("BytecodeQueryView") != std::string_view::npos;
}

[[nodiscard]] bool callNameIs(const clang::CallExpr* expr, llvm::StringRef expected) {
    const clang::FunctionDecl* callee = expr == nullptr ? nullptr : expr->getDirectCallee();
    return callee != nullptr && callee->getName() == expected;
}

[[nodiscard]] bool whereReceiverCanUseBytecodeDescriptor(const clang::CallExpr* expr) {
    const auto* memberCall = llvm::dyn_cast_or_null<clang::CXXMemberCallExpr>(expr);
    if (memberCall == nullptr) { return false; }

    const clang::Expr* object = ignoreNoise(memberCall->getImplicitObjectArgument());
    if (object == nullptr) { return false; }

    if (containsAkkaraBytecodeQueryView(canonicalTypeName(object->getType()))) { return true; }

    const auto* objectCall = llvm::dyn_cast_or_null<clang::CallExpr>(object);
    if (objectCall == nullptr || !callNameIs(objectCall, "query")) { return false; }
    return lambdaHasTypedSingleParam(findLambdaArgument(objectCall));
}

class QueryCallVisitor final : public clang::RecursiveASTVisitor<QueryCallVisitor> {
public:
    QueryCallVisitor(clang::ASTContext& context, clang::Rewriter* rewriter, bool dump, bool rewrite)
        : context_{context}, rewriter_{rewriter}, dump_{dump}, rewrite_{rewrite} {}

    bool VisitVarDecl(clang::VarDecl* decl) {
        if (auto opcode = customOpcodeDeclFromVar(decl)) {
            customOpcodes_.push_back(std::move(*opcode));
            if (dump_) {
                llvm::errs() << "  custom-opcode: " << decl->getNameAsString()
                             << " arity=" << static_cast<unsigned>(customOpcodes_.back().arity) << '\n';
            }
        }
        if (rewrite_ && initializerCanBecomeBytecodeView(decl->getInit())) {
            bytecodeComposableViews_.insert(canonicalValueDecl(decl));
            if (dump_) {
                llvm::errs() << "  bytecode-view: " << decl->getNameAsString() << '\n';
            }
        }
        return true;
    }

    bool VisitCallExpr(clang::CallExpr* expr) {
        if (!dump_ && !rewrite_) { return true; }

        const clang::FunctionDecl* callee = expr->getDirectCallee();
        if (callee == nullptr) { return true; }

        if (!isAkkaraQueryApiCall(expr, callee)) { return true; }

        const auto name = callee->getNameAsString();
        if (rewrite_ && name == "where" && !whereReceiverCanUseBytecodeDescriptor(expr) && !whereReceiverCanBecomeBytecodeView(expr)) {
            if (dump_) { llvm::errs() << "  rewrite skipped: where receiver is not bytecode-composable\n"; }
            return true;
        }

        const clang::LambdaExpr* lambda = findLambdaArgument(expr);
        if (lambda == nullptr) { return true; }

        const auto loc = expr->getBeginLoc();
        if (!loc.isValid()) { return true; }

        const auto& sourceManager = context_.getSourceManager();
        if (dump_) {
            llvm::errs() << "[akkara-query] candidate call `" << name << "` at "
                         << sourceManager.getFilename(loc) << ':'
                         << sourceManager.getSpellingLineNumber(loc) << ':'
                         << sourceManager.getSpellingColumnNumber(loc) << '\n';
        }

        LambdaContext lambdaContext{context_, {}, &customOpcodes_};
        for (const clang::ParmVarDecl* param : lambda->getCallOperator()->parameters()) {
            lambdaContext.rowParams.push_back(param);
        }
        LambdaAnalyzer analyzer{std::move(lambdaContext)};
        const IrNode ir = analyzer.analyze(lambda);
        const PlanSummary plan = analyzer.plan(ir);
        if (dump_) { analyzer.print(ir, plan); }

        if (rewrite_ && rewriter_ != nullptr) {
            const RewriteResult result = analyzer.rewrite(lambda, ir, name != "where");
            if (result.ok) {
                const bool failed = rewriter_->ReplaceText(lambda->getSourceRange(), result.replacementSource);
                if (!failed) {
                    ++rewriteCount_;
                    if (dump_) { llvm::errs() << "  rewrite: " << result.replacementSource << '\n'; }
                }
                else {
                    llvm::errs() << "[akkara-query] rewrite failed at "
                                 << sourceManager.getFilename(loc) << ':'
                                 << sourceManager.getSpellingLineNumber(loc) << ':'
                                 << sourceManager.getSpellingColumnNumber(loc) << '\n';
                }
            }
            else {
                if (name == "where") {
                    if (dump_) {
                        llvm::errs() << "  rewrite skipped: where lambda is not composable bytecode: " << result.reason << '\n';
                    }
                }
                else {
                    llvm::errs() << "[akkara-query] rewrite skipped at "
                                 << sourceManager.getFilename(loc) << ':'
                                 << sourceManager.getSpellingLineNumber(loc) << ':'
                                 << sourceManager.getSpellingColumnNumber(loc) << ": "
                                 << result.reason << '\n';
                }
            }
        }

        return true;
    }

    [[nodiscard]] size_t rewriteCount() const noexcept { return rewriteCount_; }

private:
    clang::ASTContext& context_;
    clang::Rewriter* rewriter_ = nullptr;
    bool dump_;
    bool rewrite_;
    size_t rewriteCount_ = 0;
    std::vector<CustomOpcodeDecl> customOpcodes_;
    std::unordered_set<const clang::ValueDecl*> bytecodeComposableViews_;

    [[nodiscard]] static const clang::ValueDecl* canonicalValueDecl(const clang::ValueDecl* decl) {
        if (decl == nullptr) { return nullptr; }
        return llvm::dyn_cast_or_null<clang::ValueDecl>(decl->getCanonicalDecl());
    }

    [[nodiscard]] bool lambdaCanRewriteAsComposableWhereBytecode(const clang::LambdaExpr* lambda) const {
        if (!lambdaHasTypedSingleParam(lambda)) { return false; }
        LambdaContext lambdaContext{context_, {}, &customOpcodes_};
        for (const clang::ParmVarDecl* param : lambda->getCallOperator()->parameters()) {
            lambdaContext.rowParams.push_back(param);
        }
        LambdaAnalyzer analyzer{std::move(lambdaContext)};
        const IrNode ir = analyzer.analyze(lambda);
        return analyzer.rewrite(lambda, ir, false).ok;
    }

    [[nodiscard]] bool initializerCanBecomeBytecodeView(const clang::Expr* expr) const {
        expr = ignoreNoise(expr);
        if (expr == nullptr) { return false; }

        if (containsAkkaraBytecodeQueryView(canonicalTypeName(expr->getType()))) { return true; }

        if (const auto* ref = llvm::dyn_cast<clang::DeclRefExpr>(expr)) {
            return bytecodeComposableViews_.contains(canonicalValueDecl(ref->getDecl()));
        }

        const auto* call = llvm::dyn_cast<clang::CallExpr>(expr);
        if (call == nullptr) { return false; }
        const clang::FunctionDecl* callee = call->getDirectCallee();
        if (!isAkkaraQueryApiCall(call, callee)) { return false; }

        const llvm::StringRef name = callee->getName();
        const clang::LambdaExpr* lambda = findLambdaArgument(call);
        if (name == "query") { return lambdaHasTypedSingleParam(lambda); }
        if (name != "where") { return false; }
        if (!lambdaCanRewriteAsComposableWhereBytecode(lambda)) { return false; }
        return whereReceiverCanUseBytecodeDescriptor(call) || whereReceiverCanBecomeBytecodeView(call);
    }

    [[nodiscard]] bool whereReceiverCanBecomeBytecodeView(const clang::CallExpr* expr) const {
        const auto* memberCall = llvm::dyn_cast_or_null<clang::CXXMemberCallExpr>(expr);
        if (memberCall == nullptr) { return false; }
        return initializerCanBecomeBytecodeView(memberCall->getImplicitObjectArgument());
    }
};

class AkkaraQueryConsumer final : public clang::ASTConsumer {
public:
    AkkaraQueryConsumer(clang::CompilerInstance& compiler, bool dump, bool rewrite, bool rewriteInPlace, std::string rewriteOutput)
        : rewriter_{compiler.getSourceManager(), compiler.getLangOpts()},
          dump_{dump},
          rewrite_{rewrite},
          rewriteInPlace_{rewriteInPlace},
          rewriteOutput_{std::move(rewriteOutput)} {}

    void HandleTranslationUnit(clang::ASTContext& context) override {
        QueryCallVisitor visitor{context, rewrite_ ? &rewriter_ : nullptr, dump_, rewrite_};
        visitor.TraverseDecl(context.getTranslationUnitDecl());
        if (!rewrite_) { return; }
        if (rewriteInPlace_) { overwriteChangedFiles(visitor.rewriteCount()); }
        else { writeRewrittenMainFile(context, visitor.rewriteCount()); }
    }

private:
    void overwriteChangedFiles(size_t rewriteCount) {
        if (rewriter_.overwriteChangedFiles()) {
            llvm::errs() << "[akkara-query] failed to overwrite rewritten source files\n";
            return;
        }
        llvm::errs() << "[akkara-query] overwrote " << rewriteCount << " rewrite(s) in place\n";
    }

    void writeRewrittenMainFile(clang::ASTContext& context, size_t rewriteCount) {
        if (rewriteOutput_.empty()) {
            llvm::errs() << "[akkara-query] rewrite requested but rewrite-output=<path> was not provided\n";
            return;
        }

        std::error_code error;
        llvm::raw_fd_ostream out{rewriteOutput_, error, llvm::sys::fs::OF_Text};
        if (error) {
            llvm::errs() << "[akkara-query] cannot open rewrite output `" << rewriteOutput_ << "`: " << error.message() << '\n';
            return;
        }

        const clang::FileID mainFile = context.getSourceManager().getMainFileID();
        rewriter_.getEditBuffer(mainFile).write(out);
        llvm::errs() << "[akkara-query] wrote " << rewriteCount << " rewrite(s) to " << rewriteOutput_ << '\n';
    }

    clang::Rewriter rewriter_;
    bool dump_;
    bool rewrite_;
    bool rewriteInPlace_;
    std::string rewriteOutput_;
};

class AkkaraQueryAction final : public clang::PluginASTAction {
public:
    std::unique_ptr<clang::ASTConsumer> CreateASTConsumer(clang::CompilerInstance& compiler, llvm::StringRef) override {
        return std::make_unique<AkkaraQueryConsumer>(compiler, dump_, rewrite_, rewriteInPlace_, rewriteOutput_);
    }

    bool ParseArgs(const clang::CompilerInstance&, const std::vector<std::string>& args) override {
        for (const auto& arg : args) {
            if (arg == "dump" || arg == "-dump") {
                dump_ = true;
                continue;
            }
            if (arg == "rewrite" || arg == "-rewrite") {
                rewrite_ = true;
                continue;
            }
            if (arg == "rewrite-in-place" || arg == "-rewrite-in-place") {
                rewriteInPlace_ = true;
                continue;
            }
            if (llvm::StringRef argRef{arg}; argRef.starts_with("rewrite-output=") || argRef.starts_with("-rewrite-output=")) {
                const auto split = argRef.split('=');
                rewriteOutput_ = split.second.str();
                continue;
            }
            if (arg == "help" || arg == "-help" || arg == "--help") {
                printHelp();
                continue;
            }

            llvm::errs() << "akkara-query: unknown argument `" << arg << "`\n";
            printHelp();
            return false;
        }
        return true;
    }

    clang::PluginASTAction::ActionType getActionType() override {
        return clang::PluginASTAction::AddAfterMainAction;
    }

private:
    static void printHelp() {
        llvm::errs() << "akkara-query plugin arguments:\n"
                     << "  dump                  Print candidate query calls, lowered IR, and plan summaries.\n"
                     << "  rewrite               Rewrite supported query/where lambdas in a sidecar source file.\n"
                     << "  rewrite-in-place      Overwrite changed source files in place; intended only for copied work trees.\n"
                     << "  rewrite-output=<path> Output path for rewrite mode.\n"
                     << "  help                  Print this help text.\n";
    }

    bool dump_ = false;
    bool rewrite_ = false;
    bool rewriteInPlace_ = false;
    std::string rewriteOutput_;
};

} // namespace
} // namespace akkara::query_plugin

static clang::FrontendPluginRegistry::Add<akkara::query_plugin::AkkaraQueryAction>
    registerAkkaraQueryPlugin{"akkara-query", "AkkaraDB Native C++ query lambda rewriter skeleton"};
