#include "srclient/rules/cel/CelFieldExecutor.h"
#include "srclient/serdes/Serde.h"
#include "common/value.h"
#include <vector>

namespace srclient::rules::cel {

using namespace srclient::serdes;

CelFieldExecutor::CelFieldExecutor() 
    : executor_(std::make_unique<CelExecutor>()) {
}

CelFieldExecutor::CelFieldExecutor(std::unique_ptr<CelExecutor> executor)
    : executor_(std::move(executor)) {
    if (!executor_) {
        throw SerdeError("CelExecutor cannot be null");
    }
}

SerdeValue& CelFieldExecutor::transformField(RuleContext& ctx, SerdeValue& field_value) {
    auto field_ctx_opt = ctx.currentField();
    if (!field_ctx_opt.has_value()) {
        throw SerdeError("No field context available");
    }
    
    const auto& field_ctx = field_ctx_opt.value();
    
    // Only process primitive fields
    if (!field_ctx.isPrimitive()) {
        return field_value;
    }
    
    // Create field-specific arguments for CEL execution
    auto args = createFieldArguments(ctx, field_value);
    
    try {
        auto result = executor_->execute(ctx, field_value, args);
        // TODO: Update the field value with the result once proper value assignment is implemented
        // For now, just return the original field value
        return field_value;
    } catch (const std::exception& e) {
        throw SerdeError("CEL field execution failed: " + std::string(e.what()));
    }
}

absl::flat_hash_map<std::string, ::cel::Value> CelFieldExecutor::createFieldArguments(
    const RuleContext& ctx,
    const SerdeValue& field_value) {
    
    absl::flat_hash_map<std::string, ::cel::Value> args;
    
    auto field_ctx_opt = ctx.currentField();
    if (!field_ctx_opt.has_value()) {
        throw SerdeError("No field context available for argument creation");
    }
    
    const auto& field_ctx = field_ctx_opt.value();
    
    // TODO: Implement proper field argument creation once value manager access is resolved
    // For now, return empty arguments
    return args;
}

void CelFieldExecutor::registerExecutor() {
    auto executor = std::make_shared<CelFieldExecutor>();
    global_registry::registerRuleExecutor(executor);
}

} // namespace srclient::rules::cel 