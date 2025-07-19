#include "srclient/rules/cel/CelFieldExecutor.h"
#include "srclient/serdes/json/JsonTypes.h"
#include "srclient/serdes/avro/AvroTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"

namespace srclient::rules::cel {

CelFieldExecutor::CelFieldExecutor() {
    // Create a proper CelExecutor instance like Rust version
    executor_ = std::make_shared<CelExecutor>();
}

CelFieldExecutor::CelFieldExecutor(std::shared_ptr<CelExecutor> executor) : executor_(std::move(executor)) {}

// Implement the required getType method from RuleBase
std::string CelFieldExecutor::getType() const {
    return "CEL_FIELD";
}

SerdeValue& CelFieldExecutor::transformField(RuleContext& ctx, SerdeValue& field_value) {
    auto field_ctx = ctx.currentField();
    if (!field_ctx || !field_ctx->isPrimitive()) {
        return field_value;
    }

    // Only proceed if we have a valid executor
    if (!executor_) {
        return field_value;
    }

    absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue> args;

    // Add field value like Rust version
    args.emplace("value", CelExecutor::fromSerdeValue(field_value));
    
    // Add field context information like Rust version
    static thread_local std::string temp_full_name;
    temp_full_name = field_ctx->getFullName();
    args.emplace("fullName", google::api::expr::runtime::CelValue::CreateString(&temp_full_name));
    
    static thread_local std::string temp_name;
    temp_name = field_ctx->getName();
    args.emplace("name", google::api::expr::runtime::CelValue::CreateString(&temp_name));
    
    // Convert FieldType to string representation for typeName using existing function
    static thread_local std::string temp_type_name;
    temp_type_name = srclient::serdes::fieldTypeToString(field_ctx->getFieldType());
    args.emplace("typeName", google::api::expr::runtime::CelValue::CreateString(&temp_type_name));

    // Create CEL list for tags like Rust version
    static thread_local std::vector<google::api::expr::runtime::CelValue> tags_vec;
    static thread_local std::vector<std::string> temp_tags;
    tags_vec.clear();
    temp_tags.clear();
    
    for (const auto& tag : field_ctx->getTags()) {
        temp_tags.push_back(tag);
        tags_vec.push_back(google::api::expr::runtime::CelValue::CreateString(&temp_tags.back()));
    }
    
    // Create the CEL list for tags using proper API
    // TODO: Use proper CEL list creation when available - for now comment out
    // args.emplace("tags", google::api::expr::runtime::CelValue::CreateList(tags_vec));
    
    // Add containing message like Rust version
    args.emplace("message", CelExecutor::fromSerdeValue(field_ctx->getContainingMessage()));

    // Execute the CEL expression using the shared executor
    auto result = executor_->execute(ctx, field_value, args);
    if (result) {
        // TODO: Replace field_value with result when message replacement is implemented
        // For now, just return the original field_value
        return field_value;
    }

    return field_value;
}

void CelFieldExecutor::registerExecutor() {
    // Register this field executor with the global rule registry
    // This matches the Rust version: crate::serdes::rule_registry::register_rule_executor(CelFieldExecutor::new());
    // TODO: Implement when rule registry is available
    // RuleRegistry::instance().registerRuleExecutor(std::make_shared<CelFieldExecutor>());
}

} 