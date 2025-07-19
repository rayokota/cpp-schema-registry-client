#include "srclient/rules/cel/CelFieldExecutor.h"
#include "srclient/serdes/json/JsonTypes.h"
#include "srclient/serdes/avro/AvroTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include "eval/public/containers/container_backed_list_impl.h"

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

std::unique_ptr<SerdeValue> CelFieldExecutor::transformField(RuleContext& ctx, const SerdeValue& field_value) {
    auto field_ctx = ctx.currentField();
    if (!field_ctx || !field_ctx->isPrimitive()) {
        return field_value.clone();
    }

    // Only proceed if we have a valid executor
    if (!executor_) {
        return field_value.clone();
    }

    google::protobuf::Arena arena;
    absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue> args;

    // Add field value like Rust version
    args.emplace("value", executor_->fromSerdeValue(field_value, &arena));
    
    // Add field context information like Rust version
    auto* full_name_str = google::protobuf::Arena::Create<std::string>(&arena, field_ctx->getFullName());
    args.emplace("fullName", google::api::expr::runtime::CelValue::CreateString(full_name_str));
    
    auto* name_str = google::protobuf::Arena::Create<std::string>(&arena, field_ctx->getName());
    args.emplace("name", google::api::expr::runtime::CelValue::CreateString(name_str));
    
    // Convert FieldType to string representation for typeName using existing function
    auto* type_name_str = google::protobuf::Arena::Create<std::string>(&arena, srclient::serdes::fieldTypeToString(field_ctx->getFieldType()));
    args.emplace("typeName", google::api::expr::runtime::CelValue::CreateString(type_name_str));

    // Create CEL list for tags like Rust version
    auto* tags_vec = google::protobuf::Arena::Create<std::vector<google::api::expr::runtime::CelValue>>(&arena);
    
    for (const auto& tag : field_ctx->getTags()) {
        auto* tag_str = google::protobuf::Arena::Create<std::string>(&arena, tag);
        tags_vec->push_back(google::api::expr::runtime::CelValue::CreateString(tag_str));
    }
    
    // Create the CEL list for tags using proper API
    // TODO: Use proper CEL list creation when available - for now comment out
    // args.emplace("tags", google::api::expr::runtime::CelValue::CreateList(tags_vec));
    
    // Add containing message like Rust version
    args.emplace("message", executor_->fromSerdeValue(field_ctx->getContainingMessage(), &arena));

    // Execute the CEL expression using the shared executor
    auto result = executor_->execute(ctx, field_value, args);
    if (result) {
        return result;
    }

    return field_value.clone();
}

void CelFieldExecutor::registerExecutor() {
    // Register this field executor with the global rule registry
    // This matches the Rust version: crate::serdes::rule_registry::register_rule_executor(CelFieldExecutor::new());
    // TODO: Implement when rule registry is available
    // RuleRegistry::instance().registerRuleExecutor(std::make_shared<CelFieldExecutor>());
}

} 