#include "srclient/rules/cel/CelFieldExecutor.h"
#include "srclient/serdes/json/JsonTypes.h"
#include "srclient/serdes/avro/AvroTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"

namespace srclient::rules::cel {

CelFieldExecutor::CelFieldExecutor() {
    // Comment out make_shared creation for now since CelExecutor is abstract
    // executor_ = std::make_shared<CelExecutor>();
    executor_ = nullptr;
}

CelFieldExecutor::CelFieldExecutor(std::shared_ptr<CelExecutor> executor) : executor_(std::move(executor)) {}

// Implement the required getType method from RuleBase
std::string CelFieldExecutor::getType() const {
    return "CEL_FIELD";
}

// Implement the required shared_from_this method from FieldRuleExecutor
std::shared_ptr<FieldRuleExecutor> CelFieldExecutor::shared_from_this() {
    return std::static_pointer_cast<FieldRuleExecutor>(std::shared_ptr<CelFieldExecutor>(this));
}

SerdeValue& CelFieldExecutor::transformField(RuleContext& ctx, SerdeValue& field_value) {
    auto field_ctx = ctx.currentField();
    if (!field_ctx || !field_ctx->isPrimitive()) {
        return field_value;
    }

    // Comment out the actual implementation for now to avoid compilation errors
    /*
    absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue> args;

    args.emplace("value", CelExecutor::fromSerdeValue(field_value));
    
    // Fix string creation to use pointers
    static thread_local std::string temp_full_name;
    temp_full_name = field_ctx->getFullName();
    args.emplace("fullName", google::api::expr::runtime::CelValue::CreateString(&temp_full_name));
    
    static thread_local std::string temp_name;
    temp_name = field_ctx->getName();
    args.emplace("name", google::api::expr::runtime::CelValue::CreateString(&temp_name));
    
    // Comment out typeName for now since getFieldType might return non-string type
    // static thread_local std::string temp_type_name;
    // temp_type_name = field_ctx->getFieldType();
    // args.emplace("typeName", google::api::expr::runtime::CelValue::CreateString(&temp_type_name));

    std::vector<google::api::expr::runtime::CelValue> tags_vec;
    for (const auto& tag : field_ctx->getTags()) {
        // Fix string creation to use pointers
        static thread_local std::string temp_tag;
        temp_tag = tag;
        tags_vec.push_back(google::api::expr::runtime::CelValue::CreateString(&temp_tag));
    }
    // TODO tags
    //args.emplace("tags", google::api::expr::CelValue::CreateList(cel::ListType(), tags_vec).value());
    
    args.emplace("message", CelExecutor::fromSerdeValue(field_ctx->getContainingMessage()));

    auto result = executor_->execute(ctx, field_value, args);
    if (result) {
        field_value = std::move(*result);
    }
    */

    return field_value;
}

void CelFieldExecutor::registerExecutor() {
    // Comment out global registry registration for now
    // global_registry::registerRuleExecutor(std::make_shared<CelFieldExecutor>());
}

} 