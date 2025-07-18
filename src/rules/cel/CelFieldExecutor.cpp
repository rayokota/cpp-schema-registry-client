#include "srclient/rules/cel/CelFieldExecutor.h"
#include "srclient/serdes/json/JsonTypes.h"
#include "srclient/serdes/avro/AvroTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"

namespace srclient::rules::cel {

CelFieldExecutor::CelFieldExecutor() : executor_(std::make_shared<CelExecutor>()) {}

CelFieldExecutor::CelFieldExecutor(std::shared_ptr<CelExecutor> executor) : executor_(std::move(executor)) {}

SerdeValue& CelFieldExecutor::transformField(RuleContext& ctx, SerdeValue& field_value) {
    auto field_ctx = ctx.currentField();
    if (!field_ctx || !field_ctx->isPrimitive()) {
        return field_value;
    }

    absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue> args;

    args.emplace("value", CelExecutor::fromSerdeValue(field_value));
    args.emplace("fullName", google::api::expr::runtime::CelValue::CreateString(field_ctx->getFullName()));
    args.emplace("name", google::api::expr::runtime::CelValue::CreateString(field_ctx->getName()));
    args.emplace("typeName", google::api::expr::runtime::CelValue::CreateString(field_ctx->getFieldType()));

    std::vector<google::api::expr::runtime::CelValue> tags_vec;
    for (const auto& tag : field_ctx->getTags()) {
        tags_vec.push_back(google::api::expr::runtime::CelValue::CreateString(tag));
    }
    // TODO tags
    //args.emplace("tags", google::api::expr::CelValue::CreateList(cel::ListType(), tags_vec).value());
    
    args.emplace("message", CelExecutor::fromSerdeValue(field_ctx->getContainingMessage()));

    auto result = executor_->execute(ctx, field_value, args);
    if (result) {
        field_value = std::move(*result);
    }

    return field_value;
}

void CelFieldExecutor::registerExecutor() {
    // global_registry::registerRuleExecutor(std::make_shared<CelFieldExecutor>());
}

} 