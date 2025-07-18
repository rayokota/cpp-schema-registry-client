#include "srclient/rules/cel/CelExecutor.h"
#include "srclient/serdes/json/JsonTypes.h"
#include "srclient/serdes/avro/AvroTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include "srclient/serdes/Serde.h"
#include "absl/strings/string_view.h"
#include "absl/strings/str_split.h"
#include <regex>

namespace srclient::rules::cel {

using namespace srclient::serdes;

CelExecutor::CelExecutor() {
    auto runtime_result = CelLib::createDefaultRuntime();
    if (!runtime_result.ok()) {
        throw SerdeError("Failed to create CEL runtime: " + std::string(runtime_result.status().message()));
    }
    runtime_ = std::move(runtime_result.value());
}

CelExecutor::CelExecutor(std::unique_ptr<const ::cel::Runtime> runtime) 
    : runtime_(std::move(runtime)) {
    if (!runtime_) {
        throw SerdeError("CEL runtime cannot be null");
    }
}

SerdeValue& CelExecutor::transform(RuleContext& ctx, SerdeValue& msg) {
    // TODO: Implement CEL transformation once the API details are worked out
    // For now, return the original message unchanged
    return msg;
}

std::unique_ptr<SerdeValue> CelExecutor::execute(RuleContext& ctx, 
                               const SerdeValue& msg, 
                               const absl::flat_hash_map<std::string, ::cel::Value>& args) {
    // TODO: Implement CEL execution once the API details are worked out
    // For now, return a clone of the original message
    return msg.clone();
}

std::unique_ptr<SerdeValue> CelExecutor::executeRule(RuleContext& ctx,
                                   const SerdeValue& msg,
                                   const std::string& expr,
                                   const absl::flat_hash_map<std::string, ::cel::Value>& args) {
    // TODO: Implement CEL rule execution once the API details are worked out
    // For now, return a clone of the original message
    return msg.clone();
}

absl::StatusOr<google::api::expr::v1alpha1::ParsedExpr> CelExecutor::getOrCompileExpression(const std::string& expr) {
    absl::MutexLock lock(&cache_mutex_);
    
    auto it = expression_cache_.find(expr);
    if (it != expression_cache_.end()) {
        return it->second;
    }
    
    // TODO: Implement proper CEL expression parsing
    // For now, return a simple error
    return absl::UnimplementedError("CEL expression parsing not yet implemented");
}

::cel::Value CelExecutor::fromSerdeValue(const SerdeValue& value, ::cel::ValueManager& value_manager) {
    // TODO: Implement proper SerdeValue to CEL Value conversion
    // For now, return a placeholder null value
    return value_manager.GetNullValue();
}

std::unique_ptr<SerdeValue> CelExecutor::toSerdeValue(const SerdeValue& original, const ::cel::Value& cel_value) {
    // TODO: Implement proper CEL Value to SerdeValue conversion
    // For now, return a clone of the original
    return original.clone();
}

::cel::Value CelExecutor::fromJsonValue(const nlohmann::json& json, ::cel::ValueManager& value_manager) {
    // TODO: Implement proper JSON to CEL Value conversion
    // For now, return a placeholder null value
    return value_manager.GetNullValue();
}

nlohmann::json CelExecutor::toJsonValue(const nlohmann::json& original, const ::cel::Value& cel_value) {
    // TODO: Implement proper CEL Value to JSON conversion
    // For now, return the original JSON
    return original;
}

::cel::Value CelExecutor::fromAvroValue(const ::avro::GenericDatum& avro, ::cel::ValueManager& value_manager) {
    // TODO: Implement proper Avro to CEL Value conversion
    // For now, return a placeholder null value
    return value_manager.GetNullValue();
}

::avro::GenericDatum CelExecutor::toAvroValue(const ::avro::GenericDatum& original, const ::cel::Value& cel_value) {
    // TODO: Implement proper CEL Value to Avro conversion
    // For now, return the original Avro value
    return original;
}

::cel::Value CelExecutor::fromProtobufValue(const google::protobuf::Message& protobuf, ::cel::ValueManager& value_manager) {
    // TODO: Implement proper Protobuf to CEL Value conversion
    // For now, return a placeholder null value
    return value_manager.GetNullValue();
}

std::unique_ptr<google::protobuf::Message> CelExecutor::toProtobufValue(const google::protobuf::Message& original, const ::cel::Value& cel_value) {
    // TODO: Implement proper CEL Value to Protobuf conversion
    // For now, return a copy of the original message
    return std::unique_ptr<google::protobuf::Message>(original.New());
}

void CelExecutor::registerExecutor() {
    auto executor = std::make_shared<CelExecutor>();
    global_registry::registerRuleExecutor(executor);
}

} // namespace srclient::rules::cel 