#pragma once

#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeError.h"
#include "runtime/runtime.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "eval/public/cel_expression.h"
#include "runtime/runtime.h"
#include "nlohmann/json.hpp"
#include "avro/Generic.hh"
#include <memory>
#include <unordered_map>
#include <mutex>

namespace srclient::rules::cel {

using namespace srclient::serdes;

class CelExecutor : public RuleExecutor {
public:
    CelExecutor();
    explicit CelExecutor(std::unique_ptr<const google::api::expr::runtime::CelExpressionBuilder> runtime);
    std::unique_ptr<SerdeValue> transform(RuleContext& ctx, const SerdeValue& msg) override;
    
    std::string getType() const override;

    std::unique_ptr<SerdeValue> execute(RuleContext& ctx, 
                                       const SerdeValue& msg, 
                                       const absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue>& args);
                                       
    google::api::expr::runtime::CelValue fromSerdeValue(const SerdeValue& value, google::protobuf::Arena* arena);
    std::unique_ptr<SerdeValue> toSerdeValue(const SerdeValue& original, const google::api::expr::runtime::CelValue& cel_value);
    
    static void registerExecutor();

private:
    std::unique_ptr<const google::api::expr::runtime::CelExpressionBuilder> runtime_;
    
    std::unordered_map<std::string, std::shared_ptr<google::api::expr::runtime::CelExpression>> expression_cache_;
    mutable std::mutex cache_mutex_;

    absl::StatusOr<std::unique_ptr<google::api::expr::runtime::CelExpressionBuilder>> newRuleBuilder(
        google::protobuf::Arena* arena);

    std::unique_ptr<google::api::expr::runtime::CelValue> executeRule(RuleContext& ctx,
                                       const SerdeValue& msg,
                                       const std::string& expr,
                                       const absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue>& args,
                                       google::protobuf::Arena* arena);

    absl::StatusOr<std::shared_ptr<google::api::expr::runtime::CelExpression>> getOrCompileExpression(const std::string& expr);

    google::api::expr::runtime::CelValue fromJsonValue(const nlohmann::json& json, google::protobuf::Arena* arena);
    google::api::expr::runtime::CelValue fromAvroValue(const ::avro::GenericDatum& avro, google::protobuf::Arena* arena);
    google::api::expr::runtime::CelValue fromProtobufValue(const google::protobuf::Message& msg, google::protobuf::Arena* arena);

    nlohmann::json toJsonValue(const nlohmann::json& original, const google::api::expr::runtime::CelValue& cel_value);
    ::avro::GenericDatum toAvroValue(const ::avro::GenericDatum& original, const google::api::expr::runtime::CelValue& cel_value);
    std::unique_ptr<google::protobuf::Message> toProtobufValue(const google::protobuf::Message& original, const google::api::expr::runtime::CelValue& cel_value);
};

}