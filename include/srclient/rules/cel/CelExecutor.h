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

namespace srclient::rules::cel {

using namespace srclient::serdes;

class CelExecutor : public RuleExecutor {
public:
    CelExecutor();
    explicit CelExecutor(std::unique_ptr<const google::api::expr::runtime::CelExpressionBuilder> runtime);
    SerdeValue& transform(RuleContext& ctx, SerdeValue& msg) override;
    
    // Implement the required getType method from RuleBase
    std::string getType() const override;

    static void registerExecutor();

private:
    static absl::StatusOr<std::unique_ptr<google::api::expr::runtime::CelExpressionBuilder>> newRuleBuilder(
        google::protobuf::Arena* arena);

    std::unique_ptr<SerdeValue> execute(RuleContext& ctx, 
                                       const SerdeValue& msg, 
                                       const absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue>& args);
    std::unique_ptr<google::api::expr::runtime::CelValue> executeRule(RuleContext& ctx,
                                           const SerdeValue& msg,
                                           const std::string& expr,
                                           const absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue>& args);

    absl::StatusOr<std::unique_ptr<google::api::expr::runtime::CelExpression>> getOrCompileExpression(const std::string& expr);

    static google::api::expr::runtime::CelValue fromSerdeValue(const SerdeValue& value);
    static std::unique_ptr<SerdeValue> toSerdeValue(const SerdeValue& original, const google::api::expr::runtime::CelValue& cel_value);

    static google::api::expr::runtime::CelValue fromJsonValue(const nlohmann::json& json);
    static nlohmann::json toJsonValue(const nlohmann::json& original, const google::api::expr::runtime::CelValue& cel_value);

    static google::api::expr::runtime::CelValue fromAvroValue(const ::avro::GenericDatum& avro);
    static ::avro::GenericDatum toAvroValue(const ::avro::GenericDatum& original, const google::api::expr::runtime::CelValue& cel_value);

    static google::api::expr::runtime::CelValue fromProtobufValue(const google::protobuf::Message& protobuf);
    static std::unique_ptr<google::protobuf::Message> toProtobufValue(const google::protobuf::Message& original, const google::api::expr::runtime::CelValue& cel_value);

    std::unique_ptr<const google::api::expr::runtime::CelExpressionBuilder> runtime_;
    absl::Mutex cache_mutex_;
    absl::flat_hash_map<std::string, std::unique_ptr<google::api::expr::runtime::CelExpression>> expression_cache_ ABSL_GUARDED_BY(cache_mutex_);

    friend class CelFieldExecutor;
};

}