#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "avro/Generic.hh"
#include "eval/public/cel_expression.h"
#include "google/protobuf/arena.h"
#include "jsoncons/json.hpp"
#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"

namespace srclient::rules::cel {

using namespace srclient::serdes;

class CelExecutor : public RuleExecutor {
  public:
    CelExecutor();
    explicit CelExecutor(
        std::unique_ptr<const google::api::expr::runtime::CelExpressionBuilder>
            runtime);
    std::unique_ptr<SerdeValue> transform(srclient::serdes::RuleContext &ctx,
                                          const SerdeValue &msg) override;

    std::string getType() const override;

    std::unique_ptr<SerdeValue> execute(
        srclient::serdes::RuleContext &ctx, const SerdeValue &msg,
        const absl::flat_hash_map<std::string,
                                  google::api::expr::runtime::CelValue> &args,
        google::protobuf::Arena *arena);

    google::api::expr::runtime::CelValue fromSerdeValue(
        const SerdeValue &value, google::protobuf::Arena *arena);
    std::unique_ptr<SerdeValue> toSerdeValue(
        const SerdeValue &original,
        const google::api::expr::runtime::CelValue &cel_value);

    static void registerExecutor();

  private:
    google::protobuf::Arena arena_;
    std::unique_ptr<const google::api::expr::runtime::CelExpressionBuilder>
        runtime_;

    std::unordered_map<
        std::string, std::shared_ptr<google::api::expr::runtime::CelExpression>>
        expression_cache_;
    mutable std::mutex cache_mutex_;

    absl::StatusOr<
        std::unique_ptr<google::api::expr::runtime::CelExpressionBuilder>>
    newRuleBuilder(google::protobuf::Arena *arena);

    std::unique_ptr<google::api::expr::runtime::CelValue> executeRule(
        RuleContext &ctx, const SerdeValue &msg, const std::string &expr,
        const absl::flat_hash_map<std::string,
                                  google::api::expr::runtime::CelValue> &args,
        google::protobuf::Arena *arena);

    absl::StatusOr<std::shared_ptr<google::api::expr::runtime::CelExpression>>
    getOrCompileExpression(const std::string &expr);

    google::api::expr::runtime::CelValue fromJsonValue(
        const jsoncons::ojson &json, google::protobuf::Arena *arena);
    google::api::expr::runtime::CelValue fromAvroValue(
        const ::avro::GenericDatum &avro, google::protobuf::Arena *arena);
    google::api::expr::runtime::CelValue fromProtobufValue(
        const srclient::serdes::protobuf::ProtobufVariant &variant,
        google::protobuf::Arena *arena);

    jsoncons::ojson toJsonValue(
        const jsoncons::ojson &original,
        const google::api::expr::runtime::CelValue &cel_value);
    ::avro::GenericDatum toAvroValue(
        const ::avro::GenericDatum &original,
        const google::api::expr::runtime::CelValue &cel_value);
    srclient::serdes::protobuf::ProtobufVariant toProtobufValue(
        const srclient::serdes::protobuf::ProtobufVariant &original,
        const google::api::expr::runtime::CelValue &cel_value);

    // Helper methods for protobuf conversion
    google::api::expr::runtime::CelValue convertProtobufFieldToCel(
        const google::protobuf::Message &message,
        const google::protobuf::FieldDescriptor *field,
        const google::protobuf::Reflection *reflection,
        google::protobuf::Arena *arena, int index);

    google::api::expr::runtime::CelValue convertProtobufMapToCel(
        const google::protobuf::Message &map_entry,
        const google::protobuf::FieldDescriptor *map_field,
        google::protobuf::Arena *arena);
};

}  // namespace srclient::rules::cel