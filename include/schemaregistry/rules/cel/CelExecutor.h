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
#include "nlohmann/json.hpp"
#include "schemaregistry/serdes/Serde.h"
#include "schemaregistry/serdes/SerdeError.h"
#include "schemaregistry/serdes/protobuf/ProtobufTypes.h"

namespace schemaregistry::rules::cel {

using namespace schemaregistry::serdes;

class CelExecutor : public RuleExecutor {
  public:
    CelExecutor();
    explicit CelExecutor(
        std::unique_ptr<const google::api::expr::runtime::CelExpressionBuilder>
            runtime);
    std::unique_ptr<SerdeValue> transform(
        schemaregistry::serdes::RuleContext &ctx,
        const SerdeValue &msg) override;

    std::string getType() const override;

    std::unique_ptr<SerdeValue> execute(
        schemaregistry::serdes::RuleContext &ctx, const SerdeValue &msg,
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
};

}  // namespace schemaregistry::rules::cel