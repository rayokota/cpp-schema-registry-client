#pragma once

#include "srclient/serdes/RuleRegistry.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/rules/cel/CelLib.h"
#include "runtime/runtime.h"
#include "parser/parser.h"
#include "runtime/activation.h"
#include "common/value.h"
#include "common/value_manager.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "google/api/expr/v1alpha1/syntax.pb.h"
#include <memory>
#include <string>
#include <unordered_map>

namespace srclient::rules::cel {

/**
 * CEL (Common Expression Language) rule executor
 * Ported from cel_executor.rs
 */
class CelExecutor : public srclient::serdes::RuleExecutor, 
                    public std::enable_shared_from_this<CelExecutor> {
private:
    std::unique_ptr<const ::cel::Runtime> runtime_;
    absl::flat_hash_map<std::string, google::api::expr::v1alpha1::ParsedExpr> expression_cache_;
    mutable absl::Mutex cache_mutex_;
    
public:
    CelExecutor();
    explicit CelExecutor(std::unique_ptr<const ::cel::Runtime> runtime);
    
    // RuleBase interface
    std::string getType() const override { return "CEL"; }
    
    // RuleExecutor interface
    srclient::serdes::SerdeValue& transform(srclient::serdes::RuleContext& ctx, 
                                           srclient::serdes::SerdeValue& msg) override;
    
    /**
     * Execute a CEL expression with the given message and arguments
     */
    std::unique_ptr<srclient::serdes::SerdeValue> execute(srclient::serdes::RuleContext& ctx,
                                        const srclient::serdes::SerdeValue& msg,
                                        const absl::flat_hash_map<std::string, ::cel::Value>& args);
    
    /**
     * Register this executor with the global rule registry
     */
    static void registerExecutor();
    
    /**
     * Convert SerdeValue to CEL Value for evaluation
     */
    ::cel::Value fromSerdeValue(const srclient::serdes::SerdeValue& value, ::cel::ValueManager& value_manager);
    
    /**
     * Get the CEL runtime for accessing value manager
     */
    const ::cel::Runtime& getRuntime() const { return *runtime_; }
    
private:
    /**
     * Execute a specific rule expression
     */
    std::unique_ptr<srclient::serdes::SerdeValue> executeRule(srclient::serdes::RuleContext& ctx,
                                            const srclient::serdes::SerdeValue& msg,
                                            const std::string& expr,
                                            const absl::flat_hash_map<std::string, ::cel::Value>& args);
    
    /**
     * Get or compile a CEL expression
     */
    absl::StatusOr<google::api::expr::v1alpha1::ParsedExpr> getOrCompileExpression(const std::string& expr);
    
    /**
     * Convert CEL Value back to SerdeValue, preserving the original type
     */
    std::unique_ptr<srclient::serdes::SerdeValue> toSerdeValue(const srclient::serdes::SerdeValue& original, 
                                             const ::cel::Value& cel_value);
    
    // Helper conversion methods
    ::cel::Value fromJsonValue(const nlohmann::json& json, ::cel::ValueManager& value_manager);
    ::cel::Value fromAvroValue(const ::avro::GenericDatum& avro, ::cel::ValueManager& value_manager);
    ::cel::Value fromProtobufValue(const google::protobuf::Message& protobuf, ::cel::ValueManager& value_manager);
    
    nlohmann::json toJsonValue(const nlohmann::json& original, const ::cel::Value& cel_value);
    ::avro::GenericDatum toAvroValue(const ::avro::GenericDatum& original, const ::cel::Value& cel_value);
    std::unique_ptr<google::protobuf::Message> toProtobufValue(const google::protobuf::Message& original, const ::cel::Value& cel_value);
};

} // namespace srclient::rules::cel 