#pragma once

#include "srclient/serdes/RuleRegistry.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/rules/cel/CelExecutor.h"
#include "absl/container/flat_hash_map.h"
#include "common/value.h"
#include <memory>
#include <string>

namespace srclient::rules::cel {

/**
 * CEL Field-level rule executor
 * Ported from cel_field_executor.rs
 */
class CelFieldExecutor : public srclient::serdes::FieldRuleExecutor,
                         public std::enable_shared_from_this<CelFieldExecutor> {
private:
    std::unique_ptr<CelExecutor> executor_;
    
public:
    CelFieldExecutor();
    explicit CelFieldExecutor(std::unique_ptr<CelExecutor> executor);
    
    // RuleBase interface
    std::string getType() const override { return "CEL_FIELD"; }
    
    // FieldRuleExecutor interface
    srclient::serdes::SerdeValue& transformField(srclient::serdes::RuleContext& ctx, 
                                                srclient::serdes::SerdeValue& field_value) override;
    
    /**
     * Register this executor with the global rule registry
     */
    static void registerExecutor();
    
protected:
    /**
     * Override shared_from_this for FieldRuleExecutor compatibility
     */
    std::shared_ptr<srclient::serdes::FieldRuleExecutor> shared_from_this() override {
        return std::static_pointer_cast<srclient::serdes::FieldRuleExecutor>(
            std::enable_shared_from_this<CelFieldExecutor>::shared_from_this()
        );
    }
    
private:
    /**
     * Create arguments for field-level CEL execution
     */
    absl::flat_hash_map<std::string, ::cel::Value> createFieldArguments(
        const srclient::serdes::RuleContext& ctx,
        const srclient::serdes::SerdeValue& field_value
    );
};

} // namespace srclient::rules::cel 