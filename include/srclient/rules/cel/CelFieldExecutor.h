#pragma once

#include "srclient/rules/cel/CelExecutor.h"
#include "srclient/serdes/Serde.h"

namespace srclient::rules::cel {

class CelFieldExecutor : public FieldRuleExecutor {
public:
    CelFieldExecutor();
    explicit CelFieldExecutor(std::shared_ptr<CelExecutor> executor);
    SerdeValue& transformField(RuleContext& ctx, SerdeValue& field_value) override;

    static void registerExecutor();

private:
    std::shared_ptr<CelExecutor> executor_;
};

} 