#include "srclient/rules/encryption/FieldEncryptionExecutor.h"
#include "srclient/serdes/RuleRegistry.h"

namespace srclient::rules::encryption {

using namespace srclient::serdes;

// Field encryption executor implementation
FieldEncryptionExecutor::FieldEncryptionExecutor(std::shared_ptr<Clock> clock)
    : executor_(clock) {}

void FieldEncryptionExecutor::configure(
    std::shared_ptr<const ClientConfiguration> client_config,
    const std::unordered_map<std::string, std::string> &rule_config) {
    executor_.configure(client_config, rule_config);
}

std::string FieldEncryptionExecutor::getType() const { return "ENCRYPT"; }

void FieldEncryptionExecutor::close() { executor_.close(); }

std::unique_ptr<SerdeValue>
FieldEncryptionExecutor::transformField(RuleContext &ctx,
                                        const SerdeValue &field_value) {
    auto transform = executor_.newTransform(ctx);
    auto field_ctx = ctx.currentField();
    if (!field_ctx) { throw SerdeError("no field context"); }

    return transform->transform(ctx, field_ctx->getFieldType(), field_value);
}

void FieldEncryptionExecutor::registerExecutor() {
    global_registry::registerRuleExecutor(
        std::make_shared<FieldEncryptionExecutor>(nullptr));
}

} // namespace srclient::rules::encryption