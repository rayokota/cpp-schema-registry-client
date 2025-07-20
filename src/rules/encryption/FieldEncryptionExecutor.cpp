#include "srclient/rules/encryption/FieldEncryptionExecutor.h"
#include "srclient/serdes/RuleRegistry.h"

namespace srclient::rules::encryption {

using namespace srclient::serdes;

// Field encryption executor implementation
template<typename T>
FieldEncryptionExecutor<T>::FieldEncryptionExecutor(std::shared_ptr<Clock> clock)
    : executor_(clock) {
}

template<typename T>
void FieldEncryptionExecutor<T>::configure(std::shared_ptr<const ClientConfiguration> client_config,
                                          const std::unordered_map<std::string, std::string>& rule_config) {
    executor_.configure(client_config, rule_config);
}

template<typename T>
std::string FieldEncryptionExecutor<T>::getType() const {
    return "ENCRYPT";
}

template<typename T>
void FieldEncryptionExecutor<T>::close() {
    executor_.close();
}

template<typename T>
std::unique_ptr<SerdeValue> FieldEncryptionExecutor<T>::transformField(RuleContext& ctx, const SerdeValue& field_value) {
    auto transform = executor_.newTransform(ctx);
    auto field_ctx = ctx.currentField();
    if (!field_ctx) {
        throw SerdeError("no field context");
    }
    
    return transform->transform(ctx, field_ctx->getFieldType(), field_value);
}

template<typename T>
void FieldEncryptionExecutor<T>::registerExecutor() {
    global_registry::registerRuleExecutor(
        std::make_shared<FieldEncryptionExecutor<T>>(nullptr)
    );
}

// Template instantiations
template class FieldEncryptionExecutor<DekRegistryClient>;

} // namespace srclient::rules::encryption 