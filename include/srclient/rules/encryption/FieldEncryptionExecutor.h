#pragma once

#include "srclient/rules/encryption/EncryptionExecutor.h"
#include "srclient/serdes/Serde.h"

namespace srclient::rules::encryption {

/**
 * Field-level encryption executor
 * Based on FieldEncryptionExecutor from encrypt_executor.rs
 */
template<typename T>
class FieldEncryptionExecutor : public FieldRuleExecutor {
    static_assert(std::is_base_of_v<IDekRegistryClient, T>, "T must inherit from IDekRegistryClient");

private:
    EncryptionExecutor<T> executor_;

public:
    explicit FieldEncryptionExecutor(std::shared_ptr<Clock> clock = std::make_shared<SystemClock>());

    // RuleBase interface
    void configure(std::shared_ptr<const ClientConfiguration> client_config,
                  const std::unordered_map<std::string, std::string>& rule_config) override;
    std::string getType() const override;
    void close() override;

    // FieldRuleExecutor interface
    std::unique_ptr<SerdeValue> transformField(RuleContext& ctx, const SerdeValue& field_value) override;

    // Static registration
    static void registerExecutor();
};

// Template instantiation declarations for common client types
extern template class FieldEncryptionExecutor<DekRegistryClient>;

} // namespace srclient::rules::encryption 