/**
 * ExampleKmsDriver
 * Example implementation of KmsDriver interface for demonstration purposes
 */

#pragma once

#include "srclient/rules/encryption/KmsDriver.h"
#include <memory>

namespace srclient::rules::encryption {

/**
 * Example KMS client implementation
 * This is a mock implementation for demonstration purposes
 */
class ExampleKmsClient : public crypto::tink::KmsClient {
public:
    explicit ExampleKmsClient(const std::string& keyUrlPrefix);
    
    // Implement KmsClient interface
    bool DoesSupport(absl::string_view key_uri) const override;
    
    absl::StatusOr<std::unique_ptr<crypto::tink::Aead>> GetAead(
        absl::string_view key_uri) const override;

private:
    std::string keyUrlPrefix_;
};

/**
 * Example KMS driver implementation
 * This demonstrates how to implement the KmsDriver interface
 */
class ExampleKmsDriver : public KmsDriver {
public:
    explicit ExampleKmsDriver(const std::string& keyUrlPrefix);
    
    // Implement KmsDriver interface
    const std::string& getKeyUrlPrefix() const override;
    
    std::shared_ptr<crypto::tink::KmsClient> newKmsClient(
        const std::unordered_map<std::string, std::string>& conf,
        const std::string& keyUrl) override;

private:
    std::string keyUrlPrefix_;
};

} // namespace srclient::rules::encryption 