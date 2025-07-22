/**
 * MockDekRegistryClient
 * Mock implementation of Data Encryption Key (DEK) Registry Client for testing
 */

#pragma once

#include "srclient/rest/IDekRegistryClient.h"
#include "srclient/rest/ClientConfiguration.h"
#include "srclient/rest/RestException.h"
#include "srclient/rest/DekRegistryTypes.h"
#include <memory>
#include <unordered_map>
#include <mutex>
#include <string>
#include <chrono>

namespace srclient::rest {

// Forward declarations
class DekStore;

/**
 * Mock Store for caching DEK and KEK data
 */
class MockDekStore {
public:
    MockDekStore();
    ~MockDekStore() = default;

    void setKek(const KekId& kekId, const srclient::rest::model::Kek& kek);
    void setDek(const DekId& dekId, const srclient::rest::model::Dek& dek);
    
    std::optional<srclient::rest::model::Kek> getKek(const KekId& kekId) const;
    std::optional<srclient::rest::model::Dek> getDek(const DekId& dekId) const;
    
    srclient::rest::model::Dek* getMutDek(const DekId& dekId);
    
    void clear();

private:
    std::unordered_map<KekId, srclient::rest::model::Kek> keks;
    std::unordered_map<DekId, srclient::rest::model::Dek> deks;
    
    friend class MockDekRegistryClient;
};

/**
 * Mock DEK Registry Client implementation for testing
 */
class MockDekRegistryClient : public IDekRegistryClient {
public:
    explicit MockDekRegistryClient(std::shared_ptr<const srclient::rest::ClientConfiguration> config);
    
    virtual ~MockDekRegistryClient() = default;

    virtual std::shared_ptr<const srclient::rest::ClientConfiguration> getConfiguration() const override;

    // IDekRegistryClient implementation
    virtual srclient::rest::model::Kek registerKek(
        const srclient::rest::model::CreateKekRequest& request) override;

    virtual srclient::rest::model::Dek registerDek(
        const std::string& kek_name,
        const srclient::rest::model::CreateDekRequest& request) override;

    virtual srclient::rest::model::Kek getKek(
        const std::string& name,
        bool deleted = false) override;

    virtual srclient::rest::model::Dek getDek(
        const std::string& kek_name,
        const std::string& subject,
        const std::optional<srclient::rest::model::Algorithm>& algorithm = std::nullopt,
        const std::optional<int32_t>& version = std::nullopt,
        bool deleted = false) override;

    virtual srclient::rest::model::Dek setDekKeyMaterial(
        const std::string& kek_name,
        const std::string& subject,
        const std::optional<srclient::rest::model::Algorithm>& algorithm = std::nullopt,
        const std::optional<int32_t>& version = std::nullopt,
        bool deleted = false,
        const std::vector<uint8_t>& key_material_bytes = {}) override;

    virtual void clearCaches() override;

    virtual void close() override;

private:
    std::shared_ptr<const srclient::rest::ClientConfiguration> config;
    std::shared_ptr<MockDekStore> store;
    std::shared_ptr<std::mutex> storeMutex;

    // Helper methods
    int64_t getCurrentTimestamp() const;
};

} // namespace srclient::rest 