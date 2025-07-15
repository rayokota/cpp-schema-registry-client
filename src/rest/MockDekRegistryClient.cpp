/**
 * MockDekRegistryClient
 * Mock implementation of Data Encryption Key (DEK) Registry Client for testing
 */

#include "srclient/rest/MockDekRegistryClient.h"
#include <chrono>
#include <mutex>

using namespace srclient::rest;

// MockDekStore implementation
MockDekStore::MockDekStore() {
    // Empty constructor
}

void MockDekStore::setKek(const KekId& kekId, const srclient::rest::model::Kek& kek) {
    keks[kekId] = kek;
}

void MockDekStore::setDek(const DekId& dekId, const srclient::rest::model::Dek& dek) {
    deks[dekId] = dek;
}

std::optional<srclient::rest::model::Kek> MockDekStore::getKek(const KekId& kekId) const {
    auto it = keks.find(kekId);
    if (it != keks.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<srclient::rest::model::Dek> MockDekStore::getDek(const DekId& dekId) const {
    auto it = deks.find(dekId);
    if (it != deks.end()) {
        return it->second;
    }
    return std::nullopt;
}

srclient::rest::model::Dek* MockDekStore::getMutDek(const DekId& dekId) {
    auto it = deks.find(dekId);
    if (it != deks.end()) {
        return &it->second;
    }
    return nullptr;
}

void MockDekStore::clear() {
    keks.clear();
    deks.clear();
}

// MockDekRegistryClient implementation
MockDekRegistryClient::MockDekRegistryClient(std::shared_ptr<const srclient::rest::ClientConfiguration> config)
    : config(config)
    , store(std::make_shared<MockDekStore>())
    , storeMutex(std::make_shared<std::mutex>()) {
}

std::shared_ptr<const srclient::rest::ClientConfiguration> MockDekRegistryClient::getConfiguration() const {
    return config;
}

int64_t MockDekRegistryClient::getCurrentTimestamp() const {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

srclient::rest::model::Kek MockDekRegistryClient::registerKek(
    const srclient::rest::model::CreateKekRequest& request) {
    
    std::lock_guard<std::mutex> lock(*storeMutex);
    
    KekId cacheKey = {
        request.getName(),
        false
    };
    
    // Check if KEK already exists
    auto existingKek = store->getKek(cacheKey);
    if (existingKek.has_value()) {
        return existingKek.value();
    }
    
    // Create new KEK
    srclient::rest::model::Kek kek(
        request.getName(),
        request.getKmsType(),
        request.getKmsKeyId(),
        request.getKmsProps(),
        request.getDoc(),
        request.getShared(),
        getCurrentTimestamp(),
        false
    );
    
    store->setKek(cacheKey, kek);
    return kek;
}

srclient::rest::model::Dek MockDekRegistryClient::registerDek(
    const std::string& kekName,
    const srclient::rest::model::CreateDekRequest& request) {
    
    std::lock_guard<std::mutex> lock(*storeMutex);
    
    DekId cacheKey = {
        kekName,
        request.getSubject(),
        request.getVersion().value_or(1),
        request.getAlgorithm().value_or(srclient::rest::model::Algorithm::Aes256Gcm),
        false
    };
    
    // Check if DEK already exists
    auto existingDek = store->getDek(cacheKey);
    if (existingDek.has_value()) {
        return existingDek.value();
    }
    
    // Create new DEK
    srclient::rest::model::Dek dek(
        kekName,
        request.getSubject(),
        request.getVersion().value_or(1),
        request.getAlgorithm().value_or(srclient::rest::model::Algorithm::Aes256Gcm),
        request.getEncryptedKeyMaterial(),
        std::nullopt, // keyMaterial
        getCurrentTimestamp(),
        false
    );
    
    store->setDek(cacheKey, dek);
    return dek;
}

srclient::rest::model::Kek MockDekRegistryClient::getKek(
    const std::string& name,
    bool deleted) {
    
    std::lock_guard<std::mutex> lock(*storeMutex);
    
    KekId kekId = {
        name,
        deleted
    };
    
    auto kek = store->getKek(kekId);
    if (kek.has_value()) {
        return kek.value();
    }
    
    throw srclient::rest::RestException("KEK not found: " + name);
}

srclient::rest::model::Dek MockDekRegistryClient::getDek(
    const std::string& kekName,
    const std::string& subject,
    const std::optional<srclient::rest::model::Algorithm>& algorithm,
    const std::optional<int32_t>& version,
    bool deleted) {
    
    std::lock_guard<std::mutex> lock(*storeMutex);
    
    auto alg = algorithm.value_or(srclient::rest::model::Algorithm::Aes256Gcm);
    auto ver = version.value_or(1);
    
    DekId dekId = {
        kekName,
        subject,
        ver,
        alg,
        false  // Use the stored DEK version, not the deleted parameter
    };
    
    auto dek = store->getDek(dekId);
    if (dek.has_value()) {
        return dek.value();
    }
    
    throw srclient::rest::RestException("DEK not found: " + kekName + "/" + subject);
}

srclient::rest::model::Dek MockDekRegistryClient::setDekKeyMaterial(
    const std::string& kekName,
    const std::string& subject,
    const std::optional<srclient::rest::model::Algorithm>& algorithm,
    const std::optional<int32_t>& version,
    bool deleted,
    const std::vector<uint8_t>& keyMaterialBytes) {
    
    std::lock_guard<std::mutex> lock(*storeMutex);
    
    auto alg = algorithm.value_or(srclient::rest::model::Algorithm::Aes256Gcm);
    auto ver = version.value_or(1);
    
    DekId dekId = {
        kekName,
        subject,
        ver,
        alg,
        false
    };
    
    auto* dek = store->getMutDek(dekId);
    if (dek != nullptr) {
        dek->setKeyMaterialFromBytes(keyMaterialBytes);
        return *dek;
    }
    
    throw srclient::rest::RestException("DEK not found for key material update: " + kekName + "/" + subject);
}

void MockDekRegistryClient::clearCaches() {
    std::lock_guard<std::mutex> lock(*storeMutex);
    store->clear();
}

void MockDekRegistryClient::close() {
    // No-op for mock implementation
} 