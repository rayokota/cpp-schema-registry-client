/**
 * EncryptionRegistry
 * Global registry for KMS drivers and clients - C++ equivalent of Rust mod.rs
 */

#ifndef SRCLIENT_RULES_ENCRYPTION_ENCRYPTION_REGISTRY_H_
#define SRCLIENT_RULES_ENCRYPTION_ENCRYPTION_REGISTRY_H_

#include <memory>
#include <vector>
#include <mutex>
#include <string>
#include <unordered_map>
#include "srclient/rules/encryption/KmsDriver.h"
#include "tink/kms_client.h"

namespace srclient::rules::encryption {

/**
 * Thread-safe singleton registry for KMS drivers and clients
 * 
 * This class provides a global registry system similar to the Rust mod.rs
 * implementation, managing KMS drivers and clients using thread-safe operations.
 */
class EncryptionRegistry {
public:
    /**
     * Get the singleton instance
     */
    static EncryptionRegistry& getInstance();

    // Disable copy and assignment
    EncryptionRegistry(const EncryptionRegistry&) = delete;
    EncryptionRegistry& operator=(const EncryptionRegistry&) = delete;

    /**
     * Register a new KMS driver
     * 
     * @param driver The KMS driver to register
     */
    void registerKmsDriver(std::shared_ptr<KmsDriver> driver);

    /**
     * Remove all registered KMS drivers
     */
    void clearKmsDrivers();

    /**
     * Get a KMS driver by key URI
     * 
     * @param keyUri The key URI to find a supporting driver for
     * @return Shared pointer to the driver that supports the key URI
     * @throws TinkError if no supporting driver is found
     */
    std::shared_ptr<KmsDriver> getKmsDriver(const std::string& keyUri);

    /**
     * Register a new KMS client
     * 
     * This method registers the client both in our local registry and
     * in Tink's global KmsClients registry for maximum compatibility.
     * 
     * @param client The KMS client to register
     */
    void registerKmsClient(std::shared_ptr<crypto::tink::KmsClient> client);

    /**
     * Remove all registered KMS clients
     */
    void clearKmsClients();

    /**
     * Get a KMS client by key URI
     * 
     * This method first checks our local registry, then falls back to
     * Tink's global KmsClients registry.
     * 
     * @param keyUri The key URI to find a supporting client for
     * @return Shared pointer to the client that supports the key URI
     * @throws TinkError if no supporting client is found
     */
    std::shared_ptr<crypto::tink::KmsClient> getKmsClient(const std::string& keyUri);

private:
    EncryptionRegistry() = default;
    ~EncryptionRegistry() = default;

    // Thread-safe storage for KMS drivers
    std::mutex driversMutex_;
    std::vector<std::shared_ptr<KmsDriver>> drivers_;

    // Thread-safe storage for KMS clients
    std::mutex clientsMutex_;
    std::vector<std::shared_ptr<crypto::tink::KmsClient>> clients_;
};

// Convenience functions for easier access (similar to Rust module functions)

/**
 * Register a new KMS driver
 */
template<typename T, typename... Args>
void registerKmsDriver(Args&&... args) {
    static_assert(std::is_base_of_v<KmsDriver, T>, "T must derive from KmsDriver");
    auto driver = std::make_shared<T>(std::forward<Args>(args)...);
    EncryptionRegistry::getInstance().registerKmsDriver(driver);
}

/**
 * Remove all registered KMS drivers
 */
inline void clearKmsDrivers() {
    EncryptionRegistry::getInstance().clearKmsDrivers();
}

/**
 * Get a KMS driver by key URI
 */
inline std::shared_ptr<KmsDriver> getKmsDriver(const std::string& keyUri) {
    return EncryptionRegistry::getInstance().getKmsDriver(keyUri);
}

/**
 * Register a new KMS client
 */
inline void registerKmsClient(std::shared_ptr<crypto::tink::KmsClient> client) {
    EncryptionRegistry::getInstance().registerKmsClient(client);
}

/**
 * Remove all registered KMS clients
 */
inline void clearKmsClients() {
    EncryptionRegistry::getInstance().clearKmsClients();
}

/**
 * Get a KMS client by key URI
 */
inline std::shared_ptr<crypto::tink::KmsClient> getKmsClient(const std::string& keyUri) {
    return EncryptionRegistry::getInstance().getKmsClient(keyUri);
}

} // namespace srclient::rules::encryption

#endif // SRCLIENT_RULES_ENCRYPTION_ENCRYPTION_REGISTRY_H_ 