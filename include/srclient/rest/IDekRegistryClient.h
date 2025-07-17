/**
 * IDekRegistryClient
 * Interface for Data Encryption Key (DEK) Registry Client
 */

#pragma once

#include "srclient/rest/ClientConfiguration.h"
#include "srclient/rest/model/Dek.h"
#include "srclient/rest/model/Kek.h"
#include "srclient/rest/model/CreateDekRequest.h"
#include "srclient/rest/model/CreateKekRequest.h"
#include <string>
#include <optional>
#include <vector>
#include <cstdint>

namespace srclient::rest {

/**
 * Interface for DEK Registry Client
 */
class IDekRegistryClient {
public:
    virtual ~IDekRegistryClient() = default;

    /**
     * Get client configuration
     */
    virtual std::shared_ptr<const srclient::rest::ClientConfiguration> getConfiguration() const = 0;

    /**
     * Register a new Key Encryption Key (KEK)
     */
    virtual srclient::rest::model::Kek registerKek(
        const srclient::rest::model::CreateKekRequest& request) = 0;

    /**
     * Register a new Data Encryption Key (DEK)
     */
    virtual srclient::rest::model::Dek registerDek(
        const std::string& kekName,
        const srclient::rest::model::CreateDekRequest& request) = 0;

    /**
     * Get a KEK by name
     */
    virtual srclient::rest::model::Kek getKek(
        const std::string& name,
        bool deleted = false) = 0;

    /**
     * Get a DEK by KEK name, subject, algorithm, and version
     */
    virtual srclient::rest::model::Dek getDek(
        const std::string& kekName,
        const std::string& subject,
        const std::optional<srclient::rest::model::Algorithm>& algorithm = std::nullopt,
        const std::optional<int32_t>& version = std::nullopt,
        bool deleted = false) = 0;

    /**
     * Set the key material for a DEK
     */
    virtual srclient::rest::model::Dek setDekKeyMaterial(
        const std::string& kekName,
        const std::string& subject,
        const std::optional<srclient::rest::model::Algorithm>& algorithm = std::nullopt,
        const std::optional<int32_t>& version = std::nullopt,
        bool deleted = false,
        const std::vector<uint8_t>& keyMaterialBytes = {}) = 0;

    /**
     * Clear all caches
     */
    virtual void clearCaches() = 0;

    /**
     * Close the client
     */
    virtual void close() = 0;
};

} // namespace srclient::rest 