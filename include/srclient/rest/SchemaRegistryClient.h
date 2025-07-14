/**
 * Confluent Schema Registry Client
 * Synchronous C++ client for interacting with Confluent Schema Registry
 */

#ifndef SRCLIENT_REST_SCHEMA_REGISTRY_CLIENT_H_
#define SRCLIENT_REST_SCHEMA_REGISTRY_CLIENT_H_

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <optional>
#include <mutex>
#include <thread>
#include <chrono>

#include "srclient/rest/ClientConfiguration.h"
#include "srclient/rest/RestClient.h"
#include "srclient/rest/RestException.h"
#include "srclient/rest/SchemaStore.h"
#include "srclient/rest/model/Schema.h"
#include "srclient/rest/model/RegisterSchemaResponse.h"
#include "srclient/rest/model/Config.h"
#include "srclient/rest/model/CompatibilityCheckResponse.h"

namespace srclient {
namespace rest {

/**
 * Interface for Schema Registry Client
 */
class ISchemaRegistryClient {
public:
    virtual ~ISchemaRegistryClient() = default;
    
    /**
     * Register a schema for the given subject
     */
    virtual org::openapitools::server::model::RegisterSchemaResponse registerSchema(
        const std::string& subject,
        const org::openapitools::server::model::Schema& schema,
        bool normalize = false) = 0;

    /**
     * Get schema by subject and ID
     */
    virtual org::openapitools::server::model::Schema getBySubjectAndId(
        const std::optional<std::string>& subject,
        int32_t id,
        const std::optional<std::string>& format = std::nullopt) = 0;

    /**
     * Get schema by GUID
     */
    virtual org::openapitools::server::model::Schema getByGuid(
        const std::string& guid,
        const std::optional<std::string>& format = std::nullopt) = 0;

    /**
     * Get registered schema by subject and schema
     */
    virtual org::openapitools::server::model::RegisterSchemaResponse getBySchema(
        const std::string& subject,
        const org::openapitools::server::model::Schema& schema,
        bool normalize = false,
        bool deleted = false) = 0;

    /**
     * Get registered schema by subject and version
     */
    virtual org::openapitools::server::model::RegisterSchemaResponse getVersion(
        const std::string& subject,
        int32_t version,
        bool deleted = false,
        const std::optional<std::string>& format = std::nullopt) = 0;

    /**
     * Get latest version of schema for subject
     */
    virtual org::openapitools::server::model::RegisterSchemaResponse getLatestVersion(
        const std::string& subject,
        const std::optional<std::string>& format = std::nullopt) = 0;

    /**
     * Get latest version with metadata
     */
    virtual org::openapitools::server::model::RegisterSchemaResponse getLatestWithMetadata(
        const std::string& subject,
        const std::unordered_map<std::string, std::string>& metadata,
        bool deleted = false,
        const std::optional<std::string>& format = std::nullopt) = 0;

    /**
     * Get all versions for subject
     */
    virtual std::vector<int32_t> getAllVersions(const std::string& subject) = 0;

    /**
     * Get all subjects
     */
    virtual std::vector<std::string> getAllSubjects(bool deleted = false) = 0;

    /**
     * Delete subject
     */
    virtual std::vector<int32_t> deleteSubject(const std::string& subject, bool permanent = false) = 0;

    /**
     * Delete subject version
     */
    virtual int32_t deleteSubjectVersion(const std::string& subject, int32_t version, bool permanent = false) = 0;

    /**
     * Test schema compatibility with latest version
     */
    virtual bool testSubjectCompatibility(const std::string& subject, const org::openapitools::server::model::Schema& schema) = 0;

    /**
     * Test schema compatibility with specific version
     */
    virtual bool testCompatibility(const std::string& subject, int32_t version, const org::openapitools::server::model::Schema& schema) = 0;

    /**
     * Get configuration for subject
     */
    virtual org::openapitools::server::model::Config getConfig(const std::string& subject) = 0;

    /**
     * Update configuration for subject
     */
    virtual org::openapitools::server::model::Config updateConfig(const std::string& subject, const org::openapitools::server::model::Config& config) = 0;

    /**
     * Get default configuration
     */
    virtual org::openapitools::server::model::Config getDefaultConfig() = 0;

    /**
     * Update default configuration
     */
    virtual org::openapitools::server::model::Config updateDefaultConfig(const org::openapitools::server::model::Config& config) = 0;

    /**
     * Clear latest version caches
     */
    virtual void clearLatestCaches() = 0;

    /**
     * Clear all caches
     */
    virtual void clearCaches() = 0;

    /**
     * Close client
     */
    virtual void close() = 0;
};

/**
 * Synchronous Schema Registry Client implementation
 */
class SchemaRegistryClient : public ISchemaRegistryClient {
private:
    std::shared_ptr<org::openapitools::client::api::RestClient> restClient;
    std::shared_ptr<SchemaStore> store;
    std::shared_ptr<std::mutex> storeMutex;
    
    // Caches for latest versions
    std::unordered_map<std::string, org::openapitools::server::model::RegisterSchemaResponse> latestVersionCache;
    std::unordered_map<std::string, org::openapitools::server::model::RegisterSchemaResponse> latestWithMetadataCache;
    std::shared_ptr<std::mutex> latestVersionCacheMutex;
    std::shared_ptr<std::mutex> latestWithMetadataCacheMutex;
    
    // Cache settings
    size_t cacheCapacity;
    std::chrono::seconds cacheLatestTtl;
    
    // Helper methods
    std::string urlEncode(const std::string& str) const;
    std::string createMetadataKey(const std::string& subject, const std::unordered_map<std::string, std::string>& metadata) const;
    
    // JSON processing helpers
    org::openapitools::server::model::Schema parseSchemaFromJson(const std::string& json) const;
    org::openapitools::server::model::RegisterSchemaResponse parseRegisteredSchemaFromJson(const std::string& json) const;
    org::openapitools::server::model::Config parseConfigFromJson(const std::string& json) const;
    bool parseBoolFromJson(const std::string& json) const;
    std::vector<int32_t> parseIntArrayFromJson(const std::string& json) const;
    std::vector<std::string> parseStringArrayFromJson(const std::string& json) const;
    
    // HTTP request helpers
    std::string sendHttpRequest(const std::string& path, const std::string& method, 
                              const std::multimap<std::string, std::string>& query = {},
                              const std::string& body = "") const;

public:
    /**
     * Constructor
     */
    SchemaRegistryClient(std::shared_ptr<const org::openapitools::client::api::ClientConfiguration> config);

    /**
     * Destructor
     */
    ~SchemaRegistryClient() override;

    /**
     * Get client configuration
     */
    std::shared_ptr<const org::openapitools::client::api::ClientConfiguration> getConfiguration() const;

    // Implement ISchemaRegistryClient methods
    org::openapitools::server::model::RegisterSchemaResponse registerSchema(
        const std::string& subject,
        const org::openapitools::server::model::Schema& schema,
        bool normalize = false) override;

    org::openapitools::server::model::Schema getBySubjectAndId(
        const std::optional<std::string>& subject,
        int32_t id,
        const std::optional<std::string>& format = std::nullopt) override;

    org::openapitools::server::model::Schema getByGuid(
        const std::string& guid,
        const std::optional<std::string>& format = std::nullopt) override;

    org::openapitools::server::model::RegisterSchemaResponse getBySchema(
        const std::string& subject,
        const org::openapitools::server::model::Schema& schema,
        bool normalize = false,
        bool deleted = false) override;

    org::openapitools::server::model::RegisterSchemaResponse getVersion(
        const std::string& subject,
        int32_t version,
        bool deleted = false,
        const std::optional<std::string>& format = std::nullopt) override;

    org::openapitools::server::model::RegisterSchemaResponse getLatestVersion(
        const std::string& subject,
        const std::optional<std::string>& format = std::nullopt) override;

    org::openapitools::server::model::RegisterSchemaResponse getLatestWithMetadata(
        const std::string& subject,
        const std::unordered_map<std::string, std::string>& metadata,
        bool deleted = false,
        const std::optional<std::string>& format = std::nullopt) override;

    std::vector<int32_t> getAllVersions(const std::string& subject) override;

    std::vector<std::string> getAllSubjects(bool deleted = false) override;

    std::vector<int32_t> deleteSubject(const std::string& subject, bool permanent = false) override;

    int32_t deleteSubjectVersion(const std::string& subject, int32_t version, bool permanent = false) override;

    bool testSubjectCompatibility(const std::string& subject, const org::openapitools::server::model::Schema& schema) override;

    bool testCompatibility(const std::string& subject, int32_t version, const org::openapitools::server::model::Schema& schema) override;

    org::openapitools::server::model::Config getConfig(const std::string& subject) override;

    org::openapitools::server::model::Config updateConfig(const std::string& subject, const org::openapitools::server::model::Config& config) override;

    org::openapitools::server::model::Config getDefaultConfig() override;

    org::openapitools::server::model::Config updateDefaultConfig(const org::openapitools::server::model::Config& config) override;

    void clearLatestCaches() override;

    void clearCaches() override;

    void close() override;
};

} // namespace rest
} // namespace srclient

#endif // SRCLIENT_REST_SCHEMA_REGISTRY_CLIENT_H_ 