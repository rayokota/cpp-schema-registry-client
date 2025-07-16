/**
 * Confluent Schema Registry Client Interface
 * Pure virtual interface for interacting with Confluent Schema Registry
 */

#pragma once

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <optional>

#include "srclient/rest/model/Schema.h"
#include "srclient/rest/model/RegisteredSchema.h"
#include "srclient/rest/model/ServerConfig.h"

namespace srclient::rest {

/**
 * Interface for Schema Registry Client
 */
class ISchemaRegistryClient {
public:
    virtual ~ISchemaRegistryClient() = default;

    /**
     * Register a schema for the given subject
     */
    virtual srclient::rest::model::RegisteredSchema registerSchema(
            const std::string &subject,
            const srclient::rest::model::Schema &schema,
            bool normalize = false) = 0;

    /**
     * Get schema by subject and ID
     */
    virtual srclient::rest::model::Schema getBySubjectAndId(
            const std::optional<std::string> &subject,
            int32_t id,
            const std::optional<std::string> &format = std::nullopt) = 0;

    /**
     * Get schema by GUID
     */
    virtual srclient::rest::model::Schema getByGuid(
            const std::string &guid,
            const std::optional<std::string> &format = std::nullopt) = 0;

    /**
     * Get registered schema by subject and schema
     */
    virtual srclient::rest::model::RegisteredSchema getBySchema(
            const std::string &subject,
            const srclient::rest::model::Schema &schema,
            bool normalize = false,
            bool deleted = false) = 0;

    /**
     * Get registered schema by subject and version
     */
    virtual srclient::rest::model::RegisteredSchema getVersion(
            const std::string &subject,
            int32_t version,
            bool deleted = false,
            const std::optional<std::string> &format = std::nullopt) = 0;

    /**
     * Get latest version of schema for subject
     */
    virtual srclient::rest::model::RegisteredSchema getLatestVersion(
            const std::string &subject,
            const std::optional<std::string> &format = std::nullopt) = 0;

    /**
     * Get latest version with metadata
     */
    virtual srclient::rest::model::RegisteredSchema getLatestWithMetadata(
            const std::string &subject,
            const std::unordered_map<std::string, std::string> &metadata,
            bool deleted = false,
            const std::optional<std::string> &format = std::nullopt) = 0;

    /**
     * Get all versions for subject
     */
    virtual std::vector<int32_t> getAllVersions(const std::string &subject) = 0;

    /**
     * Get all subjects
     */
    virtual std::vector<std::string> getAllSubjects(bool deleted = false) = 0;

    /**
     * Delete subject
     */
    virtual std::vector<int32_t> deleteSubject(const std::string &subject, bool permanent = false) = 0;

    /**
     * Delete subject version
     */
    virtual int32_t deleteSubjectVersion(const std::string &subject, int32_t version, bool permanent = false) = 0;

    /**
     * Test schema compatibility with latest version
     */
    virtual bool
    testSubjectCompatibility(const std::string &subject, const srclient::rest::model::Schema &schema) = 0;

    /**
     * Test schema compatibility with specific version
     */
    virtual bool
    testCompatibility(const std::string &subject, int32_t version, const srclient::rest::model::Schema &schema) = 0;

    /**
     * Get configuration for subject
     */
    virtual srclient::rest::model::ServerConfig getConfig(const std::string &subject) = 0;

    /**
     * Update configuration for subject
     */
    virtual srclient::rest::model::ServerConfig
    updateConfig(const std::string &subject, const srclient::rest::model::ServerConfig &config) = 0;

    /**
     * Get default configuration
     */
    virtual srclient::rest::model::ServerConfig getDefaultConfig() = 0;

    /**
     * Update default configuration
     */
    virtual srclient::rest::model::ServerConfig updateDefaultConfig(const srclient::rest::model::ServerConfig &config) = 0;

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

} 