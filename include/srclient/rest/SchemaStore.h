/**
 * Confluent Schema Registry Client
 * Schema store for caching schema and registered schema information
 */

#pragma once

#include <unordered_map>
#include <string>
#include <memory>
#include <optional>
#include "srclient/rest/model/Schema.h"
#include "srclient/rest/model/RegisteredSchema.h"

namespace srclient::rest {

/**
 * Schema store for caching schema and registered schema information
 */
class SchemaStore {
private:
    // Maps subject -> schema_id -> (guid, schema)
    std::unordered_map<std::string, std::unordered_map<int32_t, std::pair<std::optional<std::string>, srclient::rest::model::Schema>>> schemaIdIndex;

    // Maps guid -> schema
    std::unordered_map<std::string, srclient::rest::model::Schema> schemaGuidIndex;

    // Maps subject -> schema -> schema_id
    std::unordered_map<std::string, std::unordered_map<std::string, int32_t>> schemaIndex;

    // Maps subject -> schema_id -> registered_schema
    std::unordered_map<std::string, std::unordered_map<int32_t, srclient::rest::model::RegisteredSchema>> rsIdIndex;

    // Maps subject -> version -> registered_schema
    std::unordered_map<std::string, std::unordered_map<int32_t, srclient::rest::model::RegisteredSchema>> rsVersionIndex;

    // Maps subject -> schema -> registered_schema
    std::unordered_map<std::string, std::unordered_map<std::string, srclient::rest::model::RegisteredSchema>> rsSchemaIndex;

public:
    SchemaStore();

    ~SchemaStore() = default;

    /**
     * Set schema information
     */
    void setSchema(const std::optional<std::string> &subject,
                   const std::optional<int32_t> &schemaId,
                   const std::optional<std::string> &schemaGuid,
                   const srclient::rest::model::Schema &schema);

    /**
     * Set registered schema information
     */
    void setRegisteredSchema(const srclient::rest::model::Schema &schema,
                             const srclient::rest::model::RegisteredSchema &rs);

    /**
     * Get schema by subject and id
     */
    std::optional<std::pair<std::optional<std::string>, srclient::rest::model::Schema>>
    getSchemaById(const std::string &subject, int32_t schemaId) const;

    /**
     * Get schema by guid
     */
    std::optional<srclient::rest::model::Schema> getSchemaByGuid(const std::string &guid) const;

    /**
     * Get schema id by subject and schema
     */
    std::optional<int32_t> getIdBySchema(const std::string &subject,
                                         const srclient::rest::model::Schema &schema) const;

    /**
     * Get registered schema by subject and schema
     */
    std::optional<srclient::rest::model::RegisteredSchema>
    getRegisteredBySchema(const std::string &subject,
                          const srclient::rest::model::Schema &schema) const;

    /**
     * Get registered schema by subject and version
     */
    std::optional<srclient::rest::model::RegisteredSchema>
    getRegisteredByVersion(const std::string &subject, int32_t version) const;

    /**
     * Get registered schema by subject and id
     */
    std::optional<srclient::rest::model::RegisteredSchema>
    getRegisteredById(const std::string &subject, int32_t schemaId) const;

    /**
     * Clear all cached data
     */
    void clear();

private:
    // Helper to create a string hash for schemas
    std::string createSchemaHash(const srclient::rest::model::Schema &schema) const;
};

}