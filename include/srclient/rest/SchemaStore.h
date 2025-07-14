/**
 * Confluent Schema Registry Client
 * Schema store for caching schema and registered schema information
 */

#ifndef SRCLIENT_REST_SCHEMA_STORE_H_
#define SRCLIENT_REST_SCHEMA_STORE_H_

#include <unordered_map>
#include <string>
#include <memory>
#include <optional>
#include "srclient/rest/model/Schema.h"
#include "srclient/rest/model/RegisterSchemaResponse.h"

namespace srclient {
namespace rest {

/**
 * Schema store for caching schema and registered schema information
 */
class SchemaStore {
private:
    // Maps subject -> schema_id -> (guid, schema)
    std::unordered_map<std::string, std::unordered_map<int32_t, std::pair<std::optional<std::string>, org::openapitools::server::model::Schema>>> schemaIdIndex;
    
    // Maps guid -> schema
    std::unordered_map<std::string, org::openapitools::server::model::Schema> schemaGuidIndex;
    
    // Maps subject -> schema -> schema_id
    std::unordered_map<std::string, std::unordered_map<std::string, int32_t>> schemaIndex;
    
    // Maps subject -> schema_id -> registered_schema
    std::unordered_map<std::string, std::unordered_map<int32_t, org::openapitools::server::model::RegisterSchemaResponse>> rsIdIndex;
    
    // Maps subject -> version -> registered_schema
    std::unordered_map<std::string, std::unordered_map<int32_t, org::openapitools::server::model::RegisterSchemaResponse>> rsVersionIndex;
    
    // Maps subject -> schema -> registered_schema
    std::unordered_map<std::string, std::unordered_map<std::string, org::openapitools::server::model::RegisterSchemaResponse>> rsSchemaIndex;

public:
    SchemaStore();
    ~SchemaStore() = default;

    /**
     * Set schema information
     */
    void setSchema(const std::optional<std::string>& subject,
                   const std::optional<int32_t>& schemaId,
                   const std::optional<std::string>& schemaGuid,
                   const org::openapitools::server::model::Schema& schema);

    /**
     * Set registered schema information
     */
    void setRegisteredSchema(const org::openapitools::server::model::Schema& schema,
                           const org::openapitools::server::model::RegisterSchemaResponse& rs);

    /**
     * Get schema by subject and id
     */
    std::optional<std::pair<std::optional<std::string>, org::openapitools::server::model::Schema>> 
    getSchemaById(const std::string& subject, int32_t schemaId) const;

    /**
     * Get schema by guid
     */
    std::optional<org::openapitools::server::model::Schema> getSchemaByGuid(const std::string& guid) const;

    /**
     * Get schema id by subject and schema
     */
    std::optional<int32_t> getIdBySchema(const std::string& subject,
                                       const org::openapitools::server::model::Schema& schema) const;

    /**
     * Get registered schema by subject and schema
     */
    std::optional<org::openapitools::server::model::RegisterSchemaResponse> 
    getRegisteredBySchema(const std::string& subject,
                         const org::openapitools::server::model::Schema& schema) const;

    /**
     * Get registered schema by subject and version
     */
    std::optional<org::openapitools::server::model::RegisterSchemaResponse> 
    getRegisteredByVersion(const std::string& subject, int32_t version) const;

    /**
     * Get registered schema by subject and id
     */
    std::optional<org::openapitools::server::model::RegisterSchemaResponse> 
    getRegisteredById(const std::string& subject, int32_t schemaId) const;

    /**
     * Clear all cached data
     */
    void clear();

private:
    // Helper to create a string hash for schemas
    std::string createSchemaHash(const org::openapitools::server::model::Schema& schema) const;
};

} // namespace rest
} // namespace srclient

#endif // SRCLIENT_REST_SCHEMA_STORE_H_ 