/**
 * Confluent Schema Registry Client
 * Schema store implementation for caching schema and registered schema information
 */

#include "srclient/rest/SchemaStore.h"
#include <sstream>
#include <functional>

namespace srclient::rest {

SchemaStore::SchemaStore() {}

void SchemaStore::setSchema(const std::optional<std::string>& subject,
                           const std::optional<int32_t>& schemaId,
                           const std::optional<std::string>& schemaGuid,
                           const srclient::rest::model::Schema& schema) {
    std::string subjectStr = subject.value_or("");

    if (schemaId.has_value()) {
        // Update schema id index
        schemaIdIndex[subjectStr][schemaId.value()] = std::make_pair(schemaGuid, schema);

        // Update schema index
        std::string schemaHash = createSchemaHash(schema);
        schemaIndex[subjectStr][schemaHash] = schemaId.value();
    }

    if (schemaGuid.has_value()) {
        // Update schema guid index
        schemaGuidIndex[schemaGuid.value()] = schema;
    }
}

void SchemaStore::setRegisteredSchema(const srclient::rest::model::Schema& schema,
                                     const srclient::rest::model::RegisteredSchema& rs) {
    std::string subjectStr = "";
    if (rs.subjectIsSet()) {
        subjectStr = rs.getSubject();
    }

    std::string schemaHash = createSchemaHash(schema);

    // Update registered schema by ID index
    if (rs.idIsSet()) {
        rsIdIndex[subjectStr][rs.getId()] = rs;
    }

    // Update registered schema by version index
    if (rs.versionIsSet()) {
        rsVersionIndex[subjectStr][rs.getVersion()] = rs;
    }

    // Update registered schema by schema index
    rsSchemaIndex[subjectStr][schemaHash] = rs;

    // Also update the schema store
    std::optional<std::string> guid;
    if (rs.schemaIsSet()) {
        guid = rs.getSchema();
    }

    std::optional<std::string> subjectOpt;
    if (!subjectStr.empty()) {
        subjectOpt = subjectStr;
    }

    setSchema(subjectOpt, rs.getId(), guid, schema);
}

std::optional<std::pair<std::optional<std::string>, srclient::rest::model::Schema>>
SchemaStore::getSchemaById(const std::string& subject, int32_t schemaId) const {
    auto subjectIt = schemaIdIndex.find(subject);
    if (subjectIt != schemaIdIndex.end()) {
        auto schemaIt = subjectIt->second.find(schemaId);
        if (schemaIt != subjectIt->second.end()) {
            return schemaIt->second;
        }
    }
    return std::nullopt;
}

std::optional<srclient::rest::model::Schema>
SchemaStore::getSchemaByGuid(const std::string& guid) const {
    auto it = schemaGuidIndex.find(guid);
    if (it != schemaGuidIndex.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<int32_t> SchemaStore::getIdBySchema(const std::string& subject,
                                                 const srclient::rest::model::Schema& schema) const {
    auto subjectIt = schemaIndex.find(subject);
    if (subjectIt != schemaIndex.end()) {
        std::string schemaHash = createSchemaHash(schema);
        auto schemaIt = subjectIt->second.find(schemaHash);
        if (schemaIt != subjectIt->second.end()) {
            return schemaIt->second;
        }
    }
    return std::nullopt;
}

std::optional<srclient::rest::model::RegisteredSchema>
SchemaStore::getRegisteredBySchema(const std::string& subject,
                                  const srclient::rest::model::Schema& schema) const {
    auto subjectIt = rsSchemaIndex.find(subject);
    if (subjectIt != rsSchemaIndex.end()) {
        std::string schemaHash = createSchemaHash(schema);
        auto schemaIt = subjectIt->second.find(schemaHash);
        if (schemaIt != subjectIt->second.end()) {
            return schemaIt->second;
        }
    }
    return std::nullopt;
}

std::optional<srclient::rest::model::RegisteredSchema>
SchemaStore::getRegisteredByVersion(const std::string& subject, int32_t version) const {
    auto subjectIt = rsVersionIndex.find(subject);
    if (subjectIt != rsVersionIndex.end()) {
        auto versionIt = subjectIt->second.find(version);
        if (versionIt != subjectIt->second.end()) {
            return versionIt->second;
        }
    }
    return std::nullopt;
}

std::optional<srclient::rest::model::RegisteredSchema>
SchemaStore::getRegisteredById(const std::string& subject, int32_t schemaId) const {
    auto subjectIt = rsIdIndex.find(subject);
    if (subjectIt != rsIdIndex.end()) {
        auto idIt = subjectIt->second.find(schemaId);
        if (idIt != subjectIt->second.end()) {
            return idIt->second;
        }
    }
    return std::nullopt;
}

void SchemaStore::clear() {
    schemaIdIndex.clear();
    schemaGuidIndex.clear();
    schemaIndex.clear();
    rsIdIndex.clear();
    rsVersionIndex.clear();
    rsSchemaIndex.clear();
}

std::string SchemaStore::createSchemaHash(const srclient::rest::model::Schema& schema) const {
    // Create a hash string from the schema content
    std::stringstream ss;
    ss << schema.getSchema();
    if (schema.schemaTypeIsSet()) {
        ss << "_" << schema.getSchemaType();
    }

    // Use std::hash to create a hash value
    std::hash<std::string> hasher;
    return std::to_string(hasher(ss.str()));
}

}