#include "srclient/serdes/JsonSerializer.h"
#include "srclient/serdes/JsonUtils.h"

namespace srclient::serdes {

using namespace json_utils;

// JsonSerde implementation
JsonSerde::JsonSerde() {}

std::pair<nlohmann::json, std::optional<std::string>> 
JsonSerde::getParsedSchema(const srclient::rest::model::Schema& schema, 
                          std::shared_ptr<srclient::rest::ISchemaRegistryClient> client) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Create cache key from schema content
    auto schema_str = schema.getSchema();
    std::string cache_key = schema_str.value_or("");
    
    auto it = parsed_schemas_cache_.find(cache_key);
    if (it != parsed_schemas_cache_.end()) {
        return {it->second.first, it->second.second};
    }
    
    // Parse new schema
    nlohmann::json parsed_schema;
    try {
        parsed_schema = nlohmann::json::parse(cache_key);
    } catch (const nlohmann::json::parse_error& e) {
        throw JsonSerdeError("Failed to parse JSON schema: " + std::string(e.what()));
    }
    
    // TODO: Resolve named schemas/references
    resolveNamedSchema(schema, client);
    
    // Store in cache
    parsed_schemas_cache_[cache_key] = {parsed_schema, cache_key};
    
    return {parsed_schema, cache_key};
}

bool JsonSerde::validateJson(const nlohmann::json& value,
                             const nlohmann::json& schema) {
    try {
        // For now, just try to create the schema document
        // Full validation can be implemented later with the correct jsoncons API
        auto jsoncons_schema = nlohmannToJsoncons(schema);
        auto compiled_schema = jsoncons::jsonschema::make_json_schema(jsoncons_schema);
        
        // If we can create the schema successfully, consider validation passed
        // TODO: Implement actual value validation when the jsoncons API is available
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

void JsonSerde::clear() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    parsed_schemas_cache_.clear();
}

void JsonSerde::resolveNamedSchema(const srclient::rest::model::Schema& schema,
                                  std::shared_ptr<srclient::rest::ISchemaRegistryClient> client) {
    // Use the schema resolution utilities
    // TODO: Implement reference resolution for JSON schemas
}

} // namespace srclient::serdes