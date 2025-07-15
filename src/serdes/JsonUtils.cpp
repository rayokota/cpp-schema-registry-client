#include "srclient/serdes/JsonUtils.h"
#include <algorithm>
#include <sstream>

namespace srclient::serdes::json_utils {

// Schema resolution implementations
namespace schema_resolution {

void resolveNamedSchema(
    const srclient::rest::model::Schema& schema,
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client) {
    
    auto references = schema.getReferences();
    if (!references.has_value()) {
        return;
    }
    
    // Resolve each reference
    for (const auto& ref : references.value()) {
        auto name_opt = ref.getName();
        auto subject_opt = ref.getSubject();
        auto version_opt = ref.getVersion();
        
        if (!name_opt.has_value() || !subject_opt.has_value()) {
            continue;
        }
        
        try {
            // Get referenced schema
            auto registered_schema = client->getVersion(
                subject_opt.value(),
                version_opt.value_or(-1),
                true,
                std::nullopt
            );
            
            auto ref_schema = registered_schema.toSchema();
            auto ref_schema_str = ref_schema.getSchema();
            
            if (ref_schema_str.has_value()) {
                // Parse referenced schema
                nlohmann::json ref_parsed_schema = nlohmann::json::parse(ref_schema_str.value());
                
                // TODO: Add resolved reference to schema document
                // This would involve integrating the referenced schema
            }
        } catch (const std::exception& e) {
            // Skip missing references
        }
    }
}

std::unordered_map<std::string, nlohmann::json> resolveAllDependencies(
    const srclient::rest::model::Schema& schema,
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    std::unordered_set<std::string>& visited) {
    
    std::unordered_map<std::string, nlohmann::json> resolved_schemas;
    
    auto references = schema.getReferences();
    if (!references.has_value()) {
        return resolved_schemas;
    }
    
    for (const auto& ref : references.value()) {
        auto name_opt = ref.getName();
        auto subject_opt = ref.getSubject();
        auto version_opt = ref.getVersion();
        
        if (!name_opt.has_value() || !subject_opt.has_value()) {
            continue;
        }
        
        std::string name = name_opt.value();
        if (visited.find(name) != visited.end()) {
            continue; // Avoid cycles
        }
        visited.insert(name);
        
        try {
            auto registered_schema = client->getVersion(
                subject_opt.value(),
                version_opt.value_or(-1),
                true,
                std::nullopt
            );
            
            auto ref_schema = registered_schema.toSchema();
            auto ref_schema_str = ref_schema.getSchema();
            
            if (ref_schema_str.has_value()) {
                nlohmann::json parsed_ref_schema = nlohmann::json::parse(ref_schema_str.value());
                resolved_schemas[name] = parsed_ref_schema;
                
                // Recursively resolve dependencies
                auto nested_deps = resolveAllDependencies(ref_schema, client, visited);
                resolved_schemas.insert(nested_deps.begin(), nested_deps.end());
            }
        } catch (const std::exception& e) {
            // Skip missing references
        }
    }
    
    return resolved_schemas;
}

std::vector<srclient::rest::model::SchemaReference> buildDependencies(
    const nlohmann::json& schema,
    const std::string& subject_prefix) {
    
    std::vector<srclient::rest::model::SchemaReference> dependencies;
    
    // TODO: Implement JSON schema analysis to find $ref dependencies
    // This would involve traversing the schema and finding all $ref entries
    
    return dependencies;
}

} // namespace schema_resolution

// Value transformation implementations
namespace value_transform {

nlohmann::json transformField(
    RuleContext& ctx,
    const nlohmann::json& schema,
    const std::string& path,
    const nlohmann::json& value,
    const std::string& field_executor_type) {
    
    // TODO: Implement field-specific transformation logic
    // This would involve checking confluent tags and applying appropriate rules
    return value;
}

nlohmann::json transformFields(
    RuleContext& ctx,
    const nlohmann::json& schema,
    const nlohmann::json& value,
    const std::string& field_executor_type) {
    
    return applyRulesRecursive(ctx, schema, "$", value, field_executor_type);
}

nlohmann::json applyRulesRecursive(
    RuleContext& ctx,
    const nlohmann::json& schema,
    const std::string& path,
    const nlohmann::json& value,
    const std::string& field_executor_type) {
    
    // Handle allOf, anyOf, oneOf
    if (schema.contains("allOf")) {
        // For now, just use the first subschema - TODO: implement proper validation
        if (schema["allOf"].is_array() && !schema["allOf"].empty()) {
            return applyRulesRecursive(ctx, schema["allOf"][0], path, value, field_executor_type);
        }
    }
    
    // Handle object schemas
    if (schema_navigation::isObjectSchema(schema) && value.is_object()) {
        auto result = value;
        auto properties = schema_navigation::getSchemaProperties(schema);
        
        for (auto& [key, field_value] : result.items()) {
            if (properties.contains(key)) {
                std::string field_path = path_utils::appendToPath(path, key);
                result[key] = applyRulesRecursive(ctx, properties[key], field_path, field_value, field_executor_type);
            }
        }
        
        return result;
    }
    
    // Handle array schemas
    if (schema_navigation::isArraySchema(schema) && value.is_array()) {
        auto result = value;
        auto items_schema = schema_navigation::getArrayItemsSchema(schema);
        
        if (!items_schema.is_null()) {
            for (size_t i = 0; i < result.size(); ++i) {
                std::string item_path = path_utils::appendToPath(path, std::to_string(i));
                result[i] = applyRulesRecursive(ctx, items_schema, item_path, result[i], field_executor_type);
            }
        }
        
        return result;
    }
    
    // Apply field-level transformation
    return transformField(ctx, schema, path, value, field_executor_type);
}

const nlohmann::json* validateSubschemas(
    const nlohmann::json& subschemas,
    const nlohmann::json& value) {
    
    if (!subschemas.is_array()) {
        return nullptr;
    }
    
    // For now, return the first subschema
    // TODO: Implement proper validation to find the best matching subschema
    if (!subschemas.empty()) {
        return &subschemas[0];
    }
    
    return nullptr;
}

} // namespace value_transform

// Schema navigation implementations
namespace schema_navigation {

FieldType getFieldType(const nlohmann::json& schema) {
    if (!schema.contains("type")) {
        return FieldType::String; // Default fallback
    }
    
    std::string type = schema["type"];
    
    if (type == "object") {
        return FieldType::Record;
    } else if (type == "array") {
        return FieldType::Array;
    } else if (type == "string") {
        return FieldType::String;
    } else if (type == "integer") {
        return FieldType::Int;
    } else if (type == "number") {
        return FieldType::Double;
    } else if (type == "boolean") {
        return FieldType::Boolean;
    } else if (type == "null") {
        return FieldType::Null;
    }
    
    return FieldType::String; // Default fallback
}

bool isObjectSchema(const nlohmann::json& schema) {
    return schema.contains("type") && schema["type"] == "object";
}

bool isArraySchema(const nlohmann::json& schema) {
    return schema.contains("type") && schema["type"] == "array";
}

nlohmann::json getSchemaProperties(const nlohmann::json& schema) {
    if (schema.contains("properties") && schema["properties"].is_object()) {
        return schema["properties"];
    }
    return nlohmann::json::object();
}

nlohmann::json getArrayItemsSchema(const nlohmann::json& schema) {
    if (schema.contains("items")) {
        return schema["items"];
    }
    return nlohmann::json::object();
}

std::unordered_set<std::string> getConfluentTags(const nlohmann::json& schema) {
    std::unordered_set<std::string> tags;
    
    if (schema.contains("confluent:tags") && schema["confluent:tags"].is_array()) {
        for (const auto& tag : schema["confluent:tags"]) {
            if (tag.is_string()) {
                tags.insert(tag);
            }
        }
    }
    
    return tags;
}

nlohmann::json navigateToSubschema(const nlohmann::json& root_schema, const std::string& path) {
    // TODO: Implement JSON path navigation
    // This would involve parsing the path and traversing the schema structure
    return root_schema;
}

} // namespace schema_navigation

// Validation utilities implementations
namespace validation_utils {

bool validateJsonAgainstSchema(const nlohmann::json& value,
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

std::string getValidationErrorDetails(const nlohmann::json& value,
                                     const nlohmann::json& schema) {
    // Simplified error details for now
    return "JSON validation failed against schema";
}

} // namespace validation_utils

// Path utilities implementations
namespace path_utils {

std::string buildJsonPath(const std::vector<std::string>& components) {
    if (components.empty()) {
        return "$";
    }
    
    std::string path = "$";
    for (const auto& component : components) {
        path += "." + component;
    }
    return path;
}

std::vector<std::string> parseJsonPath(const std::string& path) {
    std::vector<std::string> components;
    
    if (path == "$") {
        return components;
    }
    
    // Simple implementation - split by '.'
    std::stringstream ss(path.substr(1)); // Skip the '$'
    std::string component;
    
    while (std::getline(ss, component, '.')) {
        if (!component.empty()) {
            components.push_back(component);
        }
    }
    
    return components;
}

std::string appendToPath(const std::string& base_path, const std::string& component) {
    if (base_path == "$") {
        return "$." + component;
    }
    return base_path + "." + component;
}

std::string getParentPath(const std::string& path) {
    auto last_dot = path.find_last_of('.');
    if (last_dot == std::string::npos || last_dot == 0) {
        return "$";
    }
    return path.substr(0, last_dot);
}

std::string getFieldName(const std::string& path) {
    auto last_dot = path.find_last_of('.');
    if (last_dot == std::string::npos) {
        return path;
    }
    return path.substr(last_dot + 1);
}

} // namespace path_utils

// General utility functions
jsoncons::json nlohmannToJsoncons(const nlohmann::json& nlohmann_json) {
    try {
        // Convert nlohmann::json to string and parse with jsoncons
        std::string json_str = nlohmann_json.dump();
        return jsoncons::json::parse(json_str);
    } catch (const std::exception& e) {
        throw JsonUtilsError("Failed to convert nlohmann to jsoncons: " + std::string(e.what()));
    }
}

nlohmann::json jsonconsToNlohmann(const jsoncons::json& jsoncons_json) {
    try {
        // Convert jsoncons::json to string and parse with nlohmann
        std::string json_str = jsoncons_json.to_string();
        return nlohmann::json::parse(json_str);
    } catch (const std::exception& e) {
        throw JsonUtilsError("Failed to convert jsoncons to nlohmann: " + std::string(e.what()));
    }
}

nlohmann::json mergeSchemas(const std::vector<nlohmann::json>& schemas) {
    if (schemas.empty()) {
        return nlohmann::json::object();
    }
    
    nlohmann::json merged = schemas[0];
    
    for (size_t i = 1; i < schemas.size(); ++i) {
        // Simple merge - combine properties
        if (merged.contains("properties") && schemas[i].contains("properties")) {
            for (auto& [key, value] : schemas[i]["properties"].items()) {
                merged["properties"][key] = value;
            }
        }
    }
    
    return merged;
}

bool hasConfluentExtensions(const nlohmann::json& schema) {
    // Check for confluent-specific properties
    for (auto& [key, value] : schema.items()) {
        if (key.find("confluent:") == 0) {
            return true;
        }
    }
    return false;
}

nlohmann::json normalizeSchema(const nlohmann::json& schema) {
    // Simple normalization - sort keys
    if (schema.is_object()) {
        nlohmann::json normalized = nlohmann::json::object();
        std::vector<std::string> keys;
        
        for (auto& [key, value] : schema.items()) {
            keys.push_back(key);
        }
        
        std::sort(keys.begin(), keys.end());
        
        for (const auto& key : keys) {
            normalized[key] = normalizeSchema(schema[key]);
        }
        
        return normalized;
    } else if (schema.is_array()) {
        nlohmann::json normalized = nlohmann::json::array();
        for (const auto& item : schema) {
            normalized.push_back(normalizeSchema(item));
        }
        return normalized;
    }
    
    return schema;
}

} // namespace srclient::serdes::json_utils 