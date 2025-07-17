#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <nlohmann/json.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>

#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/json/JsonTypes.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/Serde.h"
#include "srclient/rest/ISchemaRegistryClient.h"
#include "srclient/rest/model/Schema.h"

namespace srclient::serdes::json::utils {

/**
 * Schema resolution utilities
 */
namespace schema_resolution {

/**
 * Resolve named schemas and their references
 * @param schema Schema with potential references
 * @param client Schema registry client
 * @return Resolved schema registry for validation
 */
void resolveNamedSchema(
    const srclient::rest::model::Schema& schema,
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client);

/**
 * Resolve all dependencies for a schema
 * @param schema Root schema
 * @param client Schema registry client
 * @param visited Set of already visited schema names to prevent cycles
 * @return Map of resolved schema references
 */
std::unordered_map<std::string, nlohmann::json> resolveAllDependencies(
    const srclient::rest::model::Schema& schema,
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    std::unordered_set<std::string>& visited);

/**
 * Build dependency references for a schema
 * @param schema Schema to analyze
 * @param subject_prefix Prefix for subject names
 * @return List of schema references
 */
std::vector<srclient::rest::model::SchemaReference> buildDependencies(
    const nlohmann::json& schema,
    const std::string& subject_prefix);

} // namespace schema_resolution

/**
 * JSON transformation utilities
 */
namespace value_transform {

/**
 * Transform a JSON value according to field rules
 * @param ctx Rule execution context
 * @param schema JSON schema for the field
 * @param path JSON path to the field
 * @param value JSON value to transform
 * @param field_executor_type Type of field executor to use
 * @return Transformed JSON value
 */
nlohmann::json transformField(
    RuleContext& ctx,
    const nlohmann::json& schema,
    const std::string& path,
    const nlohmann::json& value,
    const std::string& field_executor_type);

/**
 * Transform all fields in a JSON object
 * @param ctx Rule execution context
 * @param schema Root JSON schema
 * @param value JSON object to transform
 * @param field_executor_type Type of field executor to use
 * @return Transformed JSON object
 */
nlohmann::json transformFields(
    RuleContext& ctx,
    const nlohmann::json& schema,
    const nlohmann::json& value,
    const std::string& field_executor_type);

/**
 * Apply rules to a JSON value recursively
 * @param ctx Rule execution context
 * @param schema JSON schema at current level
 * @param path Current JSON path
 * @param value JSON value at current level
 * @param field_executor_type Type of field executor to use
 * @return Transformed JSON value
 */
nlohmann::json applyRulesRecursive(
    RuleContext& ctx,
    const nlohmann::json& schema,
    const std::string& path,
    const nlohmann::json& value,
    const std::string& field_executor_type);

/**
 * Validate subschemas (for allOf, anyOf, oneOf)
 * @param subschemas Array of subschemas
 * @param value JSON value to validate
 * @return Best matching subschema or nullptr
 */
const nlohmann::json* validateSubschemas(
    const nlohmann::json& subschemas,
    const nlohmann::json& value);

} // namespace value_transform

/**
 * JSON schema navigation utilities
 */
namespace schema_navigation {

/**
 * Get field type from JSON schema
 * @param schema JSON schema object
 * @return Corresponding FieldType
 */
FieldType getFieldType(const nlohmann::json& schema);

/**
 * Check if a schema defines an object type
 * @param schema JSON schema object
 * @return True if schema defines an object
 */
bool isObjectSchema(const nlohmann::json& schema);

/**
 * Check if a schema defines an array type
 * @param schema JSON schema object
 * @return True if schema defines an array
 */
bool isArraySchema(const nlohmann::json& schema);

/**
 * Get properties from an object schema
 * @param schema JSON object schema
 * @return Properties map or empty map if not an object schema
 */
nlohmann::json getSchemaProperties(const nlohmann::json& schema);

/**
 * Get items schema from an array schema
 * @param schema JSON array schema
 * @return Items schema or null if not an array schema
 */
nlohmann::json getArrayItemsSchema(const nlohmann::json& schema);

/**
 * Get confluent tags from a schema
 * @param schema JSON schema object
 * @return Set of confluent tags
 */
std::unordered_set<std::string> getConfluentTags(const nlohmann::json& schema);

/**
 * Navigate to a subschema by JSON path
 * @param root_schema Root JSON schema
 * @param path JSON path (e.g., "/properties/field/items")
 * @return Subschema at the given path or null if not found
 */
nlohmann::json navigateToSubschema(const nlohmann::json& root_schema, const std::string& path);

} // namespace schema_navigation

/**
 * JSON validation utilities
 */
namespace validation_utils {

/**
 * Validate JSON value against schema
 * @param value JSON value to validate
 * @param schema JSON schema for validation
 * @return True if validation passes, false otherwise
 */
bool validateJsonAgainstSchema(const nlohmann::json& value,
                              const nlohmann::json& schema);

/**
 * Get validation error details
 * @param value JSON value that failed validation
 * @param schema JSON schema used for validation
 * @return Human-readable error message
 */
std::string getValidationErrorDetails(const nlohmann::json& value,
                                     const nlohmann::json& schema);

} // namespace validation_utils

/**
 * JSON path utilities
 */
namespace path_utils {

/**
 * Build JSON path from components
 * @param components Path components
 * @return JSON path string
 */
std::string buildJsonPath(const std::vector<std::string>& components);

/**
 * Parse JSON path into components
 * @param path JSON path string
 * @return Vector of path components
 */
std::vector<std::string> parseJsonPath(const std::string& path);

/**
 * Append to JSON path
 * @param base_path Base path
 * @param component Component to append
 * @return New path
 */
std::string appendToPath(const std::string& base_path, const std::string& component);

/**
 * Get parent path
 * @param path JSON path
 * @return Parent path or empty string if at root
 */
std::string getParentPath(const std::string& path);

/**
 * Get field name from path
 * @param path JSON path
 * @return Last component of the path
 */
std::string getFieldName(const std::string& path);

} // namespace path_utils

/**
 * Utility functions for JSON schema and value manipulation
 */

/**
 * Convert nlohmann::json to jsoncons::json for validation
 * @param nlohmann_json nlohmann::json object
 * @return jsoncons::json object
 */
jsoncons::json nlohmannToJsoncons(const nlohmann::json& nlohmann_json);

/**
 * Convert jsoncons::json to nlohmann::json
 * @param jsoncons_json jsoncons::json object
 * @return nlohmann::json object
 */
nlohmann::json jsonconsToNlohmann(const jsoncons::json& jsoncons_json);

/**
 * Merge JSON schemas (for allOf, etc.)
 * @param schemas Vector of schemas to merge
 * @return Merged schema
 */
nlohmann::json mergeSchemas(const std::vector<nlohmann::json>& schemas);

/**
 * Check if schema has confluent extensions
 * @param schema JSON schema
 * @return True if schema has confluent-specific extensions
 */
bool hasConfluentExtensions(const nlohmann::json& schema);

/**
 * Normalize JSON schema for comparison
 * @param schema JSON schema to normalize
 * @return Normalized schema
 */
nlohmann::json normalizeSchema(const nlohmann::json& schema);

} // namespace srclient::serdes::json::utils 