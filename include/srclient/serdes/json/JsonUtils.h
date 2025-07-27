#pragma once

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "srclient/rest/ISchemaRegistryClient.h"
#include "srclient/rest/model/Schema.h"
#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/json/JsonTypes.h"

namespace srclient::serdes::json::utils {

/**
 * Schema resolution utilities
 */
namespace schema_resolution {

/**
 * Resolve all dependencies for a schema
 * @param schema Root schema
 * @param client Schema registry client
 * @param visited Set of already visited schema names to prevent cycles
 * @return Map of resolved schema references
 */
std::unordered_map<std::string, jsoncons::ojson> resolveNamedSchema(
    const srclient::rest::model::Schema &schema,
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    std::unordered_set<std::string> &visited);

}  // namespace schema_resolution

/**
 * JSON transformation utilities
 */
namespace value_transform {

/**
 * Transform all fields in a JSON object
 * @param ctx Rule execution context
 * @param schema Root JSON schema
 * @param value JSON object to transform
 * @return Transformed JSON object
 */
jsoncons::ojson transformFields(
    RuleContext &ctx,
    std::shared_ptr<jsoncons::jsonschema::json_schema<jsoncons::ojson>> schema,
    const jsoncons::ojson &value);

/**
 * Transform a JSON value according to field rules
 * @param ctx Rule execution context
 * @param schema JSON schema for the field
 * @param path JSON path to the field
 * @param value JSON value to transform
 * @return Transformed JSON value
 */
jsoncons::ojson transformFieldWithContext(RuleContext &ctx,
                                          const jsoncons::ojson &schema,
                                          const std::string &path,
                                          const jsoncons::ojson &value);
/**
 * Transform a JSON object
 * @param ctx Rule execution context
 * @param schema JSON schema at current level
 * @param path Current JSON path
 * @param value JSON value at current level
 * @return Transformed JSON value
 */
jsoncons::ojson transform(RuleContext &ctx, const jsoncons::ojson &schema,
                          const std::string &path,
                          const jsoncons::ojson &value);

}  // namespace value_transform

/**
 * JSON schema navigation utilities
 */
namespace schema_navigation {

/**
 * Get field type from JSON schema
 * @param schema JSON schema object
 * @return Corresponding FieldType
 */
FieldType getFieldType(const jsoncons::ojson &schema);

/**
 * Check if a schema defines an object type
 * @param schema JSON schema object
 * @return True if schema defines an object
 */
bool isObjectSchema(const jsoncons::ojson &schema);

/**
 * Check if a schema defines an array type
 * @param schema JSON schema object
 * @return True if schema defines an array
 */
bool isArraySchema(const jsoncons::ojson &schema);

/**
 * Get properties from an object schema
 * @param schema JSON object schema
 * @return Properties map or empty map if not an object schema
 */
jsoncons::ojson getSchemaProperties(const jsoncons::ojson &schema);

/**
 * Get items schema from an array schema
 * @param schema JSON array schema
 * @return Items schema or null if not an array schema
 */
jsoncons::ojson getArrayItemsSchema(const jsoncons::ojson &schema);

/**
 * Get confluent tags from a schema
 * @param schema JSON schema object
 * @return Set of confluent tags
 */
std::unordered_set<std::string> getConfluentTags(const jsoncons::ojson &schema);

}  // namespace schema_navigation

/**
 * JSON validation utilities
 */
namespace validation_utils {

/**
 * Validate JSON value against schema
 * @param schema JSON schema for validation
 * @param value JSON value to validate
 * @return True if validation passes, false otherwise
 */
bool validateJson(
    std::shared_ptr<jsoncons::jsonschema::json_schema<jsoncons::ojson>> schema,
    const jsoncons::ojson &value);

}  // namespace validation_utils

/**
 * JSON path utilities
 */
namespace path_utils {

/**
 * Append to JSON path
 * @param base_path Base path
 * @param component Component to append
 * @return New path
 */
std::string appendToPath(const std::string &base_path,
                         const std::string &component);

/**
 * Get field name from path
 * @param path JSON path
 * @return Last component of the path
 */
std::string getFieldName(const std::string &path);

}  // namespace path_utils

}  // namespace srclient::serdes::json::utils