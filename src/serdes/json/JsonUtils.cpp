#include "srclient/serdes/json/JsonUtils.h"

#include <algorithm>
#include <sstream>

#include "srclient/serdes/RuleRegistry.h"  // For global_registry functions

namespace srclient::serdes::json::utils {

// Schema resolution implementations
namespace schema_resolution {

std::unordered_map<std::string, nlohmann::json> resolveNamedSchema(
    const srclient::rest::model::Schema &schema,
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    std::unordered_set<std::string> &visited) {
    std::unordered_map<std::string, nlohmann::json> resolved_schemas;

    auto references = schema.getReferences();
    if (!references.has_value()) {
        return resolved_schemas;
    }

    for (const auto &ref : references.value()) {
        auto name_opt = ref.getName();
        auto subject_opt = ref.getSubject();
        auto version_opt = ref.getVersion();

        if (!name_opt.has_value() || !subject_opt.has_value()) {
            continue;
        }

        std::string name = name_opt.value();
        if (visited.find(name) != visited.end()) {
            continue;  // Avoid cycles
        }
        visited.insert(name);

        try {
            auto registered_schema = client->getVersion(
                subject_opt.value(), version_opt.value_or(-1), true,
                std::nullopt);

            auto ref_schema = registered_schema.toSchema();
            auto ref_schema_str = ref_schema.getSchema();

            if (ref_schema_str.has_value()) {
                nlohmann::json parsed_ref_schema =
                    nlohmann::json::parse(ref_schema_str.value());
                resolved_schemas[name] = parsed_ref_schema;

                // Recursively resolve dependencies
                auto nested_deps =
                    resolveNamedSchema(ref_schema, client, visited);
                resolved_schemas.insert(nested_deps.begin(), nested_deps.end());
            }
        } catch (const std::exception &e) {
            // Skip missing references
        }
    }

    return resolved_schemas;
}

std::vector<srclient::rest::model::SchemaReference> buildDependencies(
    const nlohmann::json &schema, const std::string &subject_prefix) {
    std::vector<srclient::rest::model::SchemaReference> dependencies;

    // TODO: Implement JSON schema analysis to find $ref dependencies
    // This would involve traversing the schema and finding all $ref entries

    return dependencies;
}

}  // namespace schema_resolution

// Value transformation implementations
namespace value_transform {

nlohmann::json transformFields(RuleContext &ctx, const nlohmann::json &schema,
                               const nlohmann::json &value) {
    return transformRecursive(ctx, schema, "$", value);
}

nlohmann::json transformRecursive(RuleContext &ctx,
                                  const nlohmann::json &schema,
                                  const std::string &path,
                                  const nlohmann::json &value) {
    // Handle allOf, anyOf, oneOf
    if (schema.contains("allOf")) {
        // validate subschemas
        auto subschemas = schema["allOf"];
        auto subschema = validateSubschemas(subschemas, value);
        if (subschema) {
            return transformRecursive(ctx, *subschema, path, value);
        }
    }
    if (schema.contains("anyOf")) {
        // validate subschemas
        auto subschemas = schema["anyOf"];
        auto subschema = validateSubschemas(subschemas, value);
        if (subschema) {
            return transformRecursive(ctx, *subschema, path, value);
        }
    }
    if (schema.contains("oneOf")) {
        // validate subschemas
        auto subschemas = schema["oneOf"];
        auto subschema = validateSubschemas(subschemas, value);
        if (subschema) {
            return transformRecursive(ctx, *subschema, path, value);
        }
    }

    // Handle object schemas
    if (schema_navigation::isObjectSchema(schema) && value.is_object()) {
        auto result = value;
        auto properties = schema_navigation::getSchemaProperties(schema);

        for (auto &[key, field_value] : result.items()) {
            if (properties.contains(key)) {
                std::string field_path = path_utils::appendToPath(path, key);
                result[key] =
                    transformFieldWithContext(ctx, properties[key], field_path,
                                              field_value);
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
                std::string item_path =
                    path_utils::appendToPath(path, std::to_string(i));
                result[i] = transformRecursive(ctx, items_schema, item_path,
                                               result[i]);
            }
        }

        return result;
    }

    // Field-level transformation logic
    auto field_ctx = ctx.currentField();
    if (field_ctx.has_value()) {
        field_ctx->setFieldType(schema_navigation::getFieldType(schema));

        auto rule_tags = ctx.getRule().getTags();
        std::unordered_set<std::string> rule_tags_set;
        if (rule_tags.has_value()) {
            rule_tags_set = std::unordered_set<std::string>(rule_tags->begin(),
                                                            rule_tags->end());
        }

        // Check if rule tags overlap with field context tags (empty
        // rule_tags means apply to all)
        bool should_apply = !rule_tags.has_value() || rule_tags_set.empty();
        if (!should_apply) {
            const auto &field_tags = field_ctx->getTags();
            for (const auto &field_tag : field_tags) {
                if (rule_tags_set.find(field_tag) != rule_tags_set.end()) {
                    should_apply = true;
                    break;
                }
            }
        }

        if (should_apply) {
            auto message_value = makeJsonValue(value);

            // Get field executor type from the rule
            auto field_executor_type = ctx.getRule().getType().value_or("");

            // Try to get executor from context's rule registry first,
            // then global
            std::shared_ptr<RuleExecutor> executor;
            if (ctx.getRuleRegistry()) {
                executor =
                    ctx.getRuleRegistry()->getExecutor(field_executor_type);
            }
            if (!executor) {
                executor =
                    global_registry::getRuleExecutor(field_executor_type);
            }

            if (executor) {
                auto field_executor =
                    std::dynamic_pointer_cast<FieldRuleExecutor>(executor);
                if (!field_executor) {
                    throw JsonError("executor " + field_executor_type +
                                    " is not a field rule executor");
                }

                auto new_value =
                    field_executor->transformField(ctx, *message_value);
                if (new_value && new_value->getFormat() == SerdeFormat::Json) {
                    return asJson(*new_value);
                }
            }
        }
    }

    return value;
}

nlohmann::json transformFieldWithContext(
    RuleContext &ctx, const nlohmann::json &schema, const std::string &path,
    const nlohmann::json &value) {
    // Get field type from schema
    FieldType field_type = schema_navigation::getFieldType(schema);

    // Get field name from path
    std::string field_name = path_utils::getFieldName(path);

    // Create message value from the JSON value
    auto message_value = makeJsonValue(value);

    // Get inline tags from schema
    std::unordered_set<std::string> inline_tags =
        schema_navigation::getConfluentTags(schema);

    // Enter field context
    ctx.enterField(*message_value, path, field_name, field_type, inline_tags);

    try {
        // Transform the field value (synchronous call)
        nlohmann::json new_value =
            transformRecursive(ctx, schema, path, value);

        // Check for condition rules
        auto rule_kind = ctx.getRule().getKind();
        if (rule_kind.has_value() && rule_kind.value() == Kind::Condition) {
            if (new_value.is_boolean()) {
                bool condition_result = new_value.get<bool>();
                if (!condition_result) {
                    throw JsonError("Rule condition failed for field: " +
                                    field_name);
                }
            }
        }

        ctx.exitField();
        return new_value;

    } catch (const std::exception &e) {
        ctx.exitField();
        throw;
    }
}

const nlohmann::json *validateSubschemas(const nlohmann::json &subschemas,
                                         const nlohmann::json &value) {
    if (!subschemas.is_array()) {
        return nullptr;
    }

    // Iterate over subschemas and find the best matching one
    for (const auto &subschema : subschemas) {
        if (validation_utils::validateJsonAgainstSchema(value, subschema)) {
            return &subschema;
        }
    }

    return nullptr;
}

}  // namespace value_transform

// Schema navigation implementations
namespace schema_navigation {

FieldType getFieldType(const nlohmann::json &schema) {
    if (!schema.contains("type")) {
        return FieldType::String;  // Default fallback
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

    return FieldType::String;  // Default fallback
}

bool isObjectSchema(const nlohmann::json &schema) {
    return schema.contains("type") && schema["type"] == "object";
}

bool isArraySchema(const nlohmann::json &schema) {
    return schema.contains("type") && schema["type"] == "array";
}

nlohmann::json getSchemaProperties(const nlohmann::json &schema) {
    if (schema.contains("properties") && schema["properties"].is_object()) {
        return schema["properties"];
    }
    return nlohmann::json::object();
}

nlohmann::json getArrayItemsSchema(const nlohmann::json &schema) {
    if (schema.contains("items")) {
        return schema["items"];
    }
    return nlohmann::json::object();
}

std::unordered_set<std::string> getConfluentTags(const nlohmann::json &schema) {
    std::unordered_set<std::string> tags;

    if (schema.contains("confluent:tags") &&
        schema["confluent:tags"].is_array()) {
        for (const auto &tag : schema["confluent:tags"]) {
            if (tag.is_string()) {
                tags.insert(tag);
            }
        }
    }

    return tags;
}

}  // namespace schema_navigation

// Validation utilities implementations
namespace validation_utils {

bool validateJsonAgainstSchema(const nlohmann::json &value,
                               const nlohmann::json &schema) {
    try {
        auto jsoncons_value = nlohmannToJsoncons(value);
        auto jsoncons_schema = nlohmannToJsoncons(schema);
        auto compiled_schema =
            jsoncons::jsonschema::make_json_schema(jsoncons_schema);

        compiled_schema.validate(jsoncons_value);

        return true;
    } catch (const std::exception &e) {
        return false;
    }
}

std::string getValidationErrorDetails(const nlohmann::json &value,
                                      const nlohmann::json &schema) {
    // Simplified error details for now
    return "JSON validation failed against schema";
}

}  // namespace validation_utils

// Path utilities implementations
namespace path_utils {

std::string appendToPath(const std::string &base_path,
                         const std::string &component) {
    return base_path + "." + component;
}

std::string getFieldName(const std::string &path) {
    auto last_dot = path.find_last_of('.');
    if (last_dot == std::string::npos) {
        return path;
    }
    return path.substr(last_dot + 1);
}

}  // namespace path_utils

// General utility functions
jsoncons::json nlohmannToJsoncons(const nlohmann::json &nlohmann_json) {
    try {
        // Convert nlohmann::json to string and parse with jsoncons
        std::string json_str = nlohmann_json.dump();
        return jsoncons::json::parse(json_str);
    } catch (const std::exception &e) {
        throw JsonError("Failed to convert nlohmann to jsoncons: " +
                        std::string(e.what()));
    }
}

nlohmann::json jsonconsToNlohmann(const jsoncons::json &jsoncons_json) {
    try {
        // Convert jsoncons::json to string and parse with nlohmann
        std::string json_str = jsoncons_json.to_string();
        return nlohmann::json::parse(json_str);
    } catch (const std::exception &e) {
        throw JsonError("Failed to convert jsoncons to nlohmann: " +
                        std::string(e.what()));
    }
}

}  // namespace srclient::serdes::json::utils