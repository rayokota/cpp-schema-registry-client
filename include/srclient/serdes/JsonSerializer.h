#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>

#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeConfig.h"
#include "srclient/rest/ISchemaRegistryClient.h"

namespace srclient::serdes {

// Forward declarations
class JsonSerializer;

class JsonSerde;

/**
 * JSON-specific serialization error
 */
class JsonSerdeError : public SerdeError {
public:
    explicit JsonSerdeError(const std::string& message) : SerdeError("JSON serde error: " + message) {}
};

/**
 * JSON schema caching and validation class
 * Based on JsonSerde struct from json.rs (converted to synchronous)
 */
class JsonSerde {
public:
    JsonSerde();
    ~JsonSerde() = default;

    // Schema parsing and caching
    std::pair<nlohmann::json, std::optional<std::string>> 
    getParsedSchema(const srclient::rest::model::Schema& schema, 
                   std::shared_ptr<srclient::rest::ISchemaRegistryClient> client);

    // Validator caching
    bool validateJson(const nlohmann::json& value,
                     const nlohmann::json& schema);

    // Clear caches
    void clear();

private:
    // Cache for parsed schemas: Schema -> (parsed_json, schema_string)
    std::unordered_map<std::string, std::pair<
        nlohmann::json, 
        std::string
    >> parsed_schemas_cache_;
    
    mutable std::mutex cache_mutex_;
    
    // Helper methods
    void resolveNamedSchema(const srclient::rest::model::Schema& schema,
                           std::shared_ptr<srclient::rest::ISchemaRegistryClient> client);
};

/**
 * JSON serializer class template
 * Based on JsonSerializer from json.rs (converted to synchronous)
 */
class JsonSerializer {
public:
    /**
     * Constructor
     */
    JsonSerializer(std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
                  std::optional<srclient::rest::model::Schema> schema,
                  std::shared_ptr<RuleRegistry> rule_registry,
                  const SerializerConfig& config);

    /**
     * Serialize a JSON value to bytes with schema validation
     * @param ctx Serialization context (topic, serde type, etc.)
     * @param value JSON value to serialize
     * @return Serialized bytes with schema ID header
     */
    std::vector<uint8_t> serialize(const SerializationContext& ctx, 
                                  const nlohmann::json& value);

    /**
     * Close the serializer and cleanup resources
     */
    void close();

private:
    std::optional<srclient::rest::model::Schema> schema_;
    std::shared_ptr<BaseSerializer> base_;
    std::unique_ptr<JsonSerde> serde_;

    // Helper methods
    std::pair<nlohmann::json, std::optional<std::string>>
    getParsedSchema(const srclient::rest::model::Schema& schema);
    
    bool validateJson(const nlohmann::json& value,
                     const nlohmann::json& schema);
    
    void validateSchema(const srclient::rest::model::Schema& schema);
    
    SerdeValue transformValue(const SerdeValue& value, 
                            const srclient::rest::model::Rule& rule,
                            const RuleContext& context);
    
    nlohmann::json executeFieldTransformations(const nlohmann::json& value,
                                               const nlohmann::json& schema,
                                               const RuleContext& context,
                                               const std::string& field_executor_type);
};

// Template method implementations

JsonSerializer::JsonSerializer(
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    std::optional<srclient::rest::model::Schema> schema,
    std::shared_ptr<RuleRegistry> rule_registry,
    const SerializerConfig& config
) : schema_(std::move(schema)),
    base_(std::make_shared<BaseSerializer>(Serde(client, rule_registry), config)),
    serde_(std::make_unique<JsonSerde>())
{
    // Configure rule executors
    if (rule_registry) {
        auto executors = rule_registry->getExecutors();
        for (const auto& executor : executors) {
            try {
                auto rule_registry = base_->getSerde().getRuleRegistry();
                if (rule_registry) {
                    auto client = base_->getSerde().getClient();
                    // TODO: Fix ClientConfiguration vs ServerConfig conversion
                    // executor->configure(client->getConfig("default"), config.rule_config);
                }
            } catch (const std::exception& e) {
                throw JsonSerdeError("Failed to configure rule executor: " + std::string(e.what()));
            }
        }
    }
}

std::vector<uint8_t> JsonSerializer::serialize(
    const SerializationContext& ctx,
    const nlohmann::json& value
) {
    auto mutable_value = value; // Copy for potential transformation
    
    // Get subject using strategy
    auto strategy = base_->getConfig().subject_name_strategy;
    auto subject_opt = strategy(ctx.topic, ctx.serde_type, schema_);
    if (!subject_opt.has_value()) {
        throw JsonSerdeError("Subject name strategy returned no subject");
    }
    std::string subject = subject_opt.value();
    
    // Get or register schema
    SchemaId schema_id(SerdeFormat::Json);
    std::optional<srclient::rest::model::RegisteredSchema> latest_schema;
    
    try {
        latest_schema = base_->getSerde().getReaderSchema(subject, std::nullopt, base_->getConfig().use_schema);
    } catch (const std::exception& e) {
        // Schema not found - will use provided schema
    }
    
    srclient::rest::model::Schema target_schema;
    nlohmann::json parsed_schema;
    std::optional<std::string> schema_str;
    
    if (latest_schema.has_value()) {
        target_schema = latest_schema->toSchema();
        auto id_opt = latest_schema->getId();
        if (id_opt.has_value()) {
            schema_id.setId(id_opt.value());
        }
        auto guid_opt = latest_schema->getGuid();
        if (guid_opt.has_value()) {
            schema_id.setGuid(guid_opt.value());
        }
        
        // Get parsed schema
        std::tie(parsed_schema, schema_str) = getParsedSchema(target_schema);
        
        // Apply field transformations if rule registry exists
        if (base_->getSerde().getRuleRegistry()) {
            // TODO: Implement field transformation execution
            // This would involve traversing the JSON structure and applying rules
        }
    } else {
        // Use provided schema
        if (!schema_.has_value()) {
            throw JsonSerdeError("Schema needs to be set for auto-registration");
        }
        target_schema = schema_.value();
        
        // Register or get schema
        if (base_->getConfig().auto_register_schemas) {
            auto registered_schema = base_->getSerde().getClient()->registerSchema(
                subject, target_schema, base_->getConfig().normalize_schemas);
            auto id_opt = registered_schema.getId();
            if (id_opt.has_value()) {
                schema_id.setId(id_opt.value());
            }
            auto guid_opt = registered_schema.getGuid();
            if (guid_opt.has_value()) {
                schema_id.setGuid(guid_opt.value());
            }
        } else {
            auto registered_schema = base_->getSerde().getClient()->getBySchema(
                subject, target_schema, base_->getConfig().normalize_schemas, false);
            auto id_opt = registered_schema.getId();
            if (id_opt.has_value()) {
                schema_id.setId(id_opt.value());
            }
            auto guid_opt = registered_schema.getGuid();
            if (guid_opt.has_value()) {
                schema_id.setGuid(guid_opt.value());
            }
        }
        
        std::tie(parsed_schema, schema_str) = getParsedSchema(target_schema);
    }
    
    // Validate JSON against schema if validation is enabled
    if (base_->getConfig().validate) {
        if (schema_str.has_value()) {
            try {
                validateJson(mutable_value, parsed_schema);
            } catch (const std::exception& e) {
                throw JsonValidationError("JSON validation failed: " + std::string(e.what()));
            }
        } else {
            // If schema string is not available, we cannot validate
            // This case might need a different error handling strategy
            // For now, we'll just serialize without validation
        }
    }
    
    // Serialize JSON to bytes
    std::string json_string = mutable_value.dump();
    std::vector<uint8_t> encoded_bytes(json_string.begin(), json_string.end());
    
    // Apply encoding rules if present
    auto rule_set = target_schema.getRuleSet();
    if (rule_set.has_value()) {
        // TODO: Implement encoding rule execution
    }
    
    // Serialize schema ID with message
    auto id_serializer = base_->getConfig().schema_id_serializer;
    return id_serializer(encoded_bytes, ctx, schema_id);
}

void JsonSerializer::close() {
    // Cleanup resources
    serde_->clear();
}

// Helper method implementations
std::pair<nlohmann::json, std::optional<std::string>>
JsonSerializer::getParsedSchema(const srclient::rest::model::Schema& schema) {
    return serde_->getParsedSchema(schema, base_->getSerde().getClient());
}

bool JsonSerializer::validateJson(const nlohmann::json& value,
                                            const nlohmann::json& schema) {
    return serde_->validateJson(value, schema);
}

void JsonSerializer::validateSchema(const srclient::rest::model::Schema& schema) {
    auto schema_str = schema.getSchema();
    if (!schema_str.has_value() || schema_str->empty()) {
        throw JsonSerdeError("Schema content is empty");
    }
    
    auto schema_type = schema.getSchemaType();
    if (schema_type.has_value() && schema_type.value() != "JSON") {
        throw JsonSerdeError("Schema type must be JSON");
    }
}

SerdeValue JsonSerializer::transformValue(const SerdeValue& value,
                                                     const srclient::rest::model::Rule& rule,
                                                     const RuleContext& context) {
    // TODO: Implement value transformation based on rules
    return value;
}

nlohmann::json JsonSerializer::executeFieldTransformations(
    const nlohmann::json& value,
    const nlohmann::json& schema,
    const RuleContext& context,
    const std::string& field_executor_type) {
    // TODO: Implement field-level transformations
    return value;
}

} // namespace srclient::serdes