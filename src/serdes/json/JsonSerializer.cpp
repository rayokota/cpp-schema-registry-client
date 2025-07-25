#include "srclient/serdes/json/JsonSerializer.h"

#include "srclient/serdes/json/JsonUtils.h"

namespace srclient::serdes::json {

using namespace utils;

// JsonSerde implementation
JsonSerde::JsonSerde() {}

std::shared_ptr<jsoncons::jsonschema::json_schema<jsoncons::ojson>>
JsonSerde::getParsedSchema(
    const srclient::rest::model::Schema &schema,
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client) {
    std::lock_guard<std::mutex> lock(cache_mutex_);

    // Create cache key from schema content
    auto schema_str = schema.getSchema();
    std::string cache_key = schema_str.value_or("");

    auto it = parsed_schemas_cache_.find(cache_key);
    if (it != parsed_schemas_cache_.end()) {
        return it->second;
    }

    // Parse schema with references
    std::unordered_map<std::string, std::string> references;
    resolveNamedSchema(schema, client, references);

    // Parse new schema
    nlohmann::json parsed_schema;
    try {
        parsed_schema = nlohmann::json::parse(cache_key);
    } catch (const nlohmann::json::parse_error &e) {
        throw JsonError("Failed to parse JSON schema: " +
                        std::string(e.what()));
    }

    auto jsoncons_schema = nlohmannToJsoncons(parsed_schema);
    auto compiled_schema =
        std::make_shared<jsoncons::jsonschema::json_schema<jsoncons::ojson>>(
            jsoncons::jsonschema::make_json_schema(jsoncons_schema));

    // Store in cache
    parsed_schemas_cache_[cache_key] = compiled_schema;

    return compiled_schema;
}

void JsonSerde::clear() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    parsed_schemas_cache_.clear();
}

void JsonSerde::resolveNamedSchema(
    const srclient::rest::model::Schema &schema,
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    std::unordered_map<std::string, std::string> references) {
    // Use the schema resolution utilities
    // TODO: Implement reference resolution for JSON schemas
}

JsonSerializer::JsonSerializer(
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    std::optional<srclient::rest::model::Schema> schema,
    std::shared_ptr<RuleRegistry> rule_registry, const SerializerConfig &config)
    : schema_(std::move(schema)),
      base_(std::make_shared<BaseSerializer>(Serde(client, rule_registry),
                                             config)),
      serde_(std::make_unique<JsonSerde>()) {
    // Configure rule executors
    if (rule_registry) {
        auto executors = rule_registry->getExecutors();
        for (const auto &executor : executors) {
            try {
                auto rule_registry = base_->getSerde().getRuleRegistry();
                if (rule_registry) {
                    auto client = base_->getSerde().getClient();
                    executor->configure(client->getConfiguration(),
                                        config.rule_config);
                }
            } catch (const std::exception &e) {
                throw JsonError("Failed to configure rule executor: " +
                                std::string(e.what()));
            }
        }
    }
}

std::vector<uint8_t> JsonSerializer::serialize(const SerializationContext &ctx,
                                               const nlohmann::json &value) {
    auto mutable_value = value;  // Copy for potential transformation

    // Get subject using strategy
    auto strategy = base_->getConfig().subject_name_strategy;
    auto subject_opt = strategy(ctx.topic, ctx.serde_type, schema_);
    if (!subject_opt.has_value()) {
        throw JsonError("Subject name strategy returned no subject");
    }
    std::string subject = subject_opt.value();

    // Get or register schema
    SchemaId schema_id(SerdeFormat::Json);
    std::optional<srclient::rest::model::RegisteredSchema> latest_schema;

    try {
        latest_schema = base_->getSerde().getReaderSchema(
            subject, std::nullopt, base_->getConfig().use_schema);
    } catch (const std::exception &e) {
        // Schema not found - will use provided schema
    }

    srclient::rest::model::Schema target_schema;
    std::shared_ptr<jsoncons::jsonschema::json_schema<jsoncons::ojson>> parsed_schema;

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
        parsed_schema = getParsedSchema(target_schema);

        // Create field transformer lambda
        auto field_transformer =
            [this, &parsed_schema](
                RuleContext &ctx, const std::string &rule_type,
                const SerdeValue &msg) -> std::unique_ptr<SerdeValue> {
            if (msg.getFormat() == SerdeFormat::Json) {
                auto json = asJson(msg);
                auto transformed = utils::value_transform::transformFields(
                    ctx, parsed_schema, json);
                return makeJsonValue(transformed);
            }
            return msg.clone();
        };

        auto json_value = makeJsonValue(mutable_value);

        // Execute rules on the serde value
        auto transformed_value = base_->getSerde().executeRules(
            ctx, subject, Mode::Write, std::nullopt, target_schema, *json_value,
            {}, std::make_shared<FieldTransformer>(field_transformer));

        // Extract Json value from result
        if (transformed_value->getFormat() == SerdeFormat::Json) {
            mutable_value = asJson(*transformed_value);
        } else {
            throw JsonError(
                "Unexpected serde value type returned from rule execution");
        }
    } else {
        // Use provided schema
        if (!schema_.has_value()) {
            throw JsonError("Schema needs to be set for auto-registration");
        }
        target_schema = schema_.value();

        // Register or get schema
        if (base_->getConfig().auto_register_schemas) {
            auto registered_schema =
                base_->getSerde().getClient()->registerSchema(
                    subject, target_schema,
                    base_->getConfig().normalize_schemas);
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
                subject, target_schema, base_->getConfig().normalize_schemas,
                false);
            auto id_opt = registered_schema.getId();
            if (id_opt.has_value()) {
                schema_id.setId(id_opt.value());
            }
            auto guid_opt = registered_schema.getGuid();
            if (guid_opt.has_value()) {
                schema_id.setGuid(guid_opt.value());
            }
        }

        parsed_schema = getParsedSchema(target_schema);
    }

    // Validate JSON against schema if validation is enabled
    if (base_->getConfig().validate) {
        try {
            validation_utils::validateJson(parsed_schema, mutable_value);
        } catch (const std::exception &e) {
            throw JsonValidationError("JSON validation failed: " +
                                      std::string(e.what()));
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
std::shared_ptr<jsoncons::jsonschema::json_schema<jsoncons::ojson>>
JsonSerializer::getParsedSchema(const srclient::rest::model::Schema &schema) {
    return serde_->getParsedSchema(schema, base_->getSerde().getClient());
}

void JsonSerializer::validateSchema(
    const srclient::rest::model::Schema &schema) {
    auto schema_str = schema.getSchema();
    if (!schema_str.has_value() || schema_str->empty()) {
        throw JsonError("Schema content is empty");
    }

    auto schema_type = schema.getSchemaType();
    if (schema_type.has_value() && schema_type.value() != "JSON") {
        throw JsonError("Schema type must be JSON");
    }
}

std::unique_ptr<SerdeValue> JsonSerializer::transformValue(
    const SerdeValue &value, const Schema &schema, const std::string &subject) {
    // Apply transformations and return as unique_ptr
    // For now, create a copy and return it
    // TODO: Implement actual transformations
    return value.clone();
}

nlohmann::json JsonSerializer::executeFieldTransformations(
    const nlohmann::json &value, const nlohmann::json &schema,
    const RuleContext &context, const std::string &field_executor_type) {
    // TODO: Implement field-level transformations
    return value;
}

}  // namespace srclient::serdes::json