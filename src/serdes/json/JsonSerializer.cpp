#include "schemaregistry/serdes/json/JsonSerializer.h"

#include "schemaregistry/serdes/json/JsonUtils.h"

namespace schemaregistry::serdes::json {

using namespace utils;

// JsonSerde implementation
JsonSerde::JsonSerde() {}

std::shared_ptr<jsoncons::jsonschema::json_schema<jsoncons::ojson>>
JsonSerde::getParsedSchema(
    const schemaregistry::rest::model::Schema &schema,
    std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client) {
    std::lock_guard<std::mutex> lock(cache_mutex_);

    // Create cache key from schema content
    auto schema_str = schema.getSchema();
    std::string cache_key = schema_str.value_or("");

    auto it = parsed_schemas_cache_.find(cache_key);
    if (it != parsed_schemas_cache_.end()) {
        return it->second;
    }

    // Parse schema with references
    std::unordered_set<std::string> visited;
    auto resolved_refs =
        schema_resolution::resolveNamedSchema(schema, client, visited);
    auto resolver =
        [resolved_refs](const jsoncons::uri &uri) -> jsoncons::ojson {
        // Look up the exact URI string in the resolved references
        auto key = uri.string();
        auto it = resolved_refs.find(key);
        if (it != resolved_refs.end()) {
            return jsonToOJson(it->second);
        }

        // Fallback: try using only the path component
        auto path_only = uri.path();
        if (!path_only.empty() && path_only.front() == '/') {
            path_only.erase(0, 1);
        }
        if (!path_only.empty()) {
            auto it2 = resolved_refs.find(path_only);
            if (it2 != resolved_refs.end()) {
                return jsonToOJson(it2->second);
            }
        }

        // If not found, return an empty object
        return jsoncons::ojson::null();
    };

    // Parse new schema
    nlohmann::json parsed_schema;
    try {
        parsed_schema = nlohmann::json::parse(cache_key);
    } catch (const nlohmann::json::parse_error &e) {
        throw JsonError("Failed to parse JSON schema: " +
                        std::string(e.what()));
    }

    auto jsoncons_schema = jsonToOJson(parsed_schema);
    auto compiled_schema =
        std::make_shared<jsoncons::jsonschema::json_schema<jsoncons::ojson>>(
            jsoncons::jsonschema::make_json_schema(jsoncons_schema, resolver));

    // Store in cache
    parsed_schemas_cache_[cache_key] = compiled_schema;

    return compiled_schema;
}

void JsonSerde::clear() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    parsed_schemas_cache_.clear();
}

void JsonSerde::resolveNamedSchema(
    const schemaregistry::rest::model::Schema &schema,
    std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client,
    std::unordered_map<std::string, std::string> references) {
    if (schema.getReferences().has_value()) {
        for (const auto &ref : schema.getReferences().value()) {
            if (!ref.getName().has_value() || !ref.getSubject().has_value()) {
                continue;  // Skip invalid references
            }
            if (references.find(ref.getName().value()) == references.end()) {
                // Resolve reference recursively
                auto ref_schema =
                    client->getVersion(ref.getSubject().value(),
                                       ref.getVersion().value_or(-1), true);
                resolveNamedSchema(ref_schema.toSchema(), client, references);
            }
        }
    }
}

class JsonSerializer::Impl {
  public:
    Impl(std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client,
         std::optional<schemaregistry::rest::model::Schema> schema,
         std::shared_ptr<RuleRegistry> rule_registry,
         const SerializerConfig &config)
        : schema_(std::move(schema)),
          base_(std::make_shared<BaseSerializer>(
              Serde(std::move(client), rule_registry), config)),
          serde_(std::make_unique<JsonSerde>()) {
        std::vector<std::shared_ptr<RuleExecutor>> executors;
        if (rule_registry) {
            executors = rule_registry->getExecutors();
        } else {
            executors = global_registry::getRuleExecutors();
        }
        for (const auto &executor : executors) {
            try {
                auto cfg = base_->getSerde().getClient()->getConfiguration();
                executor->configure(cfg, config.rule_config);
            } catch (const std::exception &e) {
                throw JsonError("Failed to configure rule executor: " +
                                std::string(e.what()));
            }
        }
    }

    std::vector<uint8_t> serialize(const SerializationContext &ctx,
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
        std::optional<schemaregistry::rest::model::RegisteredSchema>
            latest_schema;

        try {
            latest_schema = base_->getSerde().getReaderSchema(
                subject, std::nullopt, base_->getConfig().use_schema);
        } catch (const std::exception &e) {
            // Schema not found - will use provided schema
        }

        schemaregistry::rest::model::Schema target_schema;
        std::shared_ptr<jsoncons::jsonschema::json_schema<jsoncons::ojson>>
            parsed_schema;

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
                ctx, subject, Mode::Write, std::nullopt, target_schema,
                *json_value, {},
                std::make_shared<FieldTransformer>(field_transformer));

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
                auto registered_schema =
                    base_->getSerde().getClient()->getBySchema(
                        subject, target_schema,
                        base_->getConfig().normalize_schemas, false);
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
        std::vector<uint8_t> encoded_bytes(json_string.begin(),
                                           json_string.end());

        // Apply encoding rules if present
        auto rule_set = target_schema.getRuleSet();
        if (rule_set.has_value()) {
            // TODO: Implement encoding rule execution
        }

        // Serialize schema ID with message
        auto id_serializer = base_->getConfig().schema_id_serializer;
        return id_serializer(encoded_bytes, ctx, schema_id);
    }

    void close() { serde_->clear(); }

    std::shared_ptr<jsoncons::jsonschema::json_schema<jsoncons::ojson>>
    getParsedSchema(const schemaregistry::rest::model::Schema &schema) {
        return serde_->getParsedSchema(schema, base_->getSerde().getClient());
    }

  private:
    std::optional<schemaregistry::rest::model::Schema> schema_;
    std::shared_ptr<BaseSerializer> base_;
    std::unique_ptr<JsonSerde> serde_;
};

JsonSerializer::JsonSerializer(
    std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client,
    std::optional<schemaregistry::rest::model::Schema> schema,
    std::shared_ptr<RuleRegistry> rule_registry, const SerializerConfig &config)
    : impl_(std::make_unique<Impl>(std::move(client), std::move(schema),
                                   std::move(rule_registry), config)) {}

JsonSerializer::~JsonSerializer() = default;

std::vector<uint8_t> JsonSerializer::serialize(const SerializationContext &ctx,
                                               const nlohmann::json &value) {
    return impl_->serialize(ctx, value);
}

void JsonSerializer::close() { impl_->close(); }

}  // namespace schemaregistry::serdes::json