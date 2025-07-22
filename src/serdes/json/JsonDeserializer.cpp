#include "srclient/serdes/json/JsonDeserializer.h"
#include "srclient/serdes/json/JsonUtils.h"

namespace srclient::serdes::json {

using namespace utils;

JsonDeserializer::JsonDeserializer(
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    std::shared_ptr<RuleRegistry> rule_registry,
    const DeserializerConfig &config)
    : base_(std::make_shared<BaseDeserializer>(Serde(client, rule_registry),
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

nlohmann::json JsonDeserializer::deserialize(const SerializationContext &ctx,
                                             const std::vector<uint8_t> &data) {
    // Determine subject
    auto strategy = base_->getConfig().subject_name_strategy;
    auto subject_opt = strategy(ctx.topic, ctx.serde_type, std::nullopt);
    std::optional<srclient::rest::model::RegisteredSchema> latest_schema;
    bool has_subject = subject_opt.has_value();

    if (has_subject) {
        latest_schema = base_->getSerde().getReaderSchema(
            subject_opt.value(), std::nullopt, base_->getConfig().use_schema);
    }

    // Parse schema ID from data
    SchemaId schema_id(SerdeFormat::Json);
    auto id_deserializer = base_->getConfig().schema_id_deserializer;
    size_t bytes_read = id_deserializer(data, ctx, schema_id);

    const uint8_t *message_data = data.data() + bytes_read;
    size_t message_size = data.size() - bytes_read;

    // Get writer schema
    auto writer_schema_raw =
        base_->getWriterSchema(schema_id, subject_opt, std::nullopt);
    auto [writer_schema, writer_schema_doc] =
        getParsedSchema(writer_schema_raw);

    // Re-determine subject if needed
    if (!has_subject) {
        subject_opt = strategy(ctx.topic, ctx.serde_type,
                               std::make_optional(writer_schema_raw));
        if (subject_opt.has_value()) {
            latest_schema = base_->getSerde().getReaderSchema(
                subject_opt.value(), std::nullopt,
                base_->getConfig().use_schema);
        }
    }

    if (!subject_opt.has_value()) {
        throw JsonError("Could not determine subject name");
    }
    std::string subject = subject_opt.value();

    // Handle encoding rules
    std::vector<uint8_t> decoded_data;
    auto rule_set = writer_schema_raw.getRuleSet();
    if (rule_set.has_value()) {
        // TODO: Implement encoding rule execution
        decoded_data.assign(message_data, message_data + message_size);
        message_data = decoded_data.data();
        message_size = decoded_data.size();
    }

    // Schema evolution handling
    std::vector<Migration> migrations;
    srclient::rest::model::Schema reader_schema_raw;
    nlohmann::json reader_schema;
    std::optional<std::string> reader_schema_str;

    if (latest_schema.has_value()) {
        // Schema evolution path
        migrations = base_->getSerde().getMigrations(
            subject, writer_schema_raw, latest_schema.value(), std::nullopt);
        reader_schema_raw = latest_schema->toSchema();
        auto [parsed_reader_schema, parsed_reader_schema_str] =
            getParsedSchema(reader_schema_raw);
        reader_schema = parsed_reader_schema;
        reader_schema_str = parsed_reader_schema_str;
    } else {
        // No evolution - writer and reader schemas are the same
        migrations = {};
        reader_schema_raw = writer_schema_raw;
        reader_schema = writer_schema;
        reader_schema_str = writer_schema_doc;
    }

    // Parse JSON from bytes
    std::string json_string(reinterpret_cast<const char *>(message_data),
                            message_size);
    nlohmann::json value;
    try {
        value = nlohmann::json::parse(json_string);
    } catch (const nlohmann::json::parse_error &e) {
        throw JsonError("Failed to parse JSON: " + std::string(e.what()));
    }

    // Apply migrations if needed
    if (!migrations.empty()) {
        value = executeMigrations(ctx, subject, migrations, value);
    }

    // Apply transformation rules
    if (base_->getSerde().getRuleRegistry()) {
        // TODO: Implement rule execution for JSON messages
        // This would involve field transformations similar to Avro/Protobuf
    }

    // Validate JSON against reader schema if validation is enabled
    if (base_->getConfig().validate) {
        try {
            validateJson(value, reader_schema);
        } catch (const std::exception &e) {
            throw JsonValidationError("JSON validation failed: " +
                                      std::string(e.what()));
        }
    }

    return value;
}

void JsonDeserializer::close() {
    // Cleanup resources
    serde_->clear();
}

// Helper method implementations
std::pair<nlohmann::json, std::optional<std::string>>
JsonDeserializer::getParsedSchema(const srclient::rest::model::Schema &schema) {
    return serde_->getParsedSchema(schema, base_->getSerde().getClient());
}

bool JsonDeserializer::validateJson(const nlohmann::json &value,
                                    const nlohmann::json &schema) {
    return serde_->validateJson(value, schema);
}

nlohmann::json JsonDeserializer::executeFieldTransformations(
    const nlohmann::json &value, const nlohmann::json &schema,
    const RuleContext &context, const std::string &field_executor_type) {
    // TODO: Implement field-level transformations
    return value;
}

nlohmann::json JsonDeserializer::executeMigrations(
    const SerializationContext &ctx, const std::string &subject,
    const std::vector<Migration> &migrations, const nlohmann::json &value) {

    // Convert to SerdeValue for migration execution
    auto serde_value = makeJsonValue(value);
    auto migrated_value = base_->getSerde().executeMigrations(
        ctx, subject, migrations, *serde_value);

    // Convert back to nlohmann::json
    return asJson(*migrated_value);
}

bool JsonDeserializer::isEvolutionRequired(
    const srclient::rest::model::Schema &writer_schema,
    const srclient::rest::model::Schema &reader_schema) {

    // Compare schema content to determine if evolution is needed
    auto writer_schema_str = writer_schema.getSchema();
    auto reader_schema_str = reader_schema.getSchema();

    return writer_schema_str != reader_schema_str;
}

} // namespace srclient::serdes::json