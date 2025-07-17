#include "srclient/serdes/avro/AvroDeserializer.h"
#include "srclient/serdes/avro/AvroUtils.h"
#include <algorithm>
#include <sstream>

namespace srclient::serdes::avro {

// AvroDeserializer implementation

AvroDeserializer::AvroDeserializer(
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    std::shared_ptr<RuleRegistry> rule_registry,
    const DeserializerConfig& config
) :     base_(std::make_shared<BaseDeserializer>(
        Serde(client, rule_registry), config)),
    serde_(std::make_shared<AvroSerde>())
{
    // Configure rule executors
    if (rule_registry) {
        auto executors = rule_registry->getExecutors();
        for (const auto& executor : executors) {
            try {
                // TODO: Fix ClientConfiguration vs ServerConfig conversion
                // executor->configure(client->getConfig("default"), config.rule_config);
            } catch (const std::exception& e) {
                throw AvroSerdeError("Failed to configure rule executor: " + std::string(e.what()));
            }
        }
    }
}

NamedValue AvroDeserializer::deserialize(
    const SerializationContext& ctx,
    const std::vector<uint8_t>& data
) {
    // Get subject using strategy
    auto strategy = base_->getConfig().subject_name_strategy;
    auto subject_opt = strategy(ctx.topic, ctx.serde_type, std::nullopt);
    std::optional<srclient::rest::model::RegisteredSchema> latest_schema;
    bool has_subject = subject_opt.has_value();
    
    if (has_subject) {
        try {
            latest_schema = base_->getSerde().getReaderSchema(
                subject_opt.value(), std::nullopt, base_->getConfig().use_schema);
        } catch (const std::exception& e) {
            // Schema not found - will be determined from writer schema
        }
    }
    
    // Extract schema ID from data
    SchemaId schema_id(SerdeFormat::Avro);
    auto id_deserializer = base_->getConfig().schema_id_deserializer;
    size_t bytes_read = id_deserializer(data, ctx, schema_id);
    std::vector<uint8_t> payload_data(data.begin() + bytes_read, data.end());
    
    // Get writer schema
    auto writer_schema_raw = base_->getWriterSchema(schema_id, subject_opt, std::nullopt);
    auto writer_parsed = serde_->getParsedSchema(writer_schema_raw, base_->getSerde().getClient());
    
    // Update subject if not initially determined
    if (!has_subject) {
        subject_opt = strategy(ctx.topic, ctx.serde_type, std::make_optional(writer_schema_raw));
        if (subject_opt.has_value()) {
            try {
                latest_schema = base_->getSerde().getReaderSchema(
                    subject_opt.value(), std::nullopt, base_->getConfig().use_schema);
            } catch (const std::exception& e) {
                // Schema not found
            }
        }
    }
    
    if (!subject_opt.has_value()) {
        throw AvroSerdeError("Could not determine subject for deserialization");
    }
    std::string subject = subject_opt.value();
    
    // Apply encoding rules if present (pre-decode)
    if (writer_schema_raw.getRuleSet().has_value()) {
        auto rule_set = writer_schema_raw.getRuleSet().value();
        if (rule_set.getEncodingRules().has_value()) {
            // TODO: Implement rule execution properly
            // For now, skip rule execution and use original payload
            // The RuleContext constructor requires more parameters than we have here
        }
    }
    
    // Migrations processing
    std::vector<Migration> migrations;
    srclient::rest::model::Schema reader_schema_raw;
    std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>> reader_parsed;
    
    if (latest_schema.has_value()) {
        // Schema evolution path
        migrations = base_->getSerde().getMigrations(
            subject, writer_schema_raw, latest_schema.value(), std::nullopt);
        reader_schema_raw = latest_schema->toSchema();
        reader_parsed = serde_->getParsedSchema(reader_schema_raw, base_->getSerde().getClient());
    } else {
        // No evolution - writer and reader schemas are the same
        reader_schema_raw = writer_schema_raw;
        reader_parsed = writer_parsed;
    }
    
    // Deserialize Avro data
    ::avro::GenericDatum value;
    if (latest_schema.has_value()) {
        // Two-step process for schema evolution
        // 1. Deserialize with writer schema
        auto intermediate = utils::deserializeAvroData(
            payload_data, writer_parsed.first, nullptr, writer_parsed.second);
        
        // 2. Convert to JSON for migration
        auto json_value = utils::avroToJson(intermediate);
        auto json_serde_value = srclient::serdes::json::makeJsonValue(json_value);
        
        // 3. Apply migrations
        auto& migrated = base_->getSerde().executeMigrations(ctx, subject, migrations, *json_serde_value);
        auto migrated_json = std::any_cast<nlohmann::json>(migrated.getValue());
        
        // 4. Convert back to Avro with reader schema
        value = utils::jsonToAvro(migrated_json, reader_parsed.first, &intermediate);
    } else {
        // Direct deserialization without evolution
        value = utils::deserializeAvroData(
            payload_data, writer_parsed.first, &reader_parsed.first, reader_parsed.second);
    }
    
    // Apply transformation rules
    if (base_->getSerde().getRuleRegistry()) {
        auto serde_value = makeAvroValue(value);
        auto avro_schema = makeAvroSchema(reader_parsed);
        
        auto& transformed = base_->getSerde().executeRules(ctx, subject, Mode::Read, 
                                                         std::nullopt, std::make_optional(reader_schema_raw),
                                                         std::make_optional(avro_schema.get()), *serde_value);
        if (transformed.isAvro()) {
            value = std::any_cast<::avro::GenericDatum>(transformed.getValue());
        }
    }
    
    return NamedValue{getName(reader_parsed.first), std::move(value)};
}

nlohmann::json AvroDeserializer::deserializeToJson(
    const SerializationContext& ctx,
    const std::vector<uint8_t>& data
) {
    auto named_value = deserialize(ctx, data);
    return utils::avroToJson(named_value.value);
}

void AvroDeserializer::close() {
    if (serde_) {
        serde_->clear();
    }
}

std::optional<std::string> AvroDeserializer::getName(const ::avro::ValidSchema& schema) {
    return utils::getSchemaName(schema);
}

std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>
AvroDeserializer::getParsedSchema(const srclient::rest::model::Schema& schema) {
    return serde_->getParsedSchema(schema, base_->getSerde().getClient());
}

nlohmann::json AvroDeserializer::avroToJson(const ::avro::GenericDatum& datum) {
    return utils::avroToJson(datum);
}

::avro::GenericDatum AvroDeserializer::jsonToAvro(
    const ::avro::GenericDatum& input_datum,
    const nlohmann::json& json_value
) {
    // Extract schema from input datum
    // This is a simplified approach - in practice, you'd need the ValidSchema
    throw AvroSerdeError("jsonToAvro with input datum template not implemented");
}
::avro::GenericDatum AvroDeserializer::transformFields(
    RuleContext& ctx,
    const ::avro::GenericDatum& datum,
    const ::avro::ValidSchema& schema
) {
    // Field transformation logic would be implemented here
    // This would recursively process records, arrays, maps, and unions
    // applying field-level rules as configured
    
    switch (schema.root()->type()) {
        case ::avro::AVRO_RECORD: {
            auto record = datum.value<::avro::GenericRecord>();
            ::avro::GenericRecord result(schema.root());
            
            for (size_t i = 0; i < record.fieldCount(); ++i) {
                auto field = record.fieldAt(i);
                auto field_schema_node = schema.root()->leafAt(i);
                ::avro::ValidSchema field_schema(field_schema_node);
                auto transformed = transformFields(ctx, field, field_schema);
                result.setFieldAt(i, transformed);
            }
            
            return ::avro::GenericDatum(schema);
        }
        
        case ::avro::AVRO_ARRAY: {
            auto array = datum.value<::avro::GenericArray>();
            ::avro::GenericArray result(schema.root());
            auto item_schema_node = schema.root()->leafAt(0);
            ::avro::ValidSchema item_schema(item_schema_node);
            
            for (size_t i = 0; i < array.value().size(); ++i) {
                auto transformed = transformFields(ctx, array.value()[i], item_schema);
                result.value().push_back(transformed);
            }
            
            return ::avro::GenericDatum(schema);
        }
        
        case ::avro::AVRO_MAP: {
            auto map = datum.value<::avro::GenericMap>();
            ::avro::GenericMap result(schema.root());
            for (const auto& [key, value] : map.value()) {
                auto value_schema_node = schema.root()->leafAt(0);
                ::avro::ValidSchema value_schema(value_schema_node);
                auto transformed = transformFields(ctx, value, value_schema);
                result.value().emplace_back(key, transformed);
            }
            return ::avro::GenericDatum(schema);
        }
        
        case ::avro::AVRO_UNION: {
            auto union_val = datum.value<::avro::GenericUnion>();
            auto [branch_idx, branch_schema] = resolveUnion(schema, datum);
            auto transformed = transformFields(ctx, union_val.datum(), branch_schema);
            
            ::avro::GenericUnion result(schema.root());
            result.selectBranch(branch_idx);
            result.datum() = transformed;
            
            return ::avro::GenericDatum(schema);
        }
        
        default:
            // For primitive types, apply field rules if applicable
            // This would check if there are any field rules that apply to this field
            return datum;
    }
}

std::pair<size_t, ::avro::ValidSchema> AvroDeserializer::resolveUnion(
    const ::avro::ValidSchema& schema,
    const ::avro::GenericDatum& datum
) {
    return utils::resolveUnion(schema, datum);
}

FieldType AvroDeserializer::getFieldType(const ::avro::ValidSchema& schema) {
    return utils::avroSchemaToFieldType(schema);
}

std::unordered_set<std::string> AvroDeserializer::getInlineTags(
    const ::avro::GenericRecord& record, 
    const std::string& field_name
) {
    return utils::getInlineTags(record, field_name);
}

} // namespace srclient::serdes::avro