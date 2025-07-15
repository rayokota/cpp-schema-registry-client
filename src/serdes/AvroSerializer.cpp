#include "srclient/serdes/AvroSerializer.h"
#include "srclient/serdes/AvroUtils.h"
#include <algorithm>
#include <sstream>

namespace srclient::serdes {

// AvroSerde implementation

template<typename ClientType>
std::pair<avro::ValidSchema, std::vector<avro::ValidSchema>>
AvroSerde::getParsedSchema(
    const srclient::rest::model::Schema& schema,
    std::shared_ptr<ClientType> client
) {
    // Generate cache key (reuse the updated SerdeTypes cache key generation)
    nlohmann::json j;
    to_json(j, schema);
    std::string cache_key = j.dump();
    
    // Check cache first
    {
        std::shared_lock lock(mutex_);
        auto it = parsed_schemas_.find(cache_key);
        if (it != parsed_schemas_.end()) {
            return it->second;
        }
    }
    
    // Parse schema with references
    std::vector<std::string> named_schema_strings;
    std::unordered_set<std::string> visited;
    resolveNamedSchema(schema, client, named_schema_strings, visited);
    
    // Parse the schema
    if (!schema.getSchema().has_value()) {
        throw AvroSerdeError("Schema string is not available");
    }
    
    auto parsed = avro_utils::parseSchemaWithNamed(
        schema.getSchema().value(),
        named_schema_strings
    );
    
    // Cache the result
    {
        std::unique_lock lock(mutex_);
        parsed_schemas_[cache_key] = parsed;
    }
    
    return parsed;
}

template<typename ClientType>
void AvroSerde::resolveNamedSchema(
    const srclient::rest::model::Schema& schema,
    std::shared_ptr<ClientType> client,
    std::vector<std::string>& schemas,
    std::unordered_set<std::string>& visited
) {
    if (!schema.getReferences().has_value()) {
        return;
    }
    
    for (const auto& ref : schema.getReferences().value()) {
        std::string name = ref.getName().value_or("");
        if (visited.contains(name)) {
            continue;
        }
        visited.insert(name);
        
        // Fetch referenced schema
        std::string subject = ref.getSubject().value_or("");
        int32_t version = ref.getVersion().value_or(-1);
        
        try {
            auto ref_schema = client->getVersion(subject, version, true, std::nullopt);
            auto converted_schema = ref_schema.toSchema();
            
            // Recursively resolve references
            resolveNamedSchema(converted_schema, client, schemas, visited);
            
            // Add this schema's content
            if (ref_schema.getSchema().has_value()) {
                schemas.push_back(ref_schema.getSchema().value());
            }
        } catch (const std::exception& e) {
            throw AvroSerdeError("Failed to resolve schema reference: " + std::string(e.what()));
        }
    }
}

void AvroSerde::clear() {
    std::unique_lock lock(mutex_);
    parsed_schemas_.clear();
}

// Explicit template instantiations
template std::pair<avro::ValidSchema, std::vector<avro::ValidSchema>>
AvroSerde::getParsedSchema<srclient::rest::ISchemaRegistryClient>(
    const srclient::rest::model::Schema& schema,
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client
);

template void AvroSerde::resolveNamedSchema<srclient::rest::ISchemaRegistryClient>(
    const srclient::rest::model::Schema& schema,
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    std::vector<std::string>& schemas,
    std::unordered_set<std::string>& visited
);

// AvroSerializer implementation

template<typename ClientType>
AvroSerializer<ClientType>::AvroSerializer(
    std::shared_ptr<ClientType> client,
    std::optional<srclient::rest::model::Schema> schema,
    std::shared_ptr<RuleRegistry> rule_registry,
    const SerializerConfig& config
) : schema_(std::move(schema)),
    base_(std::make_shared<BaseSerializer>(
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

template<typename ClientType>
std::vector<uint8_t> AvroSerializer<ClientType>::serialize(
    const SerializationContext& ctx,
    const avro::GenericDatum& datum
) {
    auto value = datum; // Copy for potential transformation
    
    // Get subject using strategy
    auto strategy = base_->getConfig().subject_name_strategy;
    auto subject_opt = strategy(ctx.topic, ctx.serde_type, schema_.has_value() ? std::make_optional(schema_.value()) : std::nullopt);
    if (!subject_opt.has_value()) {
        throw AvroSerdeError("Subject name strategy returned no subject");
    }
    std::string subject = subject_opt.value();
    
    // Get or register schema
    SchemaId schema_id(SerdeFormat::Avro);
    std::optional<srclient::rest::model::RegisteredSchema> latest_schema;
    
    try {
        latest_schema = base_->getSerde().getReaderSchema(subject, std::nullopt, base_->getConfig().use_schema);
    } catch (const std::exception& e) {
        // Schema not found - will use provided schema
    }
    
    if (latest_schema.has_value()) {
        // Use latest schema from registry
        schema_id = SchemaId(SerdeFormat::Avro, latest_schema->getId(), latest_schema->getGuid(), std::nullopt);
        
        auto schema = latest_schema->toSchema();
        auto parsed_schema = serde_->getParsedSchema(schema, base_->getSerde().getClient());
        
        // Apply rules if configured  
        if (base_->getSerde().getRuleRegistry()) {
            // TODO: Implement rule execution properly
            // For now, skip rule execution and use original value
            // The RuleContext constructor requires more parameters than we have here
        }
    } else {
        // Use provided schema and register/lookup
        if (!schema_.has_value()) {
            throw AvroSerdeError("No schema provided and none found in registry");
        }
        
        srclient::rest::model::RegisteredSchema registered_schema;
        if (base_->getConfig().auto_register_schemas) {
            registered_schema = base_->getSerde().getClient()->registerSchema(
                subject, schema_.value(), base_->getConfig().normalize_schemas);
        } else {
            registered_schema = base_->getSerde().getClient()->getBySchema(
                subject, schema_.value(), base_->getConfig().normalize_schemas, false);
        }
        
        schema_id = SchemaId(SerdeFormat::Avro, registered_schema.getId(), 
                           registered_schema.getGuid(), std::nullopt);
    }
    
    // Parse schema for serialization
    auto schema_to_use = latest_schema.has_value() ? latest_schema->toSchema() : schema_.value();
    auto parsed_schema = serde_->getParsedSchema(schema_to_use, base_->getSerde().getClient());
    
    // Serialize Avro data
    auto avro_bytes = avro_utils::serializeAvroData(value, parsed_schema.first, parsed_schema.second);
    
    // Apply encoding rules if present
    if (latest_schema.has_value()) {
        auto schema = latest_schema->toSchema();
        if (schema.getRuleSet().has_value()) {
            auto rule_set = schema.getRuleSet().value();
            if (rule_set.getEncodingRules().has_value()) {
                // TODO: Implement rule execution properly
                // For now, skip rule execution and use original bytes
                // The RuleContext constructor requires more parameters than we have here
            }
        }
    }
    
    // Add schema ID header
    auto id_serializer = base_->getConfig().schema_id_serializer;
    return id_serializer(avro_bytes, ctx, schema_id);
}

template<typename ClientType>
std::vector<uint8_t> AvroSerializer<ClientType>::serializeJson(
    const SerializationContext& ctx,
    const nlohmann::json& json_value
) {
    if (!schema_.has_value()) {
        throw AvroSerdeError("Schema required for JSON to Avro conversion");
    }
    
    auto parsed_schema = serde_->getParsedSchema(schema_.value(), base_->getSerde().getClient());
    auto datum = avro_utils::jsonToAvro(json_value, parsed_schema.first);
    
    return serialize(ctx, datum);
}

template<typename ClientType>
void AvroSerializer<ClientType>::close() {
    if (serde_) {
        serde_->clear();
    }
}

template<typename ClientType>
std::pair<avro::ValidSchema, std::vector<avro::ValidSchema>> 
AvroSerializer<ClientType>::getParsedSchema(const srclient::rest::model::Schema& schema) {
    return serde_->getParsedSchema(schema, base_->getSerde().getClient());
}

template<typename ClientType>
avro::GenericDatum AvroSerializer<ClientType>::jsonToAvro(
    const nlohmann::json& json_value,
    const avro::ValidSchema& schema
) {
    return avro_utils::jsonToAvro(json_value, schema);
}

template<typename ClientType>
avro::GenericDatum AvroSerializer<ClientType>::transformFields(
    RuleContext& ctx,
    const avro::GenericDatum& datum,
    const avro::ValidSchema& schema
) {
    // Field transformation logic would be implemented here
    // For now, return the datum unchanged
    return datum;
}

// Explicit template instantiation for the main client type
template class AvroSerializer<srclient::rest::ISchemaRegistryClient>;

} // namespace srclient::serdes 