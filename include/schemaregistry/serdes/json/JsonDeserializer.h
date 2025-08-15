#pragma once

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>

#include "schemaregistry/rest/ISchemaRegistryClient.h"
#include "schemaregistry/serdes/Serde.h"
#include "schemaregistry/serdes/SerdeConfig.h"
#include "schemaregistry/serdes/SerdeError.h"
#include "schemaregistry/serdes/SerdeTypes.h"
#include "schemaregistry/serdes/json/JsonSerializer.h"  // For JsonSerde
#include "schemaregistry/serdes/json/JsonTypes.h"

namespace schemaregistry::serdes::json {

// Forward declarations
class JsonDeserializer;
using SerializationContext = schemaregistry::serdes::SerializationContext;

/**
 * JSON deserializer class template
 * Based on JsonDeserializer from json.rs (converted to synchronous)
 */
class JsonDeserializer {
  public:
    /**
     * Constructor
     */
    JsonDeserializer(
        std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client,
        std::shared_ptr<RuleRegistry> rule_registry,
        const DeserializerConfig &config);

    /**
     * Deserialize bytes to JSON object with schema validation and migration
     * @param ctx Serialization context (topic, serde type, etc.)
     * @param data Serialized bytes with schema ID header
     * @return Deserialized JSON object
     */
    nlohmann::json deserialize(const SerializationContext &ctx,
                               const std::vector<uint8_t> &data);

    /**
     * Close the deserializer and cleanup resources
     */
    void close();

  private:
    std::shared_ptr<BaseDeserializer> base_;
    std::unique_ptr<JsonSerde> serde_;

    // Helper methods
    std::shared_ptr<jsoncons::jsonschema::json_schema<jsoncons::ojson>>
    getParsedSchema(const schemaregistry::rest::model::Schema &schema);

    nlohmann::json executeFieldTransformations(
        const nlohmann::json &value, const nlohmann::json &schema,
        const RuleContext &context, const std::string &field_executor_type);

    nlohmann::json executeMigrations(const SerializationContext &ctx,
                                     const std::string &subject,
                                     const std::vector<Migration> &migrations,
                                     const nlohmann::json &value);

    bool isEvolutionRequired(
        const schemaregistry::rest::model::Schema &writer_schema,
        const schemaregistry::rest::model::Schema &reader_schema);
};

}  // namespace schemaregistry::serdes::json