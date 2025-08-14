#pragma once

#include <memory>
#include <optional>
#include <vector>

// Avro C++ includes
#include <avro/Compiler.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/Specific.hh>
#include <avro/ValidSchema.hh>

// Project includes
#include "srclient/rest/SchemaRegistryClient.h"
#include "srclient/rest/model/Schema.h"
#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeConfig.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/avro/AvroSerializer.h"  // For AvroSerde and NamedValue
#include "srclient/serdes/avro/AvroTypes.h"

namespace srclient::serdes::avro {

// Forward declarations from parent namespace
using SerializationContext = srclient::serdes::SerializationContext;
using BaseDeserializer = srclient::serdes::BaseDeserializer;

/**
 * Avro-specific deserializer implementation
 * Converts Avro binary format to objects with schema registry integration
 */
class AvroDeserializer {
  public:
    /**
     * Constructor for AvroDeserializer
     * @param client Schema registry client for schema operations
     * @param rule_registry Optional rule registry for field transformations
     * @param config Deserializer configuration
     */
    AvroDeserializer(
        std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
        std::shared_ptr<RuleRegistry> rule_registry,
        const DeserializerConfig &config);

    /**
     * Destructor
     */
    ~AvroDeserializer() = default;

    /**
     * Deserialize bytes to a named Avro value
     * @param ctx Serialization context (topic, serde type, etc.)
     * @param data Serialized bytes with schema ID header
     * @return NamedValue containing the deserialized Avro datum
     */
    NamedValue deserialize(const SerializationContext &ctx,
                           const std::vector<uint8_t> &data);

    /**
     * Deserialize bytes to JSON
     * Converts Avro datum to JSON after deserialization
     * @param ctx Serialization context
     * @param data Serialized bytes with schema ID header
     * @return JSON representation of the deserialized data
     */
    nlohmann::json deserializeToJson(const SerializationContext &ctx,
                                     const std::vector<uint8_t> &data);

    /**
     * Close the deserializer and cleanup resources
     */
    void close();

  private:
    std::shared_ptr<BaseDeserializer> base_;
    std::shared_ptr<AvroSerde> serde_;

    /**
     * Get the schema name from an Avro schema
     * @param schema Avro schema to extract name from
     * @return Optional schema name
     */
    std::optional<std::string> getName(const ::avro::ValidSchema &schema);

    /**
     * Get parsed Avro schema with caching
     * @param schema Schema to parse
     * @return Tuple of main schema and named schemas
     */
    std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>
    getParsedSchema(const srclient::rest::model::Schema &schema);

    /**
     * Resolve union schema for a given datum
     * @param schema Union schema
     * @param datum Datum to resolve against
     * @return Index and schema of the matching union branch
     */
    std::pair<size_t, ::avro::ValidSchema> resolveUnion(
        const ::avro::ValidSchema &schema, const ::avro::GenericDatum &datum);

    /**
     * Get field type from Avro schema
     * @param schema Avro schema
     * @return Corresponding FieldType
     */
    FieldType getFieldType(const ::avro::ValidSchema &schema);
};

}  // namespace srclient::serdes::avro