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
#include "schemaregistry/rest/SchemaRegistryClient.h"
#include "schemaregistry/rest/model/Schema.h"
#include "schemaregistry/serdes/Serde.h"
#include "schemaregistry/serdes/SerdeConfig.h"
#include "schemaregistry/serdes/SerdeError.h"
#include "schemaregistry/serdes/SerdeTypes.h"
#include "schemaregistry/serdes/avro/AvroSerializer.h"  // For AvroSerde and NamedValue
#include "schemaregistry/serdes/avro/AvroTypes.h"

namespace schemaregistry::serdes::avro {

// Forward declarations from parent namespace
using SerializationContext = schemaregistry::serdes::SerializationContext;
using BaseDeserializer = schemaregistry::serdes::BaseDeserializer;

/**
 * Named value container for Avro deserialization results
 */
struct NamedValue {
    std::optional<std::string> name;
    ::avro::GenericDatum value;

    NamedValue() = default;
    NamedValue(std::optional<std::string> n, ::avro::GenericDatum v)
            : name(std::move(n)), value(std::move(v)) {}
};

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
        std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client,
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
    getParsedSchema(const schemaregistry::rest::model::Schema &schema);

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

}  // namespace schemaregistry::serdes::avro