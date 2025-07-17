#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <optional>
#include <string>

// Avro C++ includes
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Compiler.hh>
#include <avro/ValidSchema.hh>
#include <avro/Generic.hh>
#include <avro/Specific.hh>

// Project includes
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/avro/AvroTypes.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/rest/model/Schema.h"
#include <nlohmann/json.hpp>

namespace srclient::serdes::avro {

/**
 * Utility functions for Avro schema and data manipulation
 */
namespace utils {

    /**
     * Convert Avro schema type to FieldType enum
     * @param schema Avro schema to convert
     * @return Corresponding FieldType
     */
    FieldType avroSchemaToFieldType(const ::avro::ValidSchema& schema);

    /**
     * Convert Avro GenericDatum to JSON
     * @param datum Avro datum to convert
     * @return JSON representation
     */
    nlohmann::json avroToJson(const ::avro::GenericDatum& datum);

    /**
     * Convert JSON to Avro GenericDatum
     * @param json_value JSON value to convert
     * @param schema Avro schema to guide conversion
     * @param input_datum Optional template datum for type hints
     * @return Converted Avro datum
     */
    ::avro::GenericDatum jsonToAvro(
        const nlohmann::json& json_value,
        const ::avro::ValidSchema& schema,
        const ::avro::GenericDatum* input_datum = nullptr
    );

    /**
     * Resolve union schema branch for a given datum
     * @param union_schema Union schema
     * @param datum Datum to match against union branches
     * @return Pair of branch index and corresponding schema
     */
    std::pair<size_t, ::avro::ValidSchema> resolveUnion(
        const ::avro::ValidSchema& union_schema,
        const ::avro::GenericDatum& datum
    );

    /**
     * Extract schema name from Avro ValidSchema
     * @param schema Avro schema
     * @return Optional schema name
     */
    std::optional<std::string> getSchemaName(const ::avro::ValidSchema& schema);

    /**
     * Extract inline tags from Avro record field attributes
     * @param record Avro record
     * @param field_name Field name to extract tags for
     * @return Set of tags found in field attributes
     */
    std::unordered_set<std::string> getInlineTags(
        const ::avro::GenericRecord& record, 
        const std::string& field_name
    );

    /**
     * Serialize Avro datum to byte array
     * @param datum Avro datum to serialize
     * @param writer_schema Schema to use for writing
     * @param named_schemas Additional named schemas for resolution
     * @return Serialized bytes
     */
    std::vector<uint8_t> serializeAvroData(
        const ::avro::GenericDatum& datum,
        const ::avro::ValidSchema& writer_schema,
        const std::vector<::avro::ValidSchema>& named_schemas = {}
    );

    /**
     * Deserialize byte array to Avro datum
     * @param data Serialized bytes
     * @param writer_schema Schema used for writing
     * @param reader_schema Optional reader schema for schema evolution
     * @param named_schemas Additional named schemas for resolution
     * @return Deserialized Avro datum
     */
    ::avro::GenericDatum deserializeAvroData(
        const std::vector<uint8_t>& data,
        const ::avro::ValidSchema& writer_schema,
        const ::avro::ValidSchema* reader_schema = nullptr,
        const std::vector<::avro::ValidSchema>& named_schemas = {}
    );

    /**
     * Parse Avro schema string with named schema support
     * @param schema_str Main schema string
     * @param named_schemas Vector of named schema strings
     * @return Tuple of parsed main schema and named schemas
     */
    std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>
    parseSchemaWithNamed(
        const std::string& schema_str,
        const std::vector<std::string>& named_schemas = {}
    );

    /**
     * Validate schema compatibility between writer and reader
     * @param writer_schema Writer schema
     * @param reader_schema Reader schema
     * @return True if schemas are compatible
     */
    bool isSchemaCompatible(
        const ::avro::ValidSchema& writer_schema,
        const ::avro::ValidSchema& reader_schema
    );

} // namespace utils



/**
 * RAII wrapper for Avro encoders/decoders with automatic stream management
 */
class AvroStreamManager {
public:
    /**
     * Create encoder for serialization
     * @return Unique pointer to binary encoder
     */
    static std::unique_ptr<::avro::Encoder> createEncoder();

    /**
     * Create decoder for deserialization
     * @param data Input data buffer
     * @return Unique pointer to binary decoder
     */
    static std::unique_ptr<::avro::Decoder> createDecoder(const std::vector<uint8_t>& data);

    /**
     * Get serialized data from encoder stream
     * @param encoder Encoder to extract data from
     * @return Serialized bytes
     */
    static std::vector<uint8_t> getEncodedData(::avro::Encoder& encoder);
};

} // namespace srclient::serdes::avro 