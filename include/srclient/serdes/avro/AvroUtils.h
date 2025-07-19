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
     * Apply field transformation rules
     * @param ctx Rule context
     * @param datum Avro datum to transform
     * @param schema Schema for the datum
     * @return Transformed datum
     */
    ::avro::GenericDatum transformFields(
        RuleContext& ctx,
        const ::avro::GenericDatum& datum,
        const ::avro::ValidSchema& schema
    );

    /**
     * Transform individual field with context handling
     * @param ctx Rule context
     * @param record_schema Schema of the parent record
     * @param field_name Name of the field
     * @param field_datum Field datum to transform
     * @param field_schema Schema of the field
     * @return Transformed field datum
     */
    ::avro::GenericDatum transformFieldWithCtx(
        RuleContext& ctx,
        const ::avro::ValidSchema& record_schema,
        const std::string& field_name,
        const ::avro::GenericDatum& field_datum,
        const ::avro::ValidSchema& field_schema
    );

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
     * @return Converted Avro datum
     */
    ::avro::GenericDatum jsonToAvro(
        const nlohmann::json& json_value,
        const ::avro::ValidSchema& schema
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

} // namespace srclient::serdes::avro 