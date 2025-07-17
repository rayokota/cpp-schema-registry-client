#include "srclient/serdes/avro/AvroUtils.h"
#include <avro/Stream.hh>
#include <avro/Exception.hh>
#include <sstream>
#include <iostream>

namespace srclient::serdes::avro {

namespace utils {

FieldType avroSchemaToFieldType(const ::avro::ValidSchema& schema) {
    switch (schema.root()->type()) {
        case ::avro::AVRO_NULL:
            return FieldType::Null;
        case ::avro::AVRO_BOOL:
            return FieldType::Boolean;
        case ::avro::AVRO_INT:
            return FieldType::Int;
        case ::avro::AVRO_LONG:
            return FieldType::Long;
        case ::avro::AVRO_FLOAT:
            return FieldType::Float;
        case ::avro::AVRO_DOUBLE:
            return FieldType::Double;
        case ::avro::AVRO_BYTES:
            return FieldType::Bytes;
        case ::avro::AVRO_STRING:
            return FieldType::String;
        case ::avro::AVRO_RECORD:
            return FieldType::Record;
        case ::avro::AVRO_ENUM:
            return FieldType::Enum;
        case ::avro::AVRO_ARRAY:
            return FieldType::Array;
        case ::avro::AVRO_MAP:
            return FieldType::Map;
        case ::avro::AVRO_UNION:
            return FieldType::Combined;
        case ::avro::AVRO_FIXED:
            return FieldType::Fixed;
        case ::avro::AVRO_SYMBOLIC:
            return FieldType::Record; // Assume symbolic references are records
        default:
            return FieldType::String; // Default fallback
    }
}

nlohmann::json avroToJson(const ::avro::GenericDatum& datum) {
    switch (datum.type()) {
        case ::avro::AVRO_NULL:
            return nlohmann::json(nullptr);
            
        case ::avro::AVRO_BOOL:
            return nlohmann::json(datum.value<bool>());
            
        case ::avro::AVRO_INT:
            return nlohmann::json(datum.value<int32_t>());
            
        case ::avro::AVRO_LONG:
            return nlohmann::json(datum.value<int64_t>());
            
        case ::avro::AVRO_FLOAT:
            return nlohmann::json(datum.value<float>());
            
        case ::avro::AVRO_DOUBLE:
            return nlohmann::json(datum.value<double>());
            
        case ::avro::AVRO_STRING:
            return nlohmann::json(datum.value<std::string>());
            
        case ::avro::AVRO_BYTES: {
            const auto& bytes = datum.value<std::vector<uint8_t>>();
            nlohmann::json array = nlohmann::json::array();
            for (uint8_t byte : bytes) {
                array.push_back(static_cast<unsigned int>(byte));
            }
            return array;
        }
        
        case ::avro::AVRO_FIXED: {
            const auto& fixed = datum.value<::avro::GenericFixed>();
            nlohmann::json array = nlohmann::json::array();
            for (size_t i = 0; i < fixed.value().size(); ++i) {
                array.push_back(static_cast<unsigned int>(fixed.value()[i]));
            }
            return array;
        }
        
        case ::avro::AVRO_ENUM: {
            const auto& enum_val = datum.value<::avro::GenericEnum>();
            return nlohmann::json(enum_val.symbol());
        }
        
        case ::avro::AVRO_ARRAY: {
            const auto& array = datum.value<::avro::GenericArray>();
            nlohmann::json json_array = nlohmann::json::array();
            for (size_t i = 0; i < array.value().size(); ++i) {
                json_array.push_back(avroToJson(array.value()[i]));
            }
            return json_array;
        }
        
        case ::avro::AVRO_MAP: {
            const auto& map = datum.value<::avro::GenericMap>();
            nlohmann::json json_obj = nlohmann::json::object();
            for (const auto& pair : map.value()) {
                json_obj[pair.first] = avroToJson(pair.second);
            }
            return json_obj;
        }
        
        case ::avro::AVRO_RECORD: {
            const auto& record = datum.value<::avro::GenericRecord>();
            nlohmann::json json_obj = nlohmann::json::object();
            for (size_t i = 0; i < record.fieldCount(); ++i) {
                json_obj[record.schema()->nameAt(i)] = avroToJson(record.fieldAt(i));
            }
            return json_obj;
        }
        
        case ::avro::AVRO_UNION: {
            const auto& union_val = datum.value<::avro::GenericUnion>();
            return avroToJson(union_val.datum());
        }
        
        default:
            throw AvroError("Unsupported Avro type for JSON conversion");
    }
}

::avro::GenericDatum jsonToAvro(
    const nlohmann::json& json_value,
    const ::avro::ValidSchema& schema
) {
    switch (schema.root()->type()) {
        case ::avro::AVRO_NULL:
            return ::avro::GenericDatum();
            
        case ::avro::AVRO_BOOL:
            if (!json_value.is_boolean()) {
                throw AvroError("Expected boolean value");
            }
            return ::avro::GenericDatum(json_value.get<bool>());
            
        case ::avro::AVRO_INT:
            if (!json_value.is_number_integer()) {
                throw AvroError("Expected integer value");
            }
            return ::avro::GenericDatum(json_value.get<int32_t>());
            
        case ::avro::AVRO_LONG:
            if (!json_value.is_number()) {
                throw AvroError("Expected number value for long");
            }
            return ::avro::GenericDatum(json_value.get<int64_t>());
            
        case ::avro::AVRO_FLOAT:
            if (!json_value.is_number()) {
                throw AvroError("Expected number value for float");
            }
            return ::avro::GenericDatum(json_value.get<float>());
            
        case ::avro::AVRO_DOUBLE:
            if (!json_value.is_number()) {
                throw AvroError("Expected number value for double");
            }
            return ::avro::GenericDatum(json_value.get<double>());
            
        case ::avro::AVRO_STRING:
            if (!json_value.is_string()) {
                throw AvroError("Expected string value");
            }
            return ::avro::GenericDatum(json_value.get<std::string>());
            
        case ::avro::AVRO_BYTES: {
            if (!json_value.is_array()) {
                throw AvroError("Expected array for bytes");
            }
            std::vector<uint8_t> bytes;
            for (const auto& elem : json_value) {
                if (!elem.is_number_unsigned()) {
                    throw AvroError("Expected unsigned integers in bytes array");
                }
                bytes.push_back(elem.get<uint8_t>());
            }
            return ::avro::GenericDatum(bytes);
        }
        
        case ::avro::AVRO_ENUM: {
            if (!json_value.is_string()) {
                throw AvroError("Expected string value for enum");
            }
            ::avro::GenericDatum datum(schema);
            auto& enum_val = datum.value<::avro::GenericEnum>();
            enum_val.set(json_value.get<std::string>());
            return datum;
        }
        
        case ::avro::AVRO_ARRAY: {
            if (!json_value.is_array()) {
                throw AvroError("Expected array value");
            }
            ::avro::GenericDatum datum(schema);
            auto& array = datum.value<::avro::GenericArray>();
            auto item_schema = schema.root()->leafAt(0);
            ::avro::ValidSchema item_valid_schema(item_schema);
            
            for (const auto& item : json_value) {
                array.value().push_back(jsonToAvro(item, item_valid_schema));
            }
            return datum;
        }
        
        case ::avro::AVRO_MAP: {
            if (!json_value.is_object()) {
                throw AvroError("Expected object value for map");
            }
            ::avro::GenericDatum datum(schema);
            auto& map = datum.value<::avro::GenericMap>();
            auto value_schema = schema.root()->leafAt(0);
            ::avro::ValidSchema value_valid_schema(value_schema);
            
            for (const auto& [key, value] : json_value.items()) {
                map.value().push_back({key, jsonToAvro(value, value_valid_schema)});
            }
            return datum;
        }
        
        case ::avro::AVRO_RECORD: {
            if (!json_value.is_object()) {
                throw AvroError("Expected object value for record");
            }
            ::avro::GenericDatum datum(schema);
            auto& record = datum.value<::avro::GenericRecord>();
            
            for (size_t i = 0; i < schema.root()->leaves(); ++i) {
                const std::string& field_name = schema.root()->nameAt(i);
                auto field_it = json_value.find(field_name);
                if (field_it != json_value.end()) {
                    auto field_schema = schema.root()->leafAt(i);
                    ::avro::ValidSchema field_valid_schema(field_schema);
                    record.setFieldAt(i, jsonToAvro(*field_it, field_valid_schema));
                }
            }
            return datum;
        }
        
        default:
            throw AvroError("Unsupported schema type for JSON to Avro conversion");
    }
}

std::pair<size_t, ::avro::ValidSchema> resolveUnion(
    const ::avro::ValidSchema& union_schema,
    const ::avro::GenericDatum& datum
) {
    if (union_schema.root()->type() != ::avro::AVRO_UNION) {
        throw AvroError("Schema is not a union type");
    }
    
    // Try to find matching branch based on datum type
    for (size_t i = 0; i < union_schema.root()->leaves(); ++i) {
        auto branch_schema = union_schema.root()->leafAt(i);
        if (branch_schema->type() == datum.type()) {
            return {i, ::avro::ValidSchema(branch_schema)};
        }
    }
    
    throw AvroError("No matching union branch found for datum type");
}

std::optional<std::string> getSchemaName(const ::avro::ValidSchema& schema) {
    if (schema.root()->type() == ::avro::AVRO_RECORD) {
        return schema.root()->name().fullname();
    }
    return std::nullopt;
}

std::unordered_set<std::string> getInlineTags(
    const ::avro::GenericRecord& record, 
    const std::string& field_name
) {
    // TODO: Implement this
    // Note: Avro C++ doesn't directly support custom attributes like confluent:tags
    // This would need to be implemented by parsing the schema JSON and extracting
    // custom attributes. For now, return empty set.
    return {};
}

std::vector<uint8_t> serializeAvroData(
    const ::avro::GenericDatum& datum,
    const ::avro::ValidSchema& writer_schema,
    const std::vector<::avro::ValidSchema>& named_schemas
) {
    try {
        auto output_stream = ::avro::memoryOutputStream();
        auto encoder = ::avro::binaryEncoder();
        encoder->init(*output_stream);
        
        ::avro::encode(*encoder, datum);
        encoder->flush();
        
        // Get the data from the output stream
        auto input_stream = ::avro::memoryInputStream(*output_stream);
        const uint8_t* data = nullptr;
        size_t size = 0;
        if (input_stream->next(&data, &size)) {
            return std::vector<uint8_t>(data, data + size);
        }
        return {};
    } catch (const ::avro::Exception& e) {
        throw AvroError(e);
    }
}

::avro::GenericDatum deserializeAvroData(
    const std::vector<uint8_t>& data,
    const ::avro::ValidSchema& writer_schema,
    const ::avro::ValidSchema* reader_schema,
    const std::vector<::avro::ValidSchema>& named_schemas
) {
    try {
        auto input_stream = ::avro::memoryInputStream(data.data(), data.size());
        auto decoder = ::avro::binaryDecoder();
        decoder->init(*input_stream);
        
        ::avro::GenericDatum datum(reader_schema ? *reader_schema : writer_schema);
        ::avro::decode(*decoder, datum);
        
        return datum;
    } catch (const ::avro::Exception& e) {
        throw AvroError(e);
    }
}

std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>
parseSchemaWithNamed(
    const std::string& schema_str,
    const std::vector<std::string>& named_schemas
) {
    // Currently Avro does not support references to named schema
    // The code below is a placeholder for future implementation
    try {
        // Parse named schemas first
        std::vector<::avro::ValidSchema> parsed_named;
        for (const auto& named_schema_str : named_schemas) {
            std::istringstream iss(named_schema_str);
            ::avro::ValidSchema named_schema;
            ::avro::compileJsonSchema(iss, named_schema);
            parsed_named.push_back(named_schema);
        }
        
        // Parse main schema
        std::istringstream main_iss(schema_str);
        ::avro::ValidSchema main_schema;
        ::avro::compileJsonSchema(main_iss, main_schema);
        
        return {main_schema, parsed_named};
    } catch (const ::avro::Exception& e) {
        throw AvroError(e);
    }
}

// TODO: remove?
bool isSchemaCompatible(
    const ::avro::ValidSchema& writer_schema,
    const ::avro::ValidSchema& reader_schema
) {
    // Basic compatibility check - in practice, this would need more sophisticated logic
    // For now, just check if they're the same type
    return writer_schema.root()->type() == reader_schema.root()->type();
}

} // namespace utils

} // namespace srclient::serdes::avro 