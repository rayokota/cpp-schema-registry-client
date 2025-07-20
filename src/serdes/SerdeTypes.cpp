#include "srclient/serdes/SerdeTypes.h"
#include "srclient/rest/model/Schema.h"
#include "srclient/rest/model/RegisteredSchema.h"
#include "srclient/serdes/avro/AvroTypes.h"
#include "srclient/serdes/json/JsonTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include <sstream>
#include <avro/Compiler.hh>
#include <avro/ValidSchema.hh>
#include <google/protobuf/message.h>

namespace srclient::serdes {

// SchemaSelectorData static factory methods
SchemaSelectorData SchemaSelectorData::createSchemaId(int32_t id) {
    SchemaSelectorData data;
    data.type = SchemaSelector::SchemaId;
    data.schema_id = id;
    return data;
}

SchemaSelectorData SchemaSelectorData::createLatestVersion() {
    SchemaSelectorData data;
    data.type = SchemaSelector::LatestVersion;
    return data;
}

SchemaSelectorData SchemaSelectorData::createLatestWithMetadata(const std::unordered_map<std::string, std::string>& metadata) {
    SchemaSelectorData data;
    data.type = SchemaSelector::LatestWithMetadata;
    data.metadata = metadata;
    return data;
}

// Migration implementation
Migration::Migration(Mode mode, 
                    std::optional<RegisteredSchema> src,
                    std::optional<RegisteredSchema> tgt)
    : rule_mode(mode), source(src), target(tgt) {}

// FieldType to string conversion
std::string fieldTypeToString(FieldType type) {
    switch (type) {
        case FieldType::Record: return "RECORD";
        case FieldType::Enum: return "ENUM";
        case FieldType::Array: return "ARRAY";
        case FieldType::Map: return "MAP";
        case FieldType::Combined: return "COMBINED";
        case FieldType::Fixed: return "FIXED";
        case FieldType::String: return "STRING";
        case FieldType::Bytes: return "BYTES";
        case FieldType::Int: return "INT";
        case FieldType::Long: return "LONG";
        case FieldType::Float: return "FLOAT";
        case FieldType::Double: return "DOUBLE";
        case FieldType::Boolean: return "BOOLEAN";
        case FieldType::Null: return "NULL";
        default: return "UNKNOWN";
    }
}

// ParsedSchemaCache template implementation
template<typename T>
void ParsedSchemaCache<T>::set(const Schema& schema, const T& parsed_schema) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string key = getSchemaKey(schema);
    cache_[key] = parsed_schema;
}

template<typename T>
std::optional<T> ParsedSchemaCache<T>::get(const Schema& schema) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string key = getSchemaKey(schema);
    auto it = cache_.find(key);
    if (it != cache_.end()) {
        return it->second;
    }
    return std::nullopt;
}

template<typename T>
void ParsedSchemaCache<T>::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.clear();
}

template<typename T>
std::string ParsedSchemaCache<T>::getSchemaKey(const Schema& schema) const {
    // Us the JSON representation of the schema to create a hash
    nlohmann::json j;
    to_json(j, schema);
    return j.dump();
}

// Explicit template instantiations for common types
template class ParsedSchemaCache<std::string>;
template class ParsedSchemaCache<int>;

// Base64 encoding/decoding utilities
namespace {
    const std::string base64_chars = 
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789+/";

    bool is_base64(unsigned char c) {
        return (isalnum(c) || (c == '+') || (c == '/'));
    }

    std::string base64_encode(const std::vector<uint8_t>& bytes) {
        std::string ret;
        int i = 0;
        int j = 0;
        unsigned char char_array_3[3];
        unsigned char char_array_4[4];
        
        for (size_t idx = 0; idx < bytes.size(); idx++) {
            char_array_3[i++] = bytes[idx];
            if (i == 3) {
                char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
                char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
                char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
                char_array_4[3] = char_array_3[2] & 0x3f;
                
                for (i = 0; (i < 4); i++)
                    ret += base64_chars[char_array_4[i]];
                i = 0;
            }
        }
        
        if (i) {
            for (j = i; j < 3; j++)
                char_array_3[j] = '\0';
                
            char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
            char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
            char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
            char_array_4[3] = char_array_3[2] & 0x3f;
            
            for (j = 0; (j < i + 1); j++)
                ret += base64_chars[char_array_4[j]];
                
            while ((i++ < 3))
                ret += '=';
        }
        
        return ret;
    }

    std::vector<uint8_t> base64_decode(const std::string& encoded_string) {
        size_t in_len = encoded_string.size();
        int i = 0;
        int j = 0;
        int in = 0;
        unsigned char char_array_4[4], char_array_3[3];
        std::vector<uint8_t> ret;
        
        while (in_len-- && (encoded_string[in] != '=') && is_base64(encoded_string[in])) {
            char_array_4[i++] = encoded_string[in]; in++;
            if (i == 4) {
                for (i = 0; i < 4; i++)
                    char_array_4[i] = base64_chars.find(char_array_4[i]);
                    
                char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
                char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
                char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];
                
                for (i = 0; (i < 3); i++)
                    ret.push_back(char_array_3[i]);
                i = 0;
            }
        }
        
        if (i) {
            for (j = i; j < 4; j++)
                char_array_4[j] = 0;
                
            for (j = 0; j < 4; j++)
                char_array_4[j] = base64_chars.find(char_array_4[j]);
                
            char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
            char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
            char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];
            
            for (j = 0; (j < i - 1); j++) ret.push_back(char_array_3[j]);
        }
        
        return ret;
    }
}

// SerdeValue static factory method implementations
std::unique_ptr<SerdeValue> SerdeValue::newString(SerdeFormat format, const std::string& value) {
    switch (format) {
        case SerdeFormat::Avro: {
            ::avro::GenericDatum datum(value);
            return std::make_unique<avro::AvroValue>(datum);
        }
        case SerdeFormat::Json: {
            nlohmann::json json_value = value;
            return std::make_unique<json::JsonValue>(json_value);
        }
        case SerdeFormat::Protobuf: {
            // For protobuf, we cannot create a message from just a string value
            // This would need a specific message type context
            throw SerdeError("Cannot create Protobuf SerdeValue from string without message context");
        }
        default:
            throw SerdeError("Unsupported SerdeFormat");
    }
}

std::unique_ptr<SerdeValue> SerdeValue::newBytes(SerdeFormat format, const std::vector<uint8_t>& value) {
    switch (format) {
        case SerdeFormat::Avro: {
            ::avro::GenericDatum datum(value);
            return std::make_unique<avro::AvroValue>(datum);
        }
        case SerdeFormat::Json: {
            // For JSON, encode bytes as base64 string
            std::string base64_value = base64_encode(value);
            nlohmann::json json_value = base64_value;
            return std::make_unique<json::JsonValue>(json_value);
        }
        case SerdeFormat::Protobuf: {
            // For protobuf, we cannot create a message from just bytes value
            // This would need a specific message type context
            throw SerdeError("Cannot create Protobuf SerdeValue from bytes without message context");
        }
        default:
            throw SerdeError("Unsupported SerdeFormat");
    }
}

namespace type_utils {

std::string formatToString(SerdeFormat format) {
    switch (format) {
        case SerdeFormat::Avro: return "AVRO";
        case SerdeFormat::Json: return "JSON";
        case SerdeFormat::Protobuf: return "PROTOBUF";
        default: return "UNKNOWN";
    }
}

SerdeFormat stringToFormat(const std::string& format) {
    if (format == "AVRO" || format == "avro") {
        return SerdeFormat::Avro;
    } else if (format == "JSON" || format == "json") {
        return SerdeFormat::Json;
    } else if (format == "PROTOBUF" || format == "protobuf") {
        return SerdeFormat::Protobuf;
    }
    // Default to JSON if unknown
    return SerdeFormat::Json;
}

std::string typeToString(SerdeType type) {
    switch (type) {
        case SerdeType::Key: return "KEY";
        case SerdeType::Value: return "VALUE";
        default: return "UNKNOWN";
    }
}

std::string modeToString(Mode mode) {
    // This will use the existing mode to string conversion from the model
    // For now, return a placeholder
    return "MODE";
}

std::string phaseToString(Phase phase) {
    // This will use the existing phase to string conversion from the model
    // For now, return a placeholder
    return "PHASE";
}

std::string kindToString(Kind kind) {
    // This will use the existing kind to string conversion from the model
    // For now, return a placeholder  
    return "KIND";
}

} // namespace type_utils

// Value extraction utility functions (moved from header)
::avro::GenericDatum asAvro(const SerdeValue& value) {
    if (!value.isAvro()) {
        throw SerdeError("SerdeValue is not Avro");
    }
    return std::any_cast<::avro::GenericDatum>(value.getValue());
}

google::protobuf::Message& asProtobuf(const SerdeValue& value) {
    if (!value.isProtobuf()) {
        throw SerdeError("SerdeValue is not Protobuf");
    }
    return std::any_cast<std::reference_wrapper<google::protobuf::Message>>(value.getValue()).get();
}

} // namespace srclient::serdes 