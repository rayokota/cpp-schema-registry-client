#include "schemaregistry/serdes/SerdeTypes.h"

#include <sstream>

#include "absl/strings/escaping.h"
#include "schemaregistry/rest/model/RegisteredSchema.h"
#include "schemaregistry/rest/model/Schema.h"
// Note: Format-specific types are no longer directly included here
// They register themselves through the factory system

namespace schemaregistry::serdes {

// SchemaSelector static factory methods
SchemaSelector SchemaSelector::useSchemaId(int32_t id) {
    SchemaSelector data;
    data.type = SchemaSelectorType::SchemaId;
    data.schema_id = id;
    return data;
}

SchemaSelector SchemaSelector::useLatestVersion() {
    SchemaSelector data;
    data.type = SchemaSelectorType::LatestVersion;
    return data;
}

SchemaSelector SchemaSelector::useLatestWithMetadata(
    const std::unordered_map<std::string, std::string> &metadata) {
    SchemaSelector data;
    data.type = SchemaSelectorType::LatestWithMetadata;
    data.metadata = metadata;
    return data;
}

// Migration implementation
Migration::Migration(Mode mode, std::optional<RegisteredSchema> src,
                     std::optional<RegisteredSchema> tgt)
    : rule_mode(mode), source(src), target(tgt) {}

// FieldType to string conversion
std::string fieldTypeToString(FieldType type) {
    switch (type) {
        case FieldType::Record:
            return "RECORD";
        case FieldType::Enum:
            return "ENUM";
        case FieldType::Array:
            return "ARRAY";
        case FieldType::Map:
            return "MAP";
        case FieldType::Combined:
            return "COMBINED";
        case FieldType::Fixed:
            return "FIXED";
        case FieldType::String:
            return "STRING";
        case FieldType::Bytes:
            return "BYTES";
        case FieldType::Int:
            return "INT";
        case FieldType::Long:
            return "LONG";
        case FieldType::Float:
            return "FLOAT";
        case FieldType::Double:
            return "DOUBLE";
        case FieldType::Boolean:
            return "BOOLEAN";
        case FieldType::Null:
            return "NULL";
        default:
            return "UNKNOWN";
    }
}

// ParsedSchemaCache template implementation
template <typename T>
void ParsedSchemaCache<T>::set(const Schema &schema, const T &parsed_schema) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string key = getSchemaKey(schema);
    cache_[key] = parsed_schema;
}

template <typename T>
std::optional<T> ParsedSchemaCache<T>::get(const Schema &schema) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string key = getSchemaKey(schema);
    auto it = cache_.find(key);
    if (it != cache_.end()) {
        return it->second;
    }
    return std::nullopt;
}

template <typename T>
void ParsedSchemaCache<T>::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.clear();
}

template <typename T>
std::string ParsedSchemaCache<T>::getSchemaKey(const Schema &schema) const {
    // Us the JSON representation of the schema to create a hash
    nlohmann::json j;
    to_json(j, schema);
    return j.dump();
}

// Explicit template instantiations for common types
template class ParsedSchemaCache<std::string>;
template class ParsedSchemaCache<int>;

// Note: Base64 utilities moved to format-specific implementations

// SerdeValueFactory static member definitions
std::unordered_map<SerdeFormat, SerdeValueStringFactory>
    SerdeValueFactory::string_factories_;
std::unordered_map<SerdeFormat, SerdeValueBytesFactory>
    SerdeValueFactory::bytes_factories_;
std::unordered_map<SerdeFormat, SerdeValueJsonFactory>
    SerdeValueFactory::json_factories_;
std::mutex SerdeValueFactory::registry_mutex_;

// SerdeValueFactory implementation
void SerdeValueFactory::registerStringFactory(SerdeFormat format,
                                              SerdeValueStringFactory factory) {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    string_factories_[format] = std::move(factory);
}

void SerdeValueFactory::registerBytesFactory(SerdeFormat format,
                                             SerdeValueBytesFactory factory) {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    bytes_factories_[format] = std::move(factory);
}

void SerdeValueFactory::registerJsonFactory(SerdeFormat format,
                                            SerdeValueJsonFactory factory) {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    json_factories_[format] = std::move(factory);
}

std::unique_ptr<SerdeValue> SerdeValueFactory::createString(
    SerdeFormat format, const std::string &value) {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    auto it = string_factories_.find(format);
    if (it != string_factories_.end()) {
        return it->second(value);
    }
    throw SerdeError("No string factory registered for format: " +
                     type_utils::formatToString(format));
}

std::unique_ptr<SerdeValue> SerdeValueFactory::createBytes(
    SerdeFormat format, const std::vector<uint8_t> &value) {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    auto it = bytes_factories_.find(format);
    if (it != bytes_factories_.end()) {
        return it->second(value);
    }
    throw SerdeError("No bytes factory registered for format: " +
                     type_utils::formatToString(format));
}

std::unique_ptr<SerdeValue> SerdeValueFactory::createJson(
    SerdeFormat format, const nlohmann::json &value) {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    auto it = json_factories_.find(format);
    if (it != json_factories_.end()) {
        return it->second(value);
    }
    throw SerdeError("No JSON factory registered for format: " +
                     type_utils::formatToString(format));
}

bool SerdeValueFactory::hasStringFactory(SerdeFormat format) {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    return string_factories_.find(format) != string_factories_.end();
}

bool SerdeValueFactory::hasBytesFactory(SerdeFormat format) {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    return bytes_factories_.find(format) != bytes_factories_.end();
}

bool SerdeValueFactory::hasJsonFactory(SerdeFormat format) {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    return json_factories_.find(format) != json_factories_.end();
}

// SerdeValue static factory method implementations
std::unique_ptr<SerdeValue> SerdeValue::newString(SerdeFormat format,
                                                  const std::string &value) {
    return SerdeValueFactory::createString(format, value);
}

std::unique_ptr<SerdeValue> SerdeValue::newBytes(
    SerdeFormat format, const std::vector<uint8_t> &value) {
    return SerdeValueFactory::createBytes(format, value);
}

std::unique_ptr<SerdeValue> SerdeValue::newJson(SerdeFormat format,
                                                const nlohmann::json &value) {
    return SerdeValueFactory::createJson(format, value);
}

namespace type_utils {

std::string formatToString(SerdeFormat format) {
    switch (format) {
        case SerdeFormat::Avro:
            return "AVRO";
        case SerdeFormat::Json:
            return "JSON";
        case SerdeFormat::Protobuf:
            return "PROTOBUF";
        default:
            return "UNKNOWN";
    }
}

SerdeFormat stringToFormat(const std::string &format) {
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
        case SerdeType::Key:
            return "KEY";
        case SerdeType::Value:
            return "VALUE";
        default:
            return "UNKNOWN";
    }
}

std::string modeToString(Mode mode) {
    switch (mode) {
        case Mode::Read:
            return "READ";
        case Mode::Write:
            return "WRITE";
        case Mode::WriteRead:
            return "WRITEREAD";
        case Mode::Upgrade:
            return "UPGRADE";
        case Mode::Downgrade:
            return "DOWNGRADE";
        case Mode::UpDown:
            return "UPDOWN";
        default:
            return "UNKNOWN";
    }
}

std::string phaseToString(Phase phase) {
    switch (phase) {
        case Phase::Migration:
            return "MIGRATION";
        case Phase::Domain:
            return "DOMAIN";
        case Phase::Encoding:
            return "ENCODING";
        default:
            return "UNKNOWN";
    }
}

std::string kindToString(Kind kind) {
    switch (kind) {
        case Kind::Transform:
            return "TRANSFORM";
        case Kind::Condition:
            return "CONDITION";
        default:
            return "UNKNOWN";
    }
}

}  // namespace type_utils

}  // namespace schemaregistry::serdes