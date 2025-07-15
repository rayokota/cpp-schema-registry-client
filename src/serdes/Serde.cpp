#include "srclient/serdes/Serde.h"

namespace srclient::serdes {

// SerdeValue constructors
SerdeValue::SerdeValue(Type type, const std::string& value) 
    : type_(type), value_(value) {}

SerdeValue::SerdeValue(Type type, const std::vector<uint8_t>& value) 
    : type_(type), value_(value) {}

SerdeValue::SerdeValue(Type type, bool value) 
    : type_(type), value_(value) {}

SerdeValue::SerdeValue(Type type, int64_t value) 
    : type_(type), value_(value) {}

SerdeValue::SerdeValue(Type type, double value) 
    : type_(type), value_(value) {}

SerdeValue::SerdeValue(const nlohmann::json& json_value) 
    : type_(Type::Json), value_(json_value) {}

SerdeValue::SerdeValue(Type type, const avro::GenericDatum& avro_value) 
    : type_(type), value_(avro_value) {}

// Static factory methods
SerdeValue SerdeValue::createString(SerdeFormat format, const std::string& value) {
    Type type = (format == SerdeFormat::Json) ? Type::Json : Type::Avro;
    return SerdeValue(type, value);
}

SerdeValue SerdeValue::createBytes(SerdeFormat format, const std::vector<uint8_t>& value) {
    Type type = (format == SerdeFormat::Json) ? Type::Json : Type::Avro;
    return SerdeValue(type, value);
}

SerdeValue SerdeValue::createBool(SerdeFormat format, bool value) {
    Type type = (format == SerdeFormat::Json) ? Type::Json : Type::Avro;
    return SerdeValue(type, value);
}

SerdeValue SerdeValue::createInt(SerdeFormat format, int64_t value) {
    Type type = (format == SerdeFormat::Json) ? Type::Json : Type::Avro;
    return SerdeValue(type, value);
}

SerdeValue SerdeValue::createFloat(SerdeFormat format, double value) {
    Type type = (format == SerdeFormat::Json) ? Type::Json : Type::Avro;
    return SerdeValue(type, value);
}

// Accessor methods
bool SerdeValue::asBool() const {
    if (auto val = std::get_if<bool>(&value_)) {
        return *val;
    }
    throw SerdeError("SerdeValue is not a boolean");
}

std::string SerdeValue::asString() const {
    if (auto val = std::get_if<std::string>(&value_)) {
        return *val;
    }
    throw SerdeError("SerdeValue is not a string");
}

std::vector<uint8_t> SerdeValue::asBytes() const {
    if (auto val = std::get_if<std::vector<uint8_t>>(&value_)) {
        return *val;
    }
    throw SerdeError("SerdeValue is not bytes");
}

int64_t SerdeValue::asInt() const {
    if (auto val = std::get_if<int64_t>(&value_)) {
        return *val;
    }
    throw SerdeError("SerdeValue is not an integer");
}

double SerdeValue::asFloat() const {
    if (auto val = std::get_if<double>(&value_)) {
        return *val;
    }
    throw SerdeError("SerdeValue is not a float");
}

nlohmann::json SerdeValue::asJson() const {
    if (auto val = std::get_if<nlohmann::json>(&value_)) {
        return *val;
    }
    throw SerdeError("SerdeValue is not JSON");
}

SerdeSchema::SerdeSchema(Type type, const std::string& schema_data) 
    : type_(type), schema_data_(schema_data) {}

SerdeSchema::SerdeSchema(Type type, const std::pair<avro::ValidSchema, std::vector<avro::ValidSchema>>& avro_schema) 
    : type_(type), avro_schema_(avro_schema) {}

} // namespace srclient::serdes 