#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/reflection.h>

namespace srclient::serdes::protobuf {

// Implementation for ProtobufValue methods
bool ProtobufValue::asBool() const {
    // For protobuf, we need to examine the message's fields to extract boolean values
    // Since we don't have context about which field to read, default to true
    // This matches the Rust behavior for non-boolean types
    return true;
}

std::string ProtobufValue::asString() const {
    // For protobuf, we need to examine the message's fields to extract string values
    // Since we don't have context about which field to read, return empty string
    // This matches the Rust behavior for non-string types
    return "";
}

std::vector<uint8_t> ProtobufValue::asBytes() const {
    // For protobuf, we need to examine the message's fields to extract bytes values
    // Since we don't have context about which field to read, return empty vector
    // This matches the Rust behavior for non-bytes types
    return std::vector<uint8_t>();
}

// Additional ProtobufValue method implementations (moved from header)
bool ProtobufValue::isJson() const {
    return false;
}

bool ProtobufValue::isAvro() const {
    return false;
}

bool ProtobufValue::isProtobuf() const {
    return true;
}

std::any ProtobufValue::getValue() const {
    return std::ref(value_);
}

SerdeFormat ProtobufValue::getFormat() const {
    return SerdeFormat::Protobuf;
}

std::unique_ptr<SerdeValue> ProtobufValue::clone() const {
    // Note: Protobuf messages can't be easily cloned without knowing the concrete type
    // This may need to be handled differently in actual usage
    return std::make_unique<ProtobufValue>(value_);
}

} // namespace srclient::serdes::protobuf 