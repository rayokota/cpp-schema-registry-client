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

std::unique_ptr<SerdeValue> ProtobufValue::clone() const {
    // Create a new message of the same type and copy the content
    std::unique_ptr<google::protobuf::Message> cloned_message(value_->New());
    cloned_message->CopyFrom(*value_);
    return std::make_unique<ProtobufValue>(std::move(cloned_message));
}

} // namespace srclient::serdes::protobuf 