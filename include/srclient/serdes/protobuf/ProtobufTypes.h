#pragma once

#include <memory>
#include <string>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <nlohmann/json.hpp>
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeBase.h"

namespace srclient::serdes::protobuf {

/**
 * Protobuf-specific error class
 */
class ProtobufSerdeError : public SerdeError {
public:
    explicit ProtobufSerdeError(const std::string& message) 
        : SerdeError("Protobuf: " + message) {}
};

/**
 * Protobuf implementation of SerdeValue
 */
class ProtobufValue : public SerdeValue {
private:
    google::protobuf::Message& value_;
    
public:
    explicit ProtobufValue(google::protobuf::Message& value) : value_(value) {}
    
    bool isJson() const override { return false; }
    bool isAvro() const override { return false; }
    bool isProtobuf() const override { return true; }
    
    nlohmann::json asJson() const override { 
        throw SerdeError("SerdeValue is not JSON"); 
    }
    ::avro::GenericDatum asAvro() const override { 
        throw SerdeError("SerdeValue is not Avro"); 
    }
    google::protobuf::Message& asProtobuf() const override { 
        return value_;
    }
    
    SerdeFormat getFormat() const override { return SerdeFormat::Protobuf; }
    
    std::unique_ptr<SerdeValue> clone() const override {
        // Note: Protobuf messages can't be easily cloned without knowing the concrete type
        // This may need to be handled differently in actual usage
        return std::make_unique<ProtobufValue>(value_);
    }
    
    // Direct access to the Protobuf value
    google::protobuf::Message& getValue() const { return value_; }
};

// Helper functions for creating Protobuf SerdeValue instances
inline std::unique_ptr<SerdeValue> makeProtobufValue(google::protobuf::Message& value) {
    return std::make_unique<ProtobufValue>(value);
}



} // namespace srclient::serdes::protobuf 