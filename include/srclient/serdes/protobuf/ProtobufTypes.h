#pragma once

#include <memory>
#include <string>
#include <any>
#include <functional>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <nlohmann/json.hpp>
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"

namespace srclient::serdes::protobuf {

/**
 * Protobuf errors
 */
class ProtobufError : public SerdeError {
public:
    explicit ProtobufError(const std::string& message) : SerdeError("Protobuf error: " + message) {}
};

/**
 * Protobuf reflection errors
 * Maps to SerdeError::ProtobufReflect variant
 */
class ProtobufReflectError : public SerdeError {
public:
    explicit ProtobufReflectError(const std::string& message) : SerdeError("Protobuf reflect error: " + message) {}
};

/**
 * Protobuf implementation of SerdeValue
 */
class ProtobufValue : public SerdeValue {
private:
    google::protobuf::Message& value_;
    
public:
    explicit ProtobufValue(google::protobuf::Message& value) : value_(value) {}
    
    bool isJson() const override;
    bool isAvro() const override;
    bool isProtobuf() const override;
    
    std::any getValue() const override;
    
    SerdeFormat getFormat() const override;
    
    std::unique_ptr<SerdeValue> clone() const override;

    // Value extraction methods
    bool asBool() const override;
    std::string asString() const override;
    std::vector<uint8_t> asBytes() const override;

};

/**
 * Protobuf Schema implementation
 */
class ProtobufSchema : public SerdeSchema {
private:
    std::string schema_data_;
    
public:
    explicit ProtobufSchema(const std::string& schema_data) : schema_data_(schema_data) {}
    
    bool isAvro() const override { return false; }
    bool isJson() const override { return false; }
    bool isProtobuf() const override { return true; }
    
    SerdeFormat getFormat() const override { return SerdeFormat::Protobuf; }
    
    std::any getSchema() const override { return schema_data_; }
    
    std::unique_ptr<SerdeSchema> clone() const override {
        return std::make_unique<ProtobufSchema>(schema_data_);
    }
    
    // Direct access to Protobuf schema
    const std::string& getProtobufSchema() const { return schema_data_; }
};

// Helper functions for creating Protobuf SerdeValue instances
inline std::unique_ptr<SerdeValue> makeProtobufValue(google::protobuf::Message& value) {
    return std::make_unique<ProtobufValue>(value);
}

// Helper function for creating Protobuf SerdeSchema instances
inline std::unique_ptr<SerdeSchema> makeProtobufSchema(const std::string& schema_data) {
    return std::make_unique<ProtobufSchema>(schema_data);
}



} // namespace srclient::serdes::protobuf 