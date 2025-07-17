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
    
    std::any getValue() const override { return std::ref(value_); }
    
    SerdeFormat getFormat() const override { return SerdeFormat::Protobuf; }
    
    std::unique_ptr<SerdeValue> clone() const override {
        // Note: Protobuf messages can't be easily cloned without knowing the concrete type
        // This may need to be handled differently in actual usage
        return std::make_unique<ProtobufValue>(value_);
    }

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
    
    SerdeSchemaFormat getFormat() const override { return SerdeSchemaFormat::Protobuf; }
    
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