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
    std::unique_ptr<google::protobuf::Message> value_;
    
public:
    explicit ProtobufValue(std::unique_ptr<google::protobuf::Message> value) : value_(std::move(value)) {}
    
    // SerdeValue interface implementation
    const void* getRawValue() const override { return value_.get(); }
    void* getMutableRawValue() override { return value_.get(); }
    SerdeFormat getFormat() const override { return SerdeFormat::Protobuf; }
    const std::type_info& getType() const override { return typeid(google::protobuf::Message); }
    
    std::unique_ptr<SerdeValue> clone() const override;
    
    void moveFrom(SerdeValue&& other) override {
        if (other.getFormat() == SerdeFormat::Protobuf) {
            auto* other_msg = static_cast<google::protobuf::Message*>(other.getMutableRawValue());
            value_->CopyFrom(*other_msg);
        }
    }

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
    const google::protobuf::FileDescriptor* schema_;

public:
    explicit ProtobufSchema(const google::protobuf::FileDescriptor* schema) : schema_(schema) {}
    
    SerdeFormat getFormat() const override { return SerdeFormat::Protobuf; }
    
    std::any getSchema() const override { return schema_; }
    
    std::unique_ptr<SerdeSchema> clone() const override {
        return std::make_unique<ProtobufSchema>(schema_);
    }
    
    // Direct access to Protobuf schema
    const google::protobuf::FileDescriptor* getProtobufSchema() const { return schema_; }
};

// Helper functions for creating Protobuf SerdeValue instances
inline std::unique_ptr<SerdeValue> makeProtobufValue(std::unique_ptr<google::protobuf::Message> value) {
    auto protobuf_value = std::make_unique<ProtobufValue>(std::move(value));
    return std::unique_ptr<SerdeValue>(static_cast<SerdeValue*>(protobuf_value.release()));
}

// Overload for backward compatibility - creates a copy
inline std::unique_ptr<SerdeValue> makeProtobufValue(google::protobuf::Message& value) {
    std::unique_ptr<google::protobuf::Message> owned_value(value.New());
    owned_value->CopyFrom(value);
    auto protobuf_value = std::make_unique<ProtobufValue>(std::move(owned_value));
    return std::unique_ptr<SerdeValue>(static_cast<SerdeValue*>(protobuf_value.release()));
}

// Helper function for creating Protobuf SerdeSchema instances
inline std::unique_ptr<SerdeSchema> makeProtobufSchema(const google::protobuf::FileDescriptor* file_descriptor) {
    return std::make_unique<ProtobufSchema>(file_descriptor);
}



} // namespace srclient::serdes::protobuf 