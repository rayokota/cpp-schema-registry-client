#pragma once

#include <memory>
#include <string>
#include <nlohmann/json.hpp>
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeBase.h"

namespace srclient::serdes {

/**
 * JSON-specific serialization error
 */
class JsonSerdeError : public SerdeError {
public:
    explicit JsonSerdeError(const std::string& message) : SerdeError("JSON serde error: " + message) {}
};

/**
 * JSON implementation of SerdeValue
 */
class JsonValue : public SerdeValue {
private:
    nlohmann::json value_;
    
public:
    explicit JsonValue(const nlohmann::json& value) : value_(value) {}
    explicit JsonValue(nlohmann::json&& value) : value_(std::move(value)) {}
    
    bool isJson() const override { return true; }
    bool isAvro() const override { return false; }
    bool isProtobuf() const override { return false; }
    
    nlohmann::json asJson() const override { return value_; }
    avro::GenericDatum asAvro() const override { 
        throw SerdeError("SerdeValue is not Avro"); 
    }
    google::protobuf::Message& asProtobuf() const override { 
        throw SerdeError("SerdeValue is not Protobuf"); 
    }
    
    SerdeFormat getFormat() const override { return SerdeFormat::Json; }
    
    std::unique_ptr<SerdeValue> clone() const override {
        return std::make_unique<JsonValue>(value_);
    }
    
    // Direct access to the JSON value
    const nlohmann::json& getValue() const { return value_; }
    nlohmann::json& getValue() { return value_; }
};

// Helper functions for creating JSON SerdeValue instances
inline std::unique_ptr<SerdeValue> makeJsonValue(const nlohmann::json& value) {
    return std::make_unique<JsonValue>(value);
}

inline std::unique_ptr<SerdeValue> makeJsonValue(nlohmann::json&& value) {
    return std::make_unique<JsonValue>(std::move(value));
}



} // namespace srclient::serdes 