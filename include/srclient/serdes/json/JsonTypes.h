#pragma once

#include <memory>
#include <string>
#include <any>
#include <nlohmann/json.hpp>
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeBase.h"

namespace srclient::serdes::json {

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
    
    std::any getValue() const override { return value_; }
    
    SerdeFormat getFormat() const override { return SerdeFormat::Json; }
    
    std::unique_ptr<SerdeValue> clone() const override {
        return std::make_unique<JsonValue>(value_);
    }

};

// Helper functions for creating JSON SerdeValue instances
inline std::unique_ptr<SerdeValue> makeJsonValue(const nlohmann::json& value) {
    return std::make_unique<JsonValue>(value);
}

inline std::unique_ptr<SerdeValue> makeJsonValue(nlohmann::json&& value) {
    return std::make_unique<JsonValue>(std::move(value));
}



} // namespace srclient::serdes::json 