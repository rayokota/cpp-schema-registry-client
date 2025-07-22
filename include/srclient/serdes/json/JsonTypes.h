#pragma once

#include <memory>
#include <string>
#include <any>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <nlohmann/json.hpp>
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"

namespace srclient::serdes::json {

/**
 * JSON referencing errors
 * Maps to SerdeError::JsonReferencing variant
 */
class JsonReferencingError : public SerdeError {
public:
    explicit JsonReferencingError(const std::string& message) : SerdeError("JSON referencing error: " + message) {}
    explicit JsonReferencingError() : SerdeError("JSON referencing error") {}
};

/**
 * JSON serialization errors
 * Maps to SerdeError::Json variant
 */
class JsonError : public SerdeError {
public:
    explicit JsonError(const std::string& message) : SerdeError("JSON serde error: " + message) {}
};

/**
 * JSON validation errors
 * Maps to SerdeError::JsonValidation variant
 */
class JsonValidationError : public SerdeError {
public:
    explicit JsonValidationError(const std::string& message) : SerdeError("JSON validation error: " + message) {}
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
    
    // SerdeObject interface implementation
    const void* getRawObject() const override { return &value_; }
    void* getMutableRawObject() override { return &value_; }
    SerdeFormat getFormat() const override { return SerdeFormat::Json; }
    const std::type_info& getType() const override { return typeid(nlohmann::json); }
    
    std::unique_ptr<SerdeValue> clone() const override {
        return std::make_unique<JsonValue>(value_);
    }
    
    void moveFrom(SerdeObject&& other) override {
        if (other.getFormat() == SerdeFormat::Json) {
            value_ = std::move(*static_cast<nlohmann::json*>(other.getMutableRawObject()));
        }
    }

    // Value extraction methods
    bool asBool() const override;
    std::string asString() const override;
    std::vector<uint8_t> asBytes() const override;

};

/**
 * JSON Schema implementation
 */
template<typename Json>
class JsonSchema : public SerdeSchema {
private:
    jsoncons::jsonschema::json_schema<Json>& schema_;
    
public:
    explicit JsonSchema(const jsoncons::jsonschema::json_schema<Json>& schema) : schema_(schema) {}
    
    SerdeFormat getFormat() const override { return SerdeFormat::Json; }
    
    std::any getSchema() const override { return schema_; }
    
    std::unique_ptr<SerdeSchema> clone() const override {
        return std::make_unique<JsonSchema>(schema_);
    }
    
    // Direct access to JSON schema
    const jsoncons::jsonschema::json_schema<Json>& getJsonSchema() const { return schema_; }
};

// Helper functions for creating JSON SerdeValue instances
inline std::unique_ptr<SerdeValue> makeJsonValue(const nlohmann::json& value) {
    return std::make_unique<JsonValue>(value);
}

inline std::unique_ptr<SerdeValue> makeJsonValue(nlohmann::json&& value) {
    return std::make_unique<JsonValue>(std::move(value));
}

// Helper function for creating JSON SerdeSchema instances
template<typename Json>
inline std::unique_ptr<SerdeSchema> makeJsonSchema(const jsoncons::jsonschema::json_schema<Json>& schema) {
    return std::make_unique<JsonSchema>(schema);
}



} // namespace srclient::serdes::json 