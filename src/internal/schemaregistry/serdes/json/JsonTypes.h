#pragma once

#include <any>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>

#include "schemaregistry/rest/ISchemaRegistryClient.h"
#include "schemaregistry/serdes/SerdeError.h"
#include "schemaregistry/serdes/SerdeTypes.h"

namespace schemaregistry::serdes::json {

using Resolver = std::function<jsoncons::ojson(const jsoncons::uri &)>;

/**
 * JSON schema caching and validation class
 * Based on JsonSerde struct from json.rs (converted to synchronous)
 */
class JsonSerde {
  public:
    JsonSerde();
    ~JsonSerde() = default;

    // Schema parsing and caching
    std::shared_ptr<jsoncons::jsonschema::json_schema<jsoncons::ojson>>
    getParsedSchema(
        const schemaregistry::rest::model::Schema &schema,
        std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client);

    // Clear caches
    void clear();

  private:
    // Cache for parsed schemas: Schema -> json_schema
    std::unordered_map<
        std::string,
        std::shared_ptr<jsoncons::jsonschema::json_schema<jsoncons::ojson>>>
        parsed_schemas_cache_;

    mutable std::mutex cache_mutex_;

    // Helper methods
    void resolveNamedSchema(
        const schemaregistry::rest::model::Schema &schema,
        std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client,
        std::unordered_map<std::string, std::string> references);
};

/**
 * JSON referencing errors
 * Maps to SerdeError::JsonReferencing variant
 */
class JsonReferencingError : public SerdeError {
  public:
    explicit JsonReferencingError(const std::string &message)
        : SerdeError("JSON referencing error: " + message) {}
    explicit JsonReferencingError() : SerdeError("JSON referencing error") {}
};

/**
 * JSON serialization errors
 * Maps to SerdeError::Json variant
 */
class JsonError : public SerdeError {
  public:
    explicit JsonError(const std::string &message)
        : SerdeError("JSON serde error: " + message) {}
};

/**
 * JSON validation errors
 * Maps to SerdeError::JsonValidation variant
 */
class JsonValidationError : public SerdeError {
  public:
    explicit JsonValidationError(const std::string &message)
        : SerdeError("JSON validation error: " + message) {}
};

/**
 * JSON implementation of SerdeValue
 */
class JsonValue : public SerdeValue {
  private:
    nlohmann::json value_;

  public:
    explicit JsonValue(const nlohmann::json &value) : value_(value) {}
    explicit JsonValue(nlohmann::json &&value) : value_(std::move(value)) {}

    // SerdeValue interface implementation
    const void *getRawValue() const override { return &value_; }
    void *getMutableRawValue() override { return &value_; }
    SerdeFormat getFormat() const override { return SerdeFormat::Json; }
    const std::type_info &getType() const override {
        return typeid(nlohmann::json);
    }

    std::unique_ptr<SerdeValue> clone() const override {
        return std::make_unique<JsonValue>(value_);
    }

    void moveFrom(SerdeValue &&other) override {
        if (other.getFormat() == SerdeFormat::Json) {
            value_ = std::move(
                *static_cast<nlohmann::json *>(other.getMutableRawValue()));
        }
    }

    // Value extraction methods
    bool asBool() const override;
    std::string asString() const override;
    std::vector<uint8_t> asBytes() const override;
};

// Helper functions for creating JSON SerdeValue instances
std::unique_ptr<SerdeValue> makeJsonValue(const nlohmann::json &value);

std::unique_ptr<SerdeValue> makeJsonValue(nlohmann::json &&value);

std::unique_ptr<SerdeValue> makeJsonValue(const jsoncons::ojson &value);

std::unique_ptr<SerdeValue> makeJsonValue(jsoncons::ojson &&value);

// Utility functions for JSON value and schema extraction
nlohmann::json asJson(const SerdeValue &value);

jsoncons::ojson asOJson(const SerdeValue &value);

}  // namespace schemaregistry::serdes::json
