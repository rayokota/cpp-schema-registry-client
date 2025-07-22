#pragma once

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>

#include "srclient/rest/ISchemaRegistryClient.h"
#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeConfig.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/json/JsonTypes.h"

namespace srclient::serdes::json {

// Forward declarations
class JsonSerializer;

class JsonSerde;

/**
 * JSON schema caching and validation class
 * Based on JsonSerde struct from json.rs (converted to synchronous)
 */
class JsonSerde {
  public:
    JsonSerde();
    ~JsonSerde() = default;

    // Schema parsing and caching
    std::pair<nlohmann::json, std::optional<std::string>> getParsedSchema(
        const srclient::rest::model::Schema &schema,
        std::shared_ptr<srclient::rest::ISchemaRegistryClient> client);

    // Validator caching
    bool validateJson(const nlohmann::json &value,
                      const nlohmann::json &schema);

    // Clear caches
    void clear();

  private:
    // Cache for parsed schemas: Schema -> (parsed_json, schema_string)
    std::unordered_map<std::string, std::pair<nlohmann::json, std::string>>
        parsed_schemas_cache_;

    mutable std::mutex cache_mutex_;

    // Helper methods
    void resolveNamedSchema(
        const srclient::rest::model::Schema &schema,
        std::shared_ptr<srclient::rest::ISchemaRegistryClient> client);
};

/**
 * JSON serializer class template
 * Based on JsonSerializer from json.rs (converted to synchronous)
 */
class JsonSerializer {
  public:
    /**
     * Constructor
     */
    JsonSerializer(
        std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
        std::optional<srclient::rest::model::Schema> schema,
        std::shared_ptr<RuleRegistry> rule_registry,
        const SerializerConfig &config);

    /**
     * Serialize a JSON value to bytes with schema validation
     * @param ctx Serialization context (topic, serde type, etc.)
     * @param value JSON value to serialize
     * @return Serialized bytes with schema ID header
     */
    std::vector<uint8_t> serialize(const SerializationContext &ctx,
                                   const nlohmann::json &value);

    /**
     * Close the serializer and cleanup resources
     */
    void close();

  private:
    std::optional<srclient::rest::model::Schema> schema_;
    std::shared_ptr<BaseSerializer> base_;
    std::unique_ptr<JsonSerde> serde_;

    // Helper methods
    std::pair<nlohmann::json, std::optional<std::string>> getParsedSchema(
        const srclient::rest::model::Schema &schema);

    bool validateJson(const nlohmann::json &value,
                      const nlohmann::json &schema);

    void validateSchema(const srclient::rest::model::Schema &schema);

    std::unique_ptr<SerdeValue> transformValue(const SerdeValue &value,
                                               const Schema &schema,
                                               const std::string &subject);

    nlohmann::json executeFieldTransformations(
        const nlohmann::json &value, const nlohmann::json &schema,
        const RuleContext &context, const std::string &field_executor_type);
};

}  // namespace srclient::serdes::json