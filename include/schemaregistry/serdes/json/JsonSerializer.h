#pragma once

#include <functional>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>

#include "schemaregistry/rest/ISchemaRegistryClient.h"
#include "schemaregistry/serdes/Serde.h"
#include "schemaregistry/serdes/SerdeConfig.h"
#include "schemaregistry/serdes/SerdeError.h"
#include "schemaregistry/serdes/SerdeTypes.h"
#include "schemaregistry/serdes/json/JsonTypes.h"

namespace schemaregistry::serdes::json {

// Forward declarations
class JsonSerializer;

class JsonSerde;

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
 * JSON serializer class template
 * Based on JsonSerializer from json.rs (converted to synchronous)
 */
class JsonSerializer {
  public:
    /**
     * Constructor
     */
    JsonSerializer(
        std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> client,
        std::optional<schemaregistry::rest::model::Schema> schema,
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
    std::optional<schemaregistry::rest::model::Schema> schema_;
    std::shared_ptr<BaseSerializer> base_;
    std::unique_ptr<JsonSerde> serde_;

    // Helper methods
    std::shared_ptr<jsoncons::jsonschema::json_schema<jsoncons::ojson>>
    getParsedSchema(const schemaregistry::rest::model::Schema &schema);

    void validateSchema(const schemaregistry::rest::model::Schema &schema);

    std::unique_ptr<SerdeValue> transformValue(const SerdeValue &value,
                                               const Schema &schema,
                                               const std::string &subject);

    nlohmann::json executeFieldTransformations(
        const nlohmann::json &value, const nlohmann::json &schema,
        const RuleContext &context, const std::string &field_executor_type);
};

}  // namespace schemaregistry::serdes::json