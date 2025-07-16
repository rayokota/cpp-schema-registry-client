#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>

#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeConfig.h"
#include "srclient/serdes/JsonSerializer.h" // For JsonSerde
#include "srclient/rest/ISchemaRegistryClient.h"

namespace srclient::serdes {

// Forward declarations
class JsonDeserializer;
struct SerializationContext;

/**
 * JSON deserializer class template
 * Based on JsonDeserializer from json.rs (converted to synchronous)
 */
class JsonDeserializer {
public:
    /**
     * Constructor
     */
    JsonDeserializer(std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
                    std::shared_ptr<RuleRegistry> rule_registry,
                    const DeserializerConfig& config);

    /**
     * Deserialize bytes to JSON object with schema validation and migration
     * @param ctx Serialization context (topic, serde type, etc.)
     * @param data Serialized bytes with schema ID header
     * @return Deserialized JSON object
     */
    nlohmann::json deserialize(const SerializationContext& ctx,
                              const std::vector<uint8_t>& data);

    /**
     * Close the deserializer and cleanup resources
     */
    void close();

private:
    std::shared_ptr<BaseDeserializer> base_;
    std::unique_ptr<JsonSerde> serde_;

    // Helper methods
    std::pair<nlohmann::json, std::optional<std::string>>
    getParsedSchema(const srclient::rest::model::Schema& schema);
    
    bool validateJson(const nlohmann::json& value,
                     const nlohmann::json& schema);
    
    nlohmann::json executeFieldTransformations(const nlohmann::json& value,
                                               const nlohmann::json& schema,
                                               const RuleContext& context,
                                               const std::string& field_executor_type);
    
    nlohmann::json executeMigrations(const SerializationContext& ctx,
                                    const std::string& subject,
                                    const std::vector<Migration>& migrations,
                                    const nlohmann::json& value);
    
    bool isEvolutionRequired(const srclient::rest::model::Schema& writer_schema,
                           const srclient::rest::model::Schema& reader_schema);
};

} // namespace srclient::serdes