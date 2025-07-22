/**
 * JsonTest
 * Tests for the JSON serialization functionality
 */

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <nlohmann/json.hpp>

// Project includes
#include "srclient/rest/MockSchemaRegistryClient.h"
#include "srclient/rest/ClientConfiguration.h"
#include "srclient/rest/SchemaRegistryClient.h"
#include "srclient/serdes/SerdeConfig.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/RuleRegistry.h"
#include "srclient/serdes/json/JsonSerializer.h"
#include "srclient/serdes/json/JsonDeserializer.h"
#include "srclient/rest/model/Schema.h"
#include "srclient/rest/model/Rule.h"
#include "srclient/rest/model/RuleSet.h"
#include "srclient/rules/cel/CelFieldExecutor.h"
#include "srclient/rules/encryption/FieldEncryptionExecutor.h"
#include "srclient/rules/encryption/EncryptionExecutor.h"
#include "srclient/rules/encryption/localkms/LocalKmsDriver.h"
#include "srclient/rest/MockDekRegistryClient.h"

// Additional includes needed for proper compilation
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/Serde.h"

using namespace srclient::serdes;
using namespace srclient::serdes::json;
using namespace srclient::rest;
using namespace srclient::rest::model;
using namespace srclient::rules::cel;
using namespace srclient::rules::encryption;
using namespace srclient::rules::encryption::localkms;

TEST(JsonTest, BasicSerialization) {
    // Create client configuration with mock URL
    std::vector<std::string> urls = {"mock://"};
    auto client_config = std::make_shared<const ClientConfiguration>(urls);
    auto client = SchemaRegistryClient::newClient(client_config);
    
    // Create serializer configuration
    auto ser_conf = SerializerConfig::createDefault();
    
    // Define JSON schema string
    std::string schema_str = R"(
    {
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "doubleField": {"type": "number"},
            "stringField": {
                "type": "string",
                "confluent:tags": ["PII"]
            },
            "booleanField": {"type": "boolean"},
            "bytesField": {
                "type": "string",
                "contentEncoding": "base64",
                "confluent:tags": ["PII"]
            }
        }
    }
    )";
    
    // Create schema object
    Schema schema;
    schema.setSchemaType("JSON");
    schema.setSchema(schema_str);
    
    // Create test JSON object
    std::string obj_str = R"(
    {
        "intField": 123,
        "doubleField": 45.67,
        "stringField": "hi",
        "booleanField": true,
        "bytesField": "Zm9vYmFy"
    }
    )";
    
    nlohmann::json obj = nlohmann::json::parse(obj_str);
    
    // Create rule registry
    auto rule_registry = std::make_shared<RuleRegistry>();
    
    // Create JsonSerializer
    JsonSerializer serializer(client, schema, rule_registry, ser_conf);
    
    // Create serialization context
    SerializationContext ser_ctx;
    ser_ctx.topic = "test";
    ser_ctx.serde_type = SerdeType::Value;
    ser_ctx.serde_format = SerdeFormat::Json;
    
    // Serialize the JSON object
    std::vector<uint8_t> bytes = serializer.serialize(ser_ctx, obj);
    
    // Create JsonDeserializer
    auto deser_conf = DeserializerConfig::createDefault();
    JsonDeserializer deserializer(client, rule_registry, deser_conf);
    
    // Deserialize back to JSON object
    nlohmann::json obj2 = deserializer.deserialize(ser_ctx, bytes);
    
    // Assert that the original and deserialized objects are equal
    ASSERT_EQ(obj2, obj);
}
