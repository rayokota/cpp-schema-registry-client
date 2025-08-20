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
#include "schemaregistry/rest/MockSchemaRegistryClient.h"
#include "schemaregistry/rest/ClientConfiguration.h"
#include "schemaregistry/rest/SchemaRegistryClient.h"
#include "schemaregistry/serdes/SerdeConfig.h"
#include "schemaregistry/serdes/SerdeTypes.h"
#include "schemaregistry/serdes/RuleRegistry.h"
#include "schemaregistry/serdes/json/JsonSerializer.h"
#include "schemaregistry/serdes/json/JsonDeserializer.h"
#include "schemaregistry/rest/model/Schema.h"
#include "schemaregistry/rest/model/Rule.h"
#include "schemaregistry/rest/model/RuleSet.h"
#include "schemaregistry/rules/cel/CelFieldExecutor.h"
#include "schemaregistry/rules/encryption/FieldEncryptionExecutor.h"
#include "schemaregistry/rules/encryption/EncryptionExecutor.h"
#include "schemaregistry/rules/encryption/localkms/LocalKmsDriver.h"
#include "schemaregistry/rest/MockDekRegistryClient.h"

// Additional includes needed for proper compilation
#include "schemaregistry/serdes/SerdeError.h"
#include "schemaregistry/serdes/Serde.h"

using namespace schemaregistry::serdes;
using namespace schemaregistry::serdes::json;
using namespace schemaregistry::rest;
using namespace schemaregistry::rest::model;
using namespace schemaregistry::rules::cel;
using namespace schemaregistry::rules::encryption;
using namespace schemaregistry::rules::encryption::localkms;

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

TEST(JsonTest, SerializeReferences) {
    // Create client configuration with mock URL
    std::vector<std::string> urls = {"mock://"};
    auto client_config = std::make_shared<const ClientConfiguration>(urls);
    auto client = SchemaRegistryClient::newClient(client_config);
    
    // Create serializer configuration
    auto ser_conf = SerializerConfig(
        false,  // auto_register_schemas
        std::make_optional(SchemaSelectorData::useLatestVersion()),  // use_schema
        true,   // normalize_schemas  
        true,  // validate
        {}      // rule_config
    );
    
    // Define reference schema string
    std::string ref_schema_str = R"(
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
    
    // Create reference schema object
    Schema ref_schema;
    ref_schema.setSchemaType(std::make_optional<std::string>("JSON"));
    ref_schema.setSchema(std::make_optional<std::string>(ref_schema_str));
    
    // Register the reference schema
    auto registered_ref_schema = client->registerSchema("ref", ref_schema, false);
    
    // Define main schema string that references the "ref" schema
    std::string schema_str = R"(
    {
        "type": "object",
        "properties": {
            "otherField": {"$ref": "ref"}
        }
    }
    )";
    
    // Create schema reference
    SchemaReference schema_ref;
    schema_ref.setName(std::make_optional<std::string>("ref"));
    schema_ref.setSubject(std::make_optional<std::string>("ref"));
    schema_ref.setVersion(std::make_optional<int32_t>(1));
    
    // Create vector of references
    std::vector<SchemaReference> refs = {schema_ref};
    
    // Create main schema object with references
    Schema schema;
    schema.setSchemaType(std::make_optional<std::string>("JSON"));
    schema.setReferences(std::make_optional<std::vector<SchemaReference>>(refs));
    schema.setSchema(std::make_optional<std::string>(schema_str));
    
    // Register the main schema
    auto registered_schema = client->registerSchema("test-value", schema, false);
    
    // Create test JSON object
    std::string obj_str = R"(
    {
        "otherField": {
            "intField": 123,
            "doubleField": 45.67,
            "stringField": "hi",
            "booleanField": true,
            "bytesField": "Zm9vYmFy"
        }
    }
    )";
    
    nlohmann::json obj = nlohmann::json::parse(obj_str);
    
    // Create rule registry
    auto rule_registry = std::make_shared<RuleRegistry>();
    
    // Create JsonSerializer with no specific schema (uses latest from registry)
    JsonSerializer serializer(client, std::nullopt, rule_registry, ser_conf);
    
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

TEST(JsonTest, CelField) {
    // Create client configuration with mock URL
    std::vector<std::string> urls = {"mock://"};
    auto client_config = std::make_shared<const ClientConfiguration>(urls);
    auto client = SchemaRegistryClient::newClient(client_config);
    
    // Create serializer configuration to use latest schema from registry
    auto ser_conf = SerializerConfig(
        false,  // auto_register_schemas
        std::make_optional(SchemaSelectorData::useLatestVersion()),  // use_schema
        false,  // normalize_schemas
        true,  // validate
        {}  // rule_config
    );
    
    // Define JSON schema string with PII tags
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
    
    // Create CEL rule
    Rule rule;
    rule.setName(std::make_optional<std::string>("test-cel"));
    rule.setKind(std::make_optional<Kind>(Kind::Transform));
    rule.setMode(std::make_optional<Mode>(Mode::Write));
    rule.setType(std::make_optional<std::string>("CEL_FIELD"));
    rule.setExpr(std::make_optional<std::string>("name == 'stringField' ; value + '-suffix'"));
    
    // Create rule set with domain rules
    RuleSet rule_set;
    std::vector<Rule> domain_rules = {rule};
    rule_set.setDomainRules(std::make_optional<std::vector<Rule>>(domain_rules));
    
    // Create schema object with rule set
    Schema schema;
    schema.setSchemaType(std::make_optional<std::string>("JSON"));
    schema.setSchema(std::make_optional<std::string>(schema_str));
    schema.setRuleSet(std::make_optional<RuleSet>(rule_set));
    
    // Register the schema
    auto registered_schema = client->registerSchema("test-value", schema, false);
    
    // Create original test JSON object
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
    
    // Create rule registry and register CEL field executor
    auto rule_registry = std::make_shared<RuleRegistry>();
    auto cel_field_executor = std::make_shared<CelFieldExecutor>();
    rule_registry->registerExecutor(cel_field_executor);
    
    // Create JsonSerializer with rule registry - no schema needed since we use latest from registry
    JsonSerializer serializer(client, std::nullopt, rule_registry, ser_conf);
    
    // Create serialization context
    SerializationContext ser_ctx;
    ser_ctx.topic = "test";
    ser_ctx.serde_type = SerdeType::Value;
    ser_ctx.serde_format = SerdeFormat::Json;
    
    // Serialize the JSON object
    std::vector<uint8_t> bytes = serializer.serialize(ser_ctx, obj);
    
    // Create JsonDeserializer with rule registry
    auto deser_conf = DeserializerConfig::createDefault();
    JsonDeserializer deserializer(client, rule_registry, deser_conf);
    
    // Create expected JSON object (with "-suffix" added to stringField)
    std::string expected_obj_str = R"(
    {
        "intField": 123,
        "doubleField": 45.67,
        "stringField": "hi-suffix",
        "booleanField": true,
        "bytesField": "Zm9vYmFy"
    }
    )";
    
    nlohmann::json expected_obj = nlohmann::json::parse(expected_obj_str);
    
    // Deserialize back to JSON object
    nlohmann::json obj2 = deserializer.deserialize(ser_ctx, bytes);
    
    // Assert that the deserialized object matches the expected object (with suffix)
    ASSERT_EQ(obj2, expected_obj);
}


TEST(JsonTest, Encryption) {
    // Register LocalKmsDriver
    LocalKmsDriver::registerDriver();
    
    // Create client configuration with mock URL
    std::vector<std::string> urls = {"mock://"};
    auto client_config = std::make_shared<const ClientConfiguration>(urls);
    auto client = SchemaRegistryClient::newClient(client_config);
    
    // Create rule configuration with secret
    std::unordered_map<std::string, std::string> rule_config;
    rule_config["secret"] = "mysecret";
    
    // Create serializer configuration to use latest schema from registry with rule config
    auto ser_conf = SerializerConfig(
        false,  // auto_register_schemas
        std::make_optional(SchemaSelectorData::useLatestVersion()),  // use_schema
        false,  // normalize_schemas
        false,  // validate
        rule_config  // rule_config
    );
    
    // Define JSON schema string with PII tags
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
    
    // Create encryption rule
    Rule rule;
    rule.setName(std::make_optional<std::string>("test-encrypt"));
    rule.setKind(std::make_optional<Kind>(Kind::Transform));
    rule.setMode(std::make_optional<Mode>(Mode::WriteRead));
    rule.setType(std::make_optional<std::string>("ENCRYPT"));
    
    // Set tags for PII fields
    std::vector<std::string> tags = {"PII"};
    rule.setTags(std::make_optional<std::vector<std::string>>(tags));
    
    // Set encryption parameters - use std::map instead of std::unordered_map
    std::map<std::string, std::string> params;
    params["encrypt.kek.name"] = "kek1";
    params["encrypt.kms.type"] = "local-kms";
    params["encrypt.kms.key.id"] = "mykey";
    rule.setParams(std::make_optional<std::map<std::string, std::string>>(params));
    
    // Set on_failure
    rule.setOnFailure(std::make_optional<std::string>("ERROR,NONE"));
    
    // Create rule set with domain rules
    RuleSet rule_set;
    std::vector<Rule> domain_rules = {rule};
    rule_set.setDomainRules(std::make_optional<std::vector<Rule>>(domain_rules));
    
    // Create schema object with rule set
    Schema schema;
    schema.setSchemaType(std::make_optional<std::string>("JSON"));
    schema.setSchema(std::make_optional<std::string>(schema_str));
    schema.setRuleSet(std::make_optional<RuleSet>(rule_set));
    
    // Register the schema
    auto registered_schema = client->registerSchema("test-value", schema, false);
    
    // Create original test JSON object
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
    
    // Create rule registry and register field encryption executor
    auto rule_registry = std::make_shared<RuleRegistry>();
    auto dek_client = std::make_shared<MockDekRegistryClient>(client_config);
    auto field_encryption_executor = std::make_shared<FieldEncryptionExecutor>();
    rule_registry->registerExecutor(field_encryption_executor);
    
    // Create JsonSerializer with rule registry - no schema needed since we use latest from registry
    JsonSerializer serializer(client, std::nullopt, rule_registry, ser_conf);
    
    // Create serialization context
    SerializationContext ser_ctx;
    ser_ctx.topic = "test";
    ser_ctx.serde_type = SerdeType::Value;
    ser_ctx.serde_format = SerdeFormat::Json;
    
    // Serialize the JSON object
    std::vector<uint8_t> bytes = serializer.serialize(ser_ctx, obj);
    
    // Create JsonDeserializer with rule registry
    auto deser_conf = DeserializerConfig::createDefault();
    JsonDeserializer deserializer(client, rule_registry, deser_conf);
    
    // Deserialize back to JSON object
    nlohmann::json obj2 = deserializer.deserialize(ser_ctx, bytes);
    
    // Assert that the original and deserialized objects are equal
    ASSERT_EQ(obj2, obj);
}