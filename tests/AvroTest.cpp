/**
 * AvroTest
 * Tests for the Avro serialization functionality
 */

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <avro/Compiler.hh>
#include <avro/ValidSchema.hh>
#include <avro/Generic.hh>

// Project includes
#include "srclient/rest/MockSchemaRegistryClient.h"
#include "srclient/rest/ClientConfiguration.h"
#include "srclient/serdes/avro/AvroSerializer.h"
#include "srclient/serdes/avro/AvroDeserializer.h"
#include "srclient/serdes/SerdeConfig.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/RuleRegistry.h"
#include "srclient/rest/model/Schema.h"
#include "srclient/rest/model/Rule.h"
#include "srclient/rest/model/RuleSet.h"
#include "srclient/rules/cel/CelFieldExecutor.h"
#include "srclient/rules/encryption/FieldEncryptionExecutor.h"
#include "srclient/rules/encryption/EncryptionExecutor.h"
#include "srclient/rules/encryption/localkms/LocalKmsDriver.h"
#include "srclient/rest/MockDekRegistryClient.h"

using namespace srclient::serdes;
using namespace srclient::serdes::avro;
using namespace srclient::rest;
using namespace srclient::rest::model;
using namespace srclient::rules::cel;
using namespace srclient::rules::encryption;
using namespace srclient::rules::encryption::localkms;

TEST(AvroTest, BasicSerialization) {
    // Create client configuration with mock URL
    std::vector<std::string> urls = {"mock://"};
    auto client_config = std::make_shared<const ClientConfiguration>(urls);
    auto client = SchemaRegistryClient::newClient(client_config);
    
    // Create serializer and deserializer configurations
    auto ser_config = SerializerConfig::createDefault();
    auto deser_config = DeserializerConfig::createDefault();
    
    // Define the Avro schema
    const std::string schema_str = R"({
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "intField", "type": "int"},
            {"name": "doubleField", "type": "double"},
            {"name": "stringField", "type": "string"},
            {"name": "booleanField", "type": "boolean"},
            {"name": "bytesField", "type": "bytes"}
        ]
    })";
    
    // Create schema object
    srclient::rest::model::Schema schema;
    schema.setSchemaType(std::make_optional<std::string>("AVRO"));
    schema.setSchema(std::make_optional<std::string>(schema_str));
    
    // Parse the Avro schema
    std::istringstream schema_stream(schema_str);
    ::avro::ValidSchema avro_schema;
    ::avro::compileJsonSchema(schema_stream, avro_schema);
    
    // Create the Avro record
    ::avro::GenericDatum datum(avro_schema);
    auto& record = datum.value<::avro::GenericRecord>();
    
    // Set field values
    record.setFieldAt(0, ::avro::GenericDatum(static_cast<int32_t>(123)));           // intField
    record.setFieldAt(1, ::avro::GenericDatum(45.67));                              // doubleField  
    record.setFieldAt(2, ::avro::GenericDatum(std::string("hi")));                  // stringField
    record.setFieldAt(3, ::avro::GenericDatum(true));                               // booleanField
    record.setFieldAt(4, ::avro::GenericDatum(std::vector<uint8_t>{1, 2, 3}));      // bytesField
    
    // Create rule registry
    auto rule_registry = std::make_shared<RuleRegistry>();
    
    // Create serializer and deserializer
    AvroSerializer serializer(client, std::make_optional(schema), rule_registry, ser_config);
    AvroDeserializer deserializer(client, rule_registry, deser_config);
    
    // Create serialization context
    SerializationContext ser_ctx;
    ser_ctx.topic = "test";
    ser_ctx.serde_type = SerdeType::Value;
    ser_ctx.serde_format = SerdeFormat::Avro;
    
    // Serialize the record
    auto serialized_bytes = serializer.serialize(ser_ctx, datum);
    
    // Deserialize the record
    auto deserialized_value = deserializer.deserialize(ser_ctx, serialized_bytes);
    
    // Verify the deserialized record
    ASSERT_TRUE(deserialized_value.value.type() == ::avro::AVRO_RECORD);
    
    auto& deserialized_record = deserialized_value.value.value<::avro::GenericRecord>();
    ASSERT_EQ(deserialized_record.fieldCount(), 5);
    
    // Verify field values
    EXPECT_EQ(deserialized_record.fieldAt(0).value<int32_t>(), 123);
    EXPECT_DOUBLE_EQ(deserialized_record.fieldAt(1).value<double>(), 45.67);
    EXPECT_EQ(deserialized_record.fieldAt(2).value<std::string>(), "hi");
    EXPECT_EQ(deserialized_record.fieldAt(3).value<bool>(), true);
    
    auto bytes_field = deserialized_record.fieldAt(4).value<std::vector<uint8_t>>();
    ASSERT_EQ(bytes_field.size(), 3);
    EXPECT_EQ(bytes_field[0], 1);
    EXPECT_EQ(bytes_field[1], 2);
    EXPECT_EQ(bytes_field[2], 3);
}

TEST(AvroTest, CelFieldTransformation) {
    // Create client configuration with mock URL
    std::vector<std::string> urls = {"mock://"};
    auto client_config = std::make_shared<const ClientConfiguration>(urls);
    auto client = SchemaRegistryClient::newClient(client_config);
    
    // Create serializer configuration
    auto ser_config = SerializerConfig(
        false,  // auto_register_schemas
        std::make_optional(SchemaSelectorData::createLatestVersion()),  // use_schema
        true,   // normalize_schemas
        false,  // validate
        std::unordered_map<std::string, std::string>{}  // rule_config
    );
    auto deser_config = DeserializerConfig::createDefault();
    
    // Define the Avro schema
    const std::string schema_str = R"({
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "intField", "type": "int"},
            {"name": "doubleField", "type": "double"},
            {"name": "stringField", "type": "string"},
            {"name": "booleanField", "type": "boolean"},
            {"name": "bytesField", "type": "bytes"}
        ]
    })";
    
    // Create CEL rule for field transformation
    Rule cel_rule;
    cel_rule.setName(std::make_optional<std::string>("test-cel"));
    cel_rule.setKind(std::make_optional<Kind>(Kind::Transform));
    cel_rule.setMode(std::make_optional<Mode>(Mode::Write));
    cel_rule.setType(std::make_optional<std::string>("CEL_FIELD"));
    cel_rule.setExpr(std::make_optional<std::string>("name == 'stringField' ; value + '-suffix'"));
    
    // Create rule set with domain rules
    RuleSet rule_set;
    std::vector<Rule> domain_rules = {cel_rule};
    rule_set.setDomainRules(std::make_optional<std::vector<Rule>>(domain_rules));
    
    // Create schema object with rule set
    Schema schema;
    schema.setSchemaType(std::make_optional<std::string>("AVRO"));
    schema.setSchema(std::make_optional<std::string>(schema_str));
    schema.setRuleSet(std::make_optional<RuleSet>(rule_set));
    
    // Register the schema
    auto registered_schema = client->registerSchema("test-value", schema, false);
    
    // Parse the Avro schema
    std::istringstream schema_stream(schema_str);
    ::avro::ValidSchema avro_schema;
    ::avro::compileJsonSchema(schema_stream, avro_schema);
    
    // Create the Avro record with test data
    ::avro::GenericDatum datum(avro_schema);
    auto& record = datum.value<::avro::GenericRecord>();
    
    // Set field values
    record.setFieldAt(0, ::avro::GenericDatum(static_cast<int32_t>(123)));           // intField
    record.setFieldAt(1, ::avro::GenericDatum(45.67));                              // doubleField  
    record.setFieldAt(2, ::avro::GenericDatum(std::string("hi")));                  // stringField
    record.setFieldAt(3, ::avro::GenericDatum(true));                               // booleanField
    record.setFieldAt(4, ::avro::GenericDatum(std::vector<uint8_t>{1, 2, 3}));      // bytesField
    
    // Create rule registry and register CEL field executor
    auto rule_registry = std::make_shared<RuleRegistry>();
    auto cel_executor = std::make_shared<CelFieldExecutor>();
    rule_registry->registerExecutor(cel_executor);
    
    // Create serializer and deserializer
    AvroSerializer serializer(client, std::nullopt, rule_registry, ser_config);
    AvroDeserializer deserializer(client, rule_registry, deser_config);
    
    // Create serialization context
    SerializationContext ser_ctx;
    ser_ctx.topic = "test";
    ser_ctx.serde_type = SerdeType::Value;
    ser_ctx.serde_format = SerdeFormat::Avro;
    
    // Serialize the record
    auto serialized_bytes = serializer.serialize(ser_ctx, datum);
    
    // Deserialize the record
    auto deserialized_value = deserializer.deserialize(ser_ctx, serialized_bytes);
    
    // Verify the deserialized record
    ASSERT_TRUE(deserialized_value.value.type() == ::avro::AVRO_RECORD);
    
    auto& deserialized_record = deserialized_value.value.value<::avro::GenericRecord>();
    ASSERT_EQ(deserialized_record.fieldCount(), 5);
    
    // Verify field values - stringField should be transformed with "-suffix"
    EXPECT_EQ(deserialized_record.fieldAt(0).value<int32_t>(), 123);
    EXPECT_DOUBLE_EQ(deserialized_record.fieldAt(1).value<double>(), 45.67);
    EXPECT_EQ(deserialized_record.fieldAt(2).value<std::string>(), "hi-suffix"); // Should be transformed
    EXPECT_EQ(deserialized_record.fieldAt(3).value<bool>(), true);
    
    auto bytes_field = deserialized_record.fieldAt(4).value<std::vector<uint8_t>>();
    ASSERT_EQ(bytes_field.size(), 3);
    EXPECT_EQ(bytes_field[0], 1);
    EXPECT_EQ(bytes_field[1], 2);
    EXPECT_EQ(bytes_field[2], 3);
}

TEST(AvroTest, FieldEncryption) {
    // Register local KMS driver
    LocalKmsDriver::registerDriver();

    // Create client configuration with mock URL
    std::vector<std::string> urls = {"mock://"};
    auto client_config = std::make_shared<const ClientConfiguration>(urls);
    auto client = SchemaRegistryClient::newClient(client_config);
    
    // Create mock DEK registry client for encryption
    auto dek_client = std::make_shared<MockDekRegistryClient>(client_config);
    
    // Create rule configuration with secret
    std::unordered_map<std::string, std::string> rule_config = {
        {"secret", "mysecret"}
    };
    
    // Create serializer configuration
    auto ser_config = SerializerConfig(
        false,  // auto_register_schemas
        std::make_optional(SchemaSelectorData::createLatestVersion()),  // use_schema
        false,  // normalize_schemas
        false,  // validate
        rule_config  // rule_config
    );
    auto deser_config = DeserializerConfig::createDefault();
    
    // Define the Avro schema with PII tags
    const std::string schema_str = R"({
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "intField", "type": "int"},
            {"name": "doubleField", "type": "double"},
            {"name": "stringField", "type": "string", "confluent:tags": ["PII"]},
            {"name": "booleanField", "type": "boolean"},
            {"name": "bytesField", "type": "bytes", "confluent:tags": ["PII"]}
        ]
    })";
    
    // Create encryption rule for PII fields
    Rule encrypt_rule;
    encrypt_rule.setName(std::make_optional<std::string>("test-encrypt"));
    encrypt_rule.setKind(std::make_optional<Kind>(Kind::Transform));
    encrypt_rule.setMode(std::make_optional<Mode>(Mode::WriteRead));
    encrypt_rule.setType(std::make_optional<std::string>("ENCRYPT"));
    
    // Set rule tags to target PII fields
    std::vector<std::string> tags = {"PII"};
    encrypt_rule.setTags(std::make_optional<std::vector<std::string>>(tags));
    
    // Set rule parameters for local KMS - use std::map instead of std::unordered_map
    std::map<std::string, std::string> params = {
        {"encrypt.kek.name", "kek1"},
        {"encrypt.kms.type", "local-kms"},
        {"encrypt.kms.key.id", "mykey"}
    };
    encrypt_rule.setParams(std::make_optional<std::map<std::string, std::string>>(params));
    encrypt_rule.setOnFailure(std::make_optional<std::string>("ERROR,NONE"));
    
    // Create rule set with domain rules
    RuleSet rule_set;
    std::vector<Rule> domain_rules = {encrypt_rule};
    rule_set.setDomainRules(std::make_optional<std::vector<Rule>>(domain_rules));
    
    // Create schema object with rule set
    Schema schema;
    schema.setSchemaType(std::make_optional<std::string>("AVRO"));
    schema.setSchema(std::make_optional<std::string>(schema_str));
    schema.setRuleSet(std::make_optional<RuleSet>(rule_set));
    
    // Register the schema
    auto registered_schema = client->registerSchema("test-value", schema, false);
    
    // Parse the Avro schema
    std::istringstream schema_stream(schema_str);
    ::avro::ValidSchema avro_schema;
    ::avro::compileJsonSchema(schema_stream, avro_schema);
    
    // Create the Avro record with test data
    ::avro::GenericDatum datum(avro_schema);
    auto& record = datum.value<::avro::GenericRecord>();
    
    // Set field values
    record.setFieldAt(0, ::avro::GenericDatum(static_cast<int32_t>(123)));           // intField
    record.setFieldAt(1, ::avro::GenericDatum(45.67));                              // doubleField  
    record.setFieldAt(2, ::avro::GenericDatum(std::string("hi")));                  // stringField (PII)
    record.setFieldAt(3, ::avro::GenericDatum(true));                               // booleanField
    record.setFieldAt(4, ::avro::GenericDatum(std::vector<uint8_t>{1, 2, 3}));      // bytesField (PII)
    
    // Create rule registry and register field encryption executor
    auto rule_registry = std::make_shared<RuleRegistry>();
    auto clock = std::make_shared<SystemClock>();
    auto encryption_executor = std::make_shared<FieldEncryptionExecutor<MockDekRegistryClient>>(clock);
    rule_registry->registerExecutor(encryption_executor);
    
    // Create serializer and deserializer
    AvroSerializer serializer(client, std::nullopt, rule_registry, ser_config);
    AvroDeserializer deserializer(client, rule_registry, deser_config);
    
    // Create serialization context
    SerializationContext ser_ctx;
    ser_ctx.topic = "test";
    ser_ctx.serde_type = SerdeType::Value;
    ser_ctx.serde_format = SerdeFormat::Avro;
    
    // Serialize the record (PII fields should be encrypted)
    auto serialized_bytes = serializer.serialize(ser_ctx, datum);
    
    // Deserialize the record (PII fields should be decrypted)
    auto deserialized_value = deserializer.deserialize(ser_ctx, serialized_bytes);
    
    // Verify the deserialized record
    ASSERT_TRUE(deserialized_value.value.type() == ::avro::AVRO_RECORD);
    
    auto& deserialized_record = deserialized_value.value.value<::avro::GenericRecord>();
    ASSERT_EQ(deserialized_record.fieldCount(), 5);
    
    // Verify field values - should match original after encryption/decryption round trip
    EXPECT_EQ(deserialized_record.fieldAt(0).value<int32_t>(), 123);
    EXPECT_DOUBLE_EQ(deserialized_record.fieldAt(1).value<double>(), 45.67);
    EXPECT_EQ(deserialized_record.fieldAt(2).value<std::string>(), "hi");  // PII field
    EXPECT_EQ(deserialized_record.fieldAt(3).value<bool>(), true);
    
    auto bytes_field = deserialized_record.fieldAt(4).value<std::vector<uint8_t>>(); // PII field
    ASSERT_EQ(bytes_field.size(), 3);
    EXPECT_EQ(bytes_field[0], 1);
    EXPECT_EQ(bytes_field[1], 2);
    EXPECT_EQ(bytes_field[2], 3);
}