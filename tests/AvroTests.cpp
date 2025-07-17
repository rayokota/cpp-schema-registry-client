/**
 * AvroTest
 * Tests for the Avro serialization functionality
 */

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <string>
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

using namespace srclient::serdes;
using namespace srclient::serdes::avro;
using namespace srclient::rest;

TEST(AvroTest, BasicSerialization) {
    // Create client configuration with mock URL
    std::vector<std::string> urls = {"mock://"};
    auto client_config = std::make_shared<const ClientConfiguration>(urls);
    
    // Create mock schema registry client
    auto client = std::make_shared<MockSchemaRegistryClient>(client_config);
    
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