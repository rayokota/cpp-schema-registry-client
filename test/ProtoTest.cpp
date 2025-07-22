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
#include "srclient/serdes/SerdeConfig.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/RuleRegistry.h"
#include "srclient/serdes/protobuf/ProtobufSerializer.h"
#include "srclient/serdes/protobuf/ProtobufDeserializer.h"
#include "srclient/serdes/protobuf/ProtobufUtils.h"
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

#include "test/dep.pb.h"
#include "test/example.pb.h"
#include "test/test.pb.h"

using namespace srclient::serdes;
using namespace srclient::serdes::protobuf;
using namespace srclient::rest;
using namespace srclient::rest::model;
using namespace srclient::rules::cel;
using namespace srclient::rules::encryption;
using namespace srclient::rules::encryption::localkms;

TEST(ProtobufTest, BasicSerialization) {
    // Create client configuration with mock URL
    std::vector<std::string> urls = {"mock://"};
    auto client_config = std::make_shared<const ClientConfiguration>(urls);
    auto client = std::make_shared<MockSchemaRegistryClient>(client_config);
    
    // Create serializer configuration
    auto ser_conf = SerializerConfig::createDefault();
    
    // Create Author object with test data
    test::Author obj;
    obj.set_name("Kafka");
    obj.set_id(123);
    obj.set_picture(std::string({1, 2, 3})); // bytes field
    obj.add_works("Metamorphosis");
    obj.add_works("The Trial");
    obj.set_oneof_string("oneof");
    
    // Create rule registry
    auto rule_registry = std::make_shared<RuleRegistry>();
    
    // Create protobuf serializer with default reference subject name strategy
    ProtobufSerializer<> ser(
        client,
        std::nullopt, // schema
        rule_registry,
        ser_conf,
        defaultReferenceSubjectNameStrategy
    );
    
    // Create serialization context
    SerializationContext ser_ctx;
    ser_ctx.topic = "test";
    ser_ctx.serde_type = SerdeType::Value;
    ser_ctx.serde_format = SerdeFormat::Protobuf;
    ser_ctx.headers = std::nullopt;

    // Serialize the Author object
    auto bytes = ser.serialize(ser_ctx, obj);
    
    // Create protobuf deserializer
    ProtobufDeserializer<> deser(
        client,
        rule_registry,
        DeserializerConfig::createDefault()
    );
    
    // Deserialize the bytes back to Author object
    auto obj2_ptr = deser.deserialize(ser_ctx, bytes);
    
    // Cast to the specific message type
    auto obj2 = dynamic_cast<const test::Author*>(obj2_ptr.get());
    ASSERT_NE(obj2, nullptr);
    
    // Assert objects are equal
    EXPECT_EQ(obj2->name(), obj.name());
    EXPECT_EQ(obj2->id(), obj.id());
    EXPECT_EQ(obj2->picture(), obj.picture());
    EXPECT_EQ(obj2->works_size(), obj.works_size());
    for (int i = 0; i < obj.works_size(); ++i) {
        EXPECT_EQ(obj2->works(i), obj.works(i));
    }
    EXPECT_EQ(obj2->oneof_string(), obj.oneof_string());
}
