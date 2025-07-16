#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>

#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeConfig.h"
#include "srclient/serdes/ProtobufUtils.h"
#include "srclient/rest/ISchemaRegistryClient.h"

namespace srclient::serdes {

// Forward declarations
class ProtobufSerializer;

class ProtobufSerde;

/**
 * Reference subject name strategy function type
 * Converts reference name and serde type to subject name
 */
using ReferenceSubjectNameStrategy = std::function<std::string(const std::string& ref_name, SerdeType serde_type)>;

/**
 * Default reference subject name strategy implementation
 */
std::string defaultReferenceSubjectNameStrategy(const std::string& ref_name, SerdeType serde_type);

/**
 * Protobuf schema caching and parsing class
 * Based on ProtobufSerde struct from protobuf.rs (converted to synchronous)
 */
class ProtobufSerde {
public:
    ProtobufSerde();
    ~ProtobufSerde() = default;

    // Schema parsing and caching
    std::pair<const google::protobuf::FileDescriptor*, const google::protobuf::DescriptorPool*> 
    getParsedSchema(const srclient::rest::model::Schema& schema, 
                   std::shared_ptr<srclient::rest::ISchemaRegistryClient> client);

    // Clear cache
    void clear();

private:
    // Cache for parsed schemas: Schema -> (FileDescriptor, DescriptorPool)
    std::unordered_map<std::string, std::pair<
        std::unique_ptr<google::protobuf::FileDescriptor>, 
        std::unique_ptr<google::protobuf::DescriptorPool>
    >> parsed_schemas_cache_;
    
    mutable std::mutex cache_mutex_;
    
    // Helper methods
    void resolveNamedSchema(const srclient::rest::model::Schema& schema,
                           std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
                           google::protobuf::DescriptorPool* pool,
                           std::unordered_set<std::string>& visited);
};

/**
 * Protobuf serializer class template
 * Based on ProtobufSerializer from protobuf.rs (converted to synchronous)
 */
class ProtobufSerializer {
public:
    /**
     * Constructor with default reference subject name strategy
     */
    ProtobufSerializer(std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
                      std::optional<srclient::rest::model::Schema> schema,
                      std::shared_ptr<RuleRegistry> rule_registry,
                      const SerializerConfig& config);

    /**
     * Constructor with custom reference subject name strategy
     */
    ProtobufSerializer(std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
                      std::optional<srclient::rest::model::Schema> schema,
                      std::shared_ptr<RuleRegistry> rule_registry,
                      const SerializerConfig& config,
                      ReferenceSubjectNameStrategy strategy);

    /**
     * Serialize a protobuf message
     */
    template<typename MessageType>
    std::vector<uint8_t> serialize(const SerializationContext& ctx, 
                                  const MessageType& message);

    /**
     * Serialize with file descriptor set
     */
    template<typename MessageType>
    std::vector<uint8_t> serializeWithFileDescriptorSet(const SerializationContext& ctx,
                                                        const MessageType& message,
                                                        const std::string& message_type_name,
                                                        const google::protobuf::FileDescriptorSet& fds);

    /**
     * Serialize with message descriptor
     */
    template<typename MessageType>
    std::vector<uint8_t> serializeWithMessageDescriptor(const SerializationContext& ctx,
                                                        const MessageType& message,
                                                        const google::protobuf::Descriptor* descriptor);

private:
    std::optional<srclient::rest::model::Schema> schema_;
    std::shared_ptr<BaseSerializer> base_;
    std::unique_ptr<ProtobufSerde> serde_;
    ReferenceSubjectNameStrategy reference_subject_name_strategy_;

    // Helper methods
    std::vector<int32_t> toIndexArray(const google::protobuf::Descriptor* descriptor);
    
    void validateSchema(const srclient::rest::model::Schema& schema);
    
    SerdeValue messageToSerdeValue(const google::protobuf::Message& message);
    
    SerdeValue transformValue(const SerdeValue& value, 
                            const srclient::rest::model::Rule& rule,
                            const RuleContext& context);
};

// Template method implementations

ProtobufSerializer::ProtobufSerializer(
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    std::optional<srclient::rest::model::Schema> schema,
    std::shared_ptr<RuleRegistry> rule_registry,
    const SerializerConfig& config
) : schema_(std::move(schema)),
    base_(std::make_shared<BaseSerializer>(Serde(client, rule_registry), config)),
    serde_(std::make_unique<ProtobufSerde>()),
    reference_subject_name_strategy_(defaultReferenceSubjectNameStrategy)
{
    // Configure rule executors
    if (rule_registry) {
        auto executors = rule_registry->getExecutors();
        for (const auto& executor : executors) {
            try {
                auto rule_registry = base_->getSerde().getRuleRegistry();
                if (rule_registry) {
                    auto client = base_->getSerde().getClient();
                    // TODO: Fix ClientConfiguration vs ServerConfig conversion
                    // executor->configure(client->getConfig("default"), config.rule_config);
                }
            } catch (const std::exception& e) {
                throw protobuf_utils::ProtobufSerdeError("Failed to configure rule executor: " + std::string(e.what()));
            }
        }
    }
}

ProtobufSerializer::ProtobufSerializer(
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    std::optional<srclient::rest::model::Schema> schema,
    std::shared_ptr<RuleRegistry> rule_registry,
    const SerializerConfig& config,
    ReferenceSubjectNameStrategy strategy
) : schema_(std::move(schema)),
    base_(std::make_shared<BaseSerializer>(Serde(client, rule_registry), config)),
    serde_(std::make_unique<ProtobufSerde>()),
    reference_subject_name_strategy_(strategy)
{
    // Configure rule executors
    if (rule_registry) {
        auto executors = rule_registry->getExecutors();
        for (const auto& executor : executors) {
            try {
                auto rule_registry = base_->getSerde().getRuleRegistry();
                if (rule_registry) {
                    auto client = base_->getSerde().getClient();
                    // TODO: Fix ClientConfiguration vs ServerConfig conversion
                    // executor->configure(client->getConfig("default"), config.rule_config);
                }
            } catch (const std::exception& e) {
                throw protobuf_utils::ProtobufSerdeError("Failed to configure rule executor: " + std::string(e.what()));
            }
        }
    }
}

template<typename MessageType>
std::vector<uint8_t> ProtobufSerializer::serialize(
    const SerializationContext& ctx,
    const MessageType& message
) {
    return serializeWithMessageDescriptor(ctx, message, message.GetDescriptor());
}

template<typename MessageType>
std::vector<uint8_t> ProtobufSerializer::serializeWithFileDescriptorSet(
    const SerializationContext& ctx,
    const MessageType& message,
    const std::string& message_type_name,
    const google::protobuf::FileDescriptorSet& fds
) {
    // Create descriptor pool from file descriptor set
    google::protobuf::DescriptorPool pool;
    for (const auto& file_desc : fds.file()) {
        const google::protobuf::FileDescriptor* file = pool.BuildFile(file_desc);
        if (!file) {
            throw protobuf_utils::ProtobufSerdeError("Failed to build file descriptor from set");
        }
    }
    
    const google::protobuf::Descriptor* descriptor = pool.FindMessageTypeByName(message_type_name);
    if (!descriptor) {
        throw protobuf_utils::ProtobufSerdeError("Message descriptor " + message_type_name + " not found");
    }
    
    return serializeWithMessageDescriptor(ctx, message, descriptor);
}

template<typename MessageType>
std::vector<uint8_t> ProtobufSerializer::serializeWithMessageDescriptor(
    const SerializationContext& ctx,
    const MessageType& message,
    const google::protobuf::Descriptor* descriptor
) {
    // Get subject using strategy
    auto strategy = base_->getConfig().subject_name_strategy;
    auto subject_opt = strategy(ctx.topic, ctx.serde_type, schema_);
    if (!subject_opt.has_value()) {
        throw protobuf_utils::ProtobufSerdeError("Subject name strategy returned no subject");
    }
    std::string subject = subject_opt.value();
    
    // Get or register schema
    SchemaId schema_id(SerdeFormat::Protobuf);
    std::optional<srclient::rest::model::RegisteredSchema> latest_schema;
    std::vector<uint8_t> encoded_bytes;
    
    try {
        latest_schema = base_->getSerde().getReaderSchema(subject, std::nullopt, base_->getConfig().use_schema);
    } catch (const std::exception& e) {
        // Schema not found - will use provided schema
    }
    
    if (latest_schema.has_value()) {
        auto schema = latest_schema->toSchema();
        auto parsed_schema = serde_->getParsedSchema(schema, base_->getSerde().getClient());
        
        // Apply rules if rule registry exists
        if (base_->getSerde().getRuleRegistry()) {
            // TODO: Implement rule execution for protobuf messages
            // This would involve field transformations similar to Avro
        }
        
        // Serialize message to bytes
        if (!message.SerializeToString(reinterpret_cast<std::string*>(&encoded_bytes))) {
            throw protobuf_utils::ProtobufSerdeError("Failed to serialize protobuf message");
        }
        
        // Apply encoding rules if present
        auto rule_set = schema.getRuleSet();
        if (rule_set.has_value()) {
            // TODO: Implement encoding rule execution
        }
    } else {
        // Direct serialization without schema evolution
        if (!message.SerializeToString(reinterpret_cast<std::string*>(&encoded_bytes))) {
            throw protobuf_utils::ProtobufSerdeError("Failed to serialize protobuf message");
        }
    }
    
    // Set message indexes for nested messages
    schema_id.setMessageIndexes(toIndexArray(descriptor));
    
    // Serialize schema ID with message
    auto id_serializer = base_->getConfig().schema_id_serializer;
    return id_serializer(encoded_bytes, ctx, schema_id);
}

} // namespace srclient::serdes