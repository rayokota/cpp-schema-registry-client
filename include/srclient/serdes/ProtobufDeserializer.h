#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>

#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeConfig.h"
#include "srclient/serdes/ProtobufSerializer.h" // For ProtobufSerde
#include "srclient/serdes/ProtobufUtils.h"
#include "srclient/rest/ISchemaRegistryClient.h"

namespace srclient::serdes {

// Forward declarations
template<typename ClientType>
class ProtobufDeserializer;
struct SerializationContext;

/**
 * Protobuf deserializer class template
 * Based on ProtobufDeserializer from protobuf.rs (converted to synchronous)
 */
template<typename ClientType = srclient::rest::ISchemaRegistryClient>
class ProtobufDeserializer {
public:
    /**
     * Constructor
     */
    ProtobufDeserializer(std::shared_ptr<ClientType> client,
                        std::shared_ptr<RuleRegistry> rule_registry,
                        const DeserializerConfig& config);

    /**
     * Deserialize bytes to protobuf message using dynamic message
     */
    std::unique_ptr<google::protobuf::Message> deserialize(const std::vector<uint8_t>& bytes,
                                                          std::optional<std::string> subject = std::nullopt,
                                                          std::optional<std::string> format = std::nullopt);

    /**
     * Deserialize bytes to specific message type
     */
    template<typename MessageType>
    MessageType deserializeTo(const std::vector<uint8_t>& bytes,
                             std::optional<std::string> subject = std::nullopt,
                             std::optional<std::string> format = std::nullopt);

    /**
     * Deserialize with message descriptor
     */
    std::unique_ptr<google::protobuf::Message> deserializeWithMessageDescriptor(
        const std::vector<uint8_t>& bytes,
        const google::protobuf::Descriptor* descriptor,
        const srclient::rest::model::Schema& writer_schema,
        std::optional<std::string> subject = std::nullopt);

    /**
     * Close the deserializer and cleanup resources
     */
    void close();

private:
    std::shared_ptr<BaseDeserializer> base_;
    std::unique_ptr<ProtobufSerde> serde_;

    // Helper methods
    std::unique_ptr<google::protobuf::Message> createMessageFromDescriptor(
        const google::protobuf::Descriptor* descriptor);
    
    void transformFields(google::protobuf::Message* message,
                        const google::protobuf::Descriptor* descriptor,
                        const srclient::rest::model::Schema& writer_schema,
                        const srclient::rest::model::Schema& reader_schema);
    
    SerdeValue messageToSerdeValue(const google::protobuf::Message& message);
    
    void serdeValueToMessage(const SerdeValue& value, 
                           google::protobuf::Message* message);
    
    bool isEvolutionRequired(const srclient::rest::model::Schema& writer_schema,
                           const srclient::rest::model::Schema& reader_schema);
    
    std::unique_ptr<google::protobuf::Message> evolveMessage(
        const google::protobuf::Message& writer_message,
        const google::protobuf::Descriptor* reader_descriptor,
        const srclient::rest::model::Schema& writer_schema,
        const srclient::rest::model::Schema& reader_schema);
};

// Template method implementations

template<typename ClientType>
ProtobufDeserializer<ClientType>::ProtobufDeserializer(
    std::shared_ptr<ClientType> client,
    std::shared_ptr<RuleRegistry> rule_registry,
    const DeserializerConfig& config
) : base_(std::make_shared<BaseDeserializer>(Serde(client, rule_registry), config)),
    serde_(std::make_unique<ProtobufSerde>())
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

template<typename ClientType>
std::unique_ptr<google::protobuf::Message> ProtobufDeserializer<ClientType>::deserialize(
    const std::vector<uint8_t>& bytes,
    std::optional<std::string> subject,
    std::optional<std::string> format
) {
    // Create a dummy context for schema ID deserialization
    SerializationContext ctx;
    ctx.topic = subject.value_or("unknown");
    ctx.serde_type = SerdeType::Value;
    
    // Deserialize schema ID from bytes
    auto id_deserializer = base_->getConfig().schema_id_deserializer;
    SchemaId schema_id(SerdeFormat::Protobuf);
    size_t bytes_read = id_deserializer(bytes, ctx, schema_id);
    
    // Extract payload after schema ID
    std::vector<uint8_t> payload(bytes.begin() + bytes_read, bytes.end());
    
    // Get writer schema
    auto writer_schema = base_->getWriterSchema(schema_id, subject, format);
    
    // Parse schema to get descriptor
    auto [file_descriptor, descriptor_pool] = serde_->getParsedSchema(writer_schema, base_->getSerde().getClient());
    
    // Find message descriptor using message indexes
    const google::protobuf::Descriptor* descriptor = nullptr;
    if (schema_id.getMessageIndexes().has_value()) {
        auto indexes = schema_id.getMessageIndexes().value();
        // Navigate through nested message types using indexes
        for (size_t i = 0; i < file_descriptor->message_type_count() && !indexes.empty(); ++i) {
            if (indexes[0] == static_cast<int32_t>(i)) {
                descriptor = file_descriptor->message_type(i);
                indexes.erase(indexes.begin());
                
                // Navigate nested types
                while (!indexes.empty() && descriptor) {
                    if (indexes[0] < descriptor->nested_type_count()) {
                        descriptor = descriptor->nested_type(indexes[0]);
                        indexes.erase(indexes.begin());
                    } else {
                        break;
                    }
                }
                break;
            }
        }
    } else {
        // Use first message type as default
        if (file_descriptor->message_type_count() > 0) {
            descriptor = file_descriptor->message_type(0);
        }
    }
    
    if (!descriptor) {
        throw protobuf_utils::ProtobufSerdeError("Could not find message descriptor in schema");
    }
    
    return deserializeWithMessageDescriptor(payload, descriptor, writer_schema, subject);
}

template<typename ClientType>
template<typename MessageType>
MessageType ProtobufDeserializer<ClientType>::deserializeTo(
    const std::vector<uint8_t>& bytes,
    std::optional<std::string> subject,
    std::optional<std::string> format
) {
    auto message = deserialize(bytes, subject, format);
    
    // Try to cast to specific type
    auto typed_message = dynamic_cast<MessageType*>(message.get());
    if (!typed_message) {
        throw protobuf_utils::ProtobufSerdeError("Cannot cast deserialized message to requested type");
    }
    
    MessageType result = *typed_message;
    message.release(); // Transfer ownership
    return result;
}

template<typename ClientType>
std::unique_ptr<google::protobuf::Message> ProtobufDeserializer<ClientType>::deserializeWithMessageDescriptor(
    const std::vector<uint8_t>& bytes,
    const google::protobuf::Descriptor* descriptor,
    const srclient::rest::model::Schema& writer_schema,
    std::optional<std::string> subject
) {
    // Create message instance
    auto message = createMessageFromDescriptor(descriptor);
    
    // Apply pre-deserialization rules if rule registry exists
    if (base_->getSerde().getRuleRegistry()) {
        // TODO: Implement rule execution for protobuf messages
        // This would involve field transformations similar to Avro
    }
    
    // Parse protobuf message from bytes
    if (!message->ParseFromArray(bytes.data(), bytes.size())) {
        throw protobuf_utils::ProtobufSerdeError("Failed to parse protobuf message from bytes");
    }
    
    // Apply post-deserialization rules
    if (base_->getSerde().getRuleRegistry()) {
        // TODO: Implement encoding rule execution
    }
    
    return message;
}

template<typename ClientType>
void ProtobufDeserializer<ClientType>::close() {
    // Cleanup resources
    serde_->clear();
}

// Explicit template instantiation declaration
extern template class ProtobufDeserializer<srclient::rest::ISchemaRegistryClient>;

} // namespace srclient::serdes 