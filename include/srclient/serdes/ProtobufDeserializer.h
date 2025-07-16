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
class ProtobufDeserializer;
struct SerializationContext;

/**
 * Protobuf deserializer class template
 * Based on ProtobufDeserializer from protobuf.rs (converted to synchronous)
 */
class ProtobufDeserializer {
public:
    /**
     * Constructor
     */
    ProtobufDeserializer(std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
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

} // namespace srclient::serdes