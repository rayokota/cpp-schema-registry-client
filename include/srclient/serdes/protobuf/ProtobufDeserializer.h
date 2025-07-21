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
#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeConfig.h"
#include "srclient/serdes/protobuf/ProtobufSerializer.h" // For ProtobufSerde
#include "srclient/serdes/protobuf/ProtobufUtils.h"
#include "srclient/rest/ISchemaRegistryClient.h"

namespace srclient::serdes::protobuf {

// Forward declarations
class ProtobufDeserializer;
using SerializationContext = srclient::serdes::SerializationContext;

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
     * Deserialize bytes to protobuf message with full context support
     * Based on the Rust deserialize method, ported to synchronous C++
     */
    std::unique_ptr<google::protobuf::Message> deserialize(const SerializationContext& ctx,
                                                          const std::vector<uint8_t>& data);

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
    
    /**
     * Get the message name from a protobuf message
     */
    std::optional<std::string> getName(const google::protobuf::Message& message);
    
    /**
     * Deserialize with message descriptor and apply rules
     */
    std::unique_ptr<google::protobuf::Message> deserializeWithMessageDescriptor(
        const std::vector<uint8_t>& payload,
        const google::protobuf::Descriptor* descriptor,
        const Schema& writer_schema,
        std::optional<std::string> subject);
};

} // namespace srclient::serdes::protobuf