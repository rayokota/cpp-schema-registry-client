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

using SerializationContext = srclient::serdes::SerializationContext;

namespace srclient::serdes::protobuf {

// Forward declaration of templated deserializer

template<typename T = google::protobuf::Message>
class ProtobufDeserializer {
public:
    ProtobufDeserializer(std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
                        std::shared_ptr<RuleRegistry> rule_registry,
                        const DeserializerConfig& config);

    std::unique_ptr<T> deserialize(const SerializationContext& ctx,
                                   const std::vector<uint8_t>& data);

    void close();

private:
    std::shared_ptr<BaseDeserializer> base_;
    std::unique_ptr<ProtobufSerde> serde_;

    std::unique_ptr<google::protobuf::Message> createMessageFromDescriptor(
        const google::protobuf::Descriptor* descriptor);

    std::optional<std::string> getName(const google::protobuf::Message& message);

    std::unique_ptr<google::protobuf::Message> deserializeWithMessageDescriptor(
        const std::vector<uint8_t>& payload,
        const google::protobuf::Descriptor* descriptor,
        const Schema& writer_schema,
        std::optional<std::string> subject);
};

} // namespace srclient::serdes::protobuf