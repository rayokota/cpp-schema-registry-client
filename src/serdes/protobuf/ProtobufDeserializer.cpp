#include "srclient/serdes/protobuf/ProtobufDeserializer.h"
#include "srclient/serdes/protobuf/ProtobufUtils.h"
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/json_util.h>

namespace srclient::serdes::protobuf {

// Helper method implementations for ProtobufDeserializer

std::unique_ptr<google::protobuf::Message> ProtobufDeserializer::createMessageFromDescriptor(
    const google::protobuf::Descriptor* descriptor) {
    
    google::protobuf::DynamicMessageFactory factory;
    const google::protobuf::Message* prototype = factory.GetPrototype(descriptor);
    if (!prototype) {
        throw ProtobufError("Failed to get message prototype for descriptor: " + descriptor->full_name());
    }
    
    return std::unique_ptr<google::protobuf::Message>(prototype->New());
}

ProtobufDeserializer::ProtobufDeserializer(
        std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
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
                    executor->configure(client->getConfiguration(), config.rule_config);
                }
            } catch (const std::exception& e) {
                throw ProtobufError("Failed to configure rule executor: " + std::string(e.what()));
            }
        }
    }
}

std::unique_ptr<google::protobuf::Message> ProtobufDeserializer::deserialize(
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
        throw ProtobufError("Could not find message descriptor in schema");
    }

    return deserializeWithMessageDescriptor(payload, descriptor, writer_schema, subject);
}

std::unique_ptr<google::protobuf::Message> ProtobufDeserializer::deserializeTo(
        const std::vector<uint8_t>& bytes,
        std::optional<std::string> subject,
        std::optional<std::string> format
) {
    return deserialize(bytes, subject, format);
}

std::unique_ptr<google::protobuf::Message> ProtobufDeserializer::deserializeWithMessageDescriptor(
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
        throw ProtobufError("Failed to parse protobuf message from bytes");
    }

    // Apply post-deserialization rules
    if (base_->getSerde().getRuleRegistry()) {
        // TODO: Implement encoding rule execution
    }

    return message;
}

void ProtobufDeserializer::close() {
    // Cleanup resources
    serde_->clear();
}

} // namespace srclient::serdes::protobuf