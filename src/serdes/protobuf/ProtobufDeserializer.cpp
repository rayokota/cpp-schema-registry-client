#include "srclient/serdes/protobuf/ProtobufDeserializer.h"
#include "srclient/serdes/protobuf/ProtobufUtils.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/json_util.h>

// Forward declaration for transformFields function from ProtobufUtils.cpp
namespace srclient::serdes::protobuf::utils {
    std::unique_ptr<srclient::serdes::SerdeValue> transformFields(
        srclient::serdes::RuleContext& ctx,
        const std::string& field_executor_type,
        const srclient::serdes::SerdeValue& value
    );
}

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
        const SerializationContext& ctx,
        const std::vector<uint8_t>& data
) {
    // Determine subject using strategy
    auto strategy = base_->getConfig().subject_name_strategy;
    auto subject_opt = strategy(ctx.topic, ctx.serde_type, std::nullopt);
    std::optional<srclient::rest::model::RegisteredSchema> latest_schema;
    bool has_subject = subject_opt.has_value();

    if (has_subject) {
        try {
            latest_schema = base_->getSerde().getReaderSchema(
                subject_opt.value(), std::nullopt, base_->getConfig().use_schema);
        } catch (const std::exception& e) {
            // Schema not found - will be determined from writer schema
        }
    }

    // Parse schema ID from data
    SchemaId schema_id(SerdeFormat::Protobuf);
    auto id_deserializer = base_->getConfig().schema_id_deserializer;
    size_t bytes_read = id_deserializer(data, ctx, schema_id);
    std::vector<uint8_t> payload_data(data.begin() + bytes_read, data.end());

    // Get writer schema
    auto writer_schema_raw = base_->getWriterSchema(schema_id, subject_opt, std::nullopt);
    auto [writer_file_descriptor, writer_pool] = serde_->getParsedSchema(writer_schema_raw, base_->getSerde().getClient());

    // Re-determine subject if not initially determined
    if (!has_subject) {
        subject_opt = strategy(ctx.topic, ctx.serde_type, std::make_optional(writer_schema_raw));
        if (subject_opt.has_value()) {
            try {
                latest_schema = base_->getSerde().getReaderSchema(
                    subject_opt.value(), std::nullopt, base_->getConfig().use_schema);
            } catch (const std::exception& e) {
                // Schema not found
            }
        }
    }

    if (!subject_opt.has_value()) {
        throw ProtobufError("Could not determine subject for deserialization");
    }
    std::string subject = subject_opt.value();

    // Handle encoding rules if present (pre-decode)
    std::vector<uint8_t> decoded_data = payload_data;
    auto rule_set = writer_schema_raw.getRuleSet();
    if (rule_set.has_value() && rule_set->getEncodingRules().has_value()) {
        auto bytes_value = SerdeValue::newBytes(SerdeFormat::Protobuf, payload_data);
        auto result = base_->getSerde().executeRulesWithPhase(
            ctx,
            subject,
            Phase::Encoding,
            Mode::Read,
            std::nullopt,
            std::make_optional(writer_schema_raw),
            std::nullopt,
            *bytes_value,
            {},
            nullptr
        );
        decoded_data = result->asBytes();
    }

    // Determine reader schema and migrations
    std::vector<Migration> migrations;
    Schema reader_schema_raw;
    const google::protobuf::FileDescriptor* reader_file_descriptor;
    const google::protobuf::DescriptorPool* reader_pool;

    if (latest_schema.has_value()) {
        migrations = base_->getSerde().getMigrations(subject, writer_schema_raw, latest_schema.value(), std::nullopt);
        reader_schema_raw = latest_schema->toSchema();
        auto reader_parsed = serde_->getParsedSchema(reader_schema_raw, base_->getSerde().getClient());
        reader_file_descriptor = reader_parsed.first;
        reader_pool = reader_parsed.second;
    } else {
        // No migrations needed
        reader_schema_raw = writer_schema_raw;
        reader_file_descriptor = writer_file_descriptor;
        reader_pool = writer_pool;
    }

    // Find message descriptor using message indexes
    const google::protobuf::Descriptor* descriptor = nullptr;
    if (schema_id.getMessageIndexes().has_value()) {
        auto indexes = schema_id.getMessageIndexes().value();
        // Navigate through nested message types using indexes
        for (size_t i = 0; i < writer_file_descriptor->message_type_count() && !indexes.empty(); ++i) {
            if (indexes[0] == static_cast<int32_t>(i)) {
                descriptor = writer_file_descriptor->message_type(i);
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
        if (writer_file_descriptor->message_type_count() > 0) {
            descriptor = writer_file_descriptor->message_type(0);
        }
    }

    if (!descriptor) {
        throw ProtobufError("Could not find message descriptor in schema");
    }

    // Create message from descriptor
    auto message = createMessageFromDescriptor(descriptor);
    
    // Parse the protobuf message from bytes
    if (!message->ParseFromArray(decoded_data.data(), decoded_data.size())) {
        throw ProtobufError("Failed to parse protobuf message from bytes");
    }

    // Apply migrations if we have them and a different reader schema
    if (!migrations.empty()) {
        // For protobuf, migrations would need to be applied by transforming the message
        // This is a complex operation that would require message-to-JSON conversion,
        // migration application, and back to protobuf
        // For now, we'll proceed without migration support
    }

    // Create field transformer for rule execution
    auto field_transformer = [&](RuleContext& ctx, const std::string& rule_type, const SerdeValue& value) -> std::unique_ptr<SerdeValue> {
        return utils::transformFields(ctx, rule_type, value);
    };

         // Apply rules if present
    if (reader_schema_raw.getRuleSet().has_value()) {
        auto protobuf_value = protobuf::makeProtobufValue(*message);
        auto protobuf_schema = protobuf::makeProtobufSchema(reader_schema_raw.getSchema().value_or(""));
         
        auto serde_value = base_->getSerde().executeRules(
            ctx,
            subject,
            Mode::Read,
            std::nullopt,
            std::make_optional(reader_schema_raw),
            std::make_optional(protobuf_schema.get()),
            *protobuf_value,
            {},
            std::make_shared<FieldTransformer>(field_transformer)
        );
         
        if (!serde_value->isProtobuf()) {
            throw ProtobufError("Unexpected serde value type after rule execution");
        }
         
        // Extract the transformed message
        auto transformed_message_ref = std::any_cast<std::reference_wrapper<google::protobuf::Message>>(serde_value->getValue());
         
        // Create a new message from the same descriptor and copy the data
        auto new_message = createMessageFromDescriptor(descriptor);
        new_message->CopyFrom(transformed_message_ref.get());
        message = std::move(new_message);
    }

    return message;
}

std::optional<std::string> ProtobufDeserializer::getName(const google::protobuf::Message& message) {
    const google::protobuf::Descriptor* descriptor = message.GetDescriptor();
    if (descriptor) {
        return descriptor->full_name();
    }
    return std::nullopt;
}

std::unique_ptr<google::protobuf::Message> ProtobufDeserializer::deserializeWithMessageDescriptor(
        const std::vector<uint8_t>& payload,
        const google::protobuf::Descriptor* descriptor,
        const Schema& writer_schema,
        std::optional<std::string> subject) {
    
    // Create message from descriptor
    auto message = createMessageFromDescriptor(descriptor);
    
    // Parse the protobuf message from bytes
    if (!message->ParseFromArray(payload.data(), payload.size())) {
        throw ProtobufError("Failed to parse protobuf message from bytes");
    }

    return message;
}

void ProtobufDeserializer::close() {
    // Cleanup resources
    serde_->clear();
}

} // namespace srclient::serdes::protobuf