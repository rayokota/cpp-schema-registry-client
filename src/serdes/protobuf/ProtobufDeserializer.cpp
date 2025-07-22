#define SRCLIENT_PROTOBUF_SKIP_TEMPLATE_IMPL
#include "srclient/serdes/protobuf/ProtobufDeserializer.h"
#include "srclient/serdes/protobuf/ProtobufUtils.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include "srclient/serdes/json/JsonTypes.h"
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/json_util.h>
#include <nlohmann/json.hpp>
#include <optional>
#include <functional>

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

template<typename T>
std::unique_ptr<google::protobuf::Message> ProtobufDeserializer<T>::createMessageFromDescriptor(
    const google::protobuf::Descriptor* descriptor) {
    
    google::protobuf::DynamicMessageFactory factory;
    const google::protobuf::Message* prototype = factory.GetPrototype(descriptor);
    if (!prototype) {
        throw ProtobufError("Failed to get message prototype for descriptor: " + descriptor->full_name());
    }
    
    return std::unique_ptr<google::protobuf::Message>(prototype->New());
}

template<typename T>
ProtobufDeserializer<T>::ProtobufDeserializer(
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

template<typename T>
std::unique_ptr<T> ProtobufDeserializer<T>::deserialize(
    const SerializationContext& ctx,
    const std::vector<uint8_t>& data
) {
    auto strategy = base_->getConfig().subject_name_strategy;
    std::optional<std::string> subject = strategy(ctx.topic, ctx.serde_type, std::nullopt);
    std::optional<srclient::rest::model::RegisteredSchema> latest_schema;
    bool has_subject = subject.has_value();

    if (has_subject) {
        latest_schema = base_->getSerde().getReaderSchema(
            subject.value(),
            "serialized",
            base_->getConfig().use_schema
        );
    }

    SchemaId schema_id(SerdeFormat::Protobuf);
    auto id_deser = base_->getConfig().schema_id_deserializer;
    size_t bytes_read = id_deser(data, ctx, schema_id);
    std::vector<int32_t> msg_index = schema_id.getMessageIndexes().value_or(std::vector<int32_t>{});

    std::vector<uint8_t> remaining_data(data.begin() + bytes_read, data.end());

    auto writer_schema_raw = base_->getWriterSchema(schema_id, subject, "serialized");

    // Get parsed schema using ProtobufSerde
    auto [writer_schema, pool_ptr] = serde_->getParsedSchema(writer_schema_raw, base_->getSerde().getClient());

    if (!writer_schema) {
        throw ProtobufError("Failed to parse writer schema");
    }

    // Get writer message descriptor using getMessageDescriptorByIndex from ProtobufUtils
    const google::protobuf::Descriptor* writer_desc =
        srclient::serdes::protobuf::utils::getMessageDescriptorByIndex(
            pool_ptr,
            writer_schema,
            msg_index
        );

    if (!writer_desc) {
        throw ProtobufError("Failed to get writer message descriptor");
    }

    if (!has_subject) {
        subject = strategy(ctx.topic, ctx.serde_type, std::make_optional(writer_schema_raw));
        if (subject.has_value()) {
            latest_schema = base_->getSerde().getReaderSchema(
                subject.value(),
                "serialized",
                base_->getConfig().use_schema
            );
        }
    }

    if (!subject.has_value()) {
        throw ProtobufError("Subject name could not be determined");
    }

    std::string subject_str = subject.value();
    std::vector<uint8_t> processed_data = remaining_data;

    // Handle encoding rules if present
    auto rule_set = writer_schema_raw.getRuleSet();
    if (rule_set.has_value() && rule_set->getEncodingRules().has_value()) {

        auto serde_value = SerdeValue::newBytes(SerdeFormat::Protobuf, processed_data);
        auto result_value = base_->getSerde().executeRulesWithPhase(
            ctx,
            subject_str,
            Phase::Encoding,
            Mode::Read,
            std::nullopt,
            std::make_optional(writer_schema_raw),
            std::nullopt,
            *serde_value,
            std::unordered_map<std::string, std::unordered_set<std::string>>{}
        );
        processed_data = result_value->asBytes();
    }

    std::vector<Migration> migrations;
    srclient::rest::model::Schema reader_schema_raw;
    const google::protobuf::FileDescriptor* reader_schema;

    if (latest_schema.has_value()) {
        migrations = base_->getSerde().getMigrations(
            subject_str,
            writer_schema_raw,
            latest_schema.value(),
            std::nullopt
        );
        reader_schema_raw = latest_schema->toSchema();

        auto [reader_fd, pool_ptr] = serde_->getParsedSchema(reader_schema_raw, base_->getSerde().getClient());
        reader_schema = reader_fd;
    } else {
        reader_schema_raw = writer_schema_raw;
        reader_schema = writer_schema;
    }

    // Initialize reader desc to first message in file
    std::vector<int32_t> first_msg_index = {0};
    const google::protobuf::Descriptor* reader_desc =
        srclient::serdes::protobuf::utils::getMessageDescriptorByIndex(
            pool_ptr,
            reader_schema,
            first_msg_index
        );

    // Attempt to find a reader desc with the same name as the writer
    const google::protobuf::Descriptor* found_desc = pool_ptr->FindMessageTypeByName(writer_desc->full_name());
    if (found_desc) {
        reader_desc = found_desc;
    }

    google::protobuf::DynamicMessageFactory factory(pool_ptr);
    std::unique_ptr<google::protobuf::Message> msg;

    if (!migrations.empty()) {
        // Handle migrations case
        const google::protobuf::Message* prototype = factory.GetPrototype(writer_desc);
        msg = std::unique_ptr<google::protobuf::Message>(prototype->New());

        // Parse from binary data
        if (!msg->ParseFromArray(processed_data.data(), processed_data.size())) {
            throw ProtobufError("Failed to parse protobuf message from binary data");
        }

        // Convert to JSON
        std::string json_str;
        auto status = google::protobuf::util::MessageToJsonString(*msg, &json_str);
        if (!status.ok()) {
            throw ProtobufError("Failed to convert message to JSON: " + std::string(status.message()));
        }

        auto json_value = nlohmann::json::parse(json_str);
        auto serde_value = srclient::serdes::json::makeJsonValue(json_value);

        // Execute migrations
        auto migrated_value = base_->getSerde().executeMigrations(
            ctx,
            subject_str,
            migrations,
            *serde_value
        );

                    if (migrated_value->getFormat() != SerdeFormat::Json) {
            throw ProtobufError("Expected JSON value after migration");
        }

        auto migrated_json = asJson(*migrated_value);
        std::string migrated_json_str = migrated_json.dump();

        // Parse back to protobuf message
        google::protobuf::util::JsonParseOptions options;
        const google::protobuf::Message* reader_prototype = factory.GetPrototype(reader_desc);
        msg = std::unique_ptr<google::protobuf::Message>(reader_prototype->New());

        auto json_status = google::protobuf::util::JsonStringToMessage(migrated_json_str, msg.get(), options);
        if (!json_status.ok()) {
            throw ProtobufError("Failed to convert JSON back to protobuf: " + std::string(json_status.message()));
        }
    } else {
        // No migrations - direct parsing
        const google::protobuf::Message* prototype = factory.GetPrototype(reader_desc);
        msg = std::unique_ptr<google::protobuf::Message>(prototype->New());

        if (!msg->ParseFromArray(processed_data.data(), processed_data.size())) {
            throw ProtobufError("Failed to parse protobuf message from binary data");
        }
    }

    // Execute transformation rules
    auto field_transformer = [](RuleContext& ctx, const std::string& field_executor_type, const SerdeValue& value) -> std::unique_ptr<SerdeValue> {
        return srclient::serdes::protobuf::utils::transformFields(ctx, field_executor_type, value);
    };

    auto protobuf_value = makeProtobufValue(*msg);
    auto protobuf_schema = std::make_shared<ProtobufSchema>(reader_schema);

    auto result_value = base_->getSerde().executeRules(
        ctx,
        subject_str,
        Mode::Read,
        std::nullopt,
        std::make_optional(reader_schema_raw),
        std::make_optional<SerdeSchema*>(protobuf_schema.get()),
        *protobuf_value,
        std::unordered_map<std::string, std::unordered_set<std::string>>{},
        std::make_shared<FieldTransformer>(field_transformer)
    );

            if (result_value->getFormat() != SerdeFormat::Protobuf) {
        throw ProtobufError("Expected protobuf value after rule execution");
    }

    // Extract the final message
    google::protobuf::Message& final_message_ref = asProtobuf(*result_value);

    // Create a copy of the message to return
    const google::protobuf::Message* prototype = factory.GetPrototype(final_message_ref.GetDescriptor());
    auto result = std::unique_ptr<google::protobuf::Message>(prototype->New());
    result->CopyFrom(final_message_ref);

    return result;
}


template<typename T>
std::optional<std::string> ProtobufDeserializer<T>::getName(const google::protobuf::Message& message) {
    const google::protobuf::Descriptor* descriptor = message.GetDescriptor();
    if (descriptor) {
        return descriptor->full_name();
    }
    return std::nullopt;
}


template<typename T>
void ProtobufDeserializer<T>::close() {
    // Cleanup resources
    serde_->clear();
}

} // namespace srclient::serdes::protobuf

// Explicit instantiation for default type
template class srclient::serdes::protobuf::ProtobufDeserializer<google::protobuf::Message>;