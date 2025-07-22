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
#include "srclient/serdes/json/JsonTypes.h"
#include <google/protobuf/util/json_util.h>
#include <nlohmann/json.hpp>

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

// ---------------------- ProtobufDeserializer -----------------------------

#ifndef SRCLIENT_PROTOBUF_SKIP_TEMPLATE_IMPL

// Forward declaration for transformFields helper (defined in ProtobufUtils.cpp)
namespace utils {
std::unique_ptr<srclient::serdes::SerdeValue> transformFields(
        srclient::serdes::RuleContext &ctx,
        const std::string &field_executor_type,
        const srclient::serdes::SerdeValue &value);
}

template<typename T>
inline std::unique_ptr<google::protobuf::Message> ProtobufDeserializer<T>::createMessageFromDescriptor(
        const google::protobuf::Descriptor *descriptor) {
    google::protobuf::DynamicMessageFactory factory;
    const auto *prototype = factory.GetPrototype(descriptor);
    if (!prototype) {
        throw ProtobufError("Failed to get message prototype for descriptor: " + descriptor->full_name());
    }
    return std::unique_ptr<google::protobuf::Message>(prototype->New());
}

template<typename T>
inline ProtobufDeserializer<T>::ProtobufDeserializer(
        std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
        std::shared_ptr<RuleRegistry> rule_registry,
        const DeserializerConfig &config)
        : base_(std::make_shared<BaseDeserializer>(Serde(std::move(client), rule_registry), config)),
          serde_(std::make_unique<ProtobufSerde>()) {
    if (rule_registry) {
        for (const auto &executor: rule_registry->getExecutors()) {
            try {
                auto rr = base_->getSerde().getRuleRegistry();
                if (rr) {
                    executor->configure(base_->getSerde().getClient()->getConfiguration(), config.rule_config);
                }
            } catch (const std::exception &e) {
                throw ProtobufError("Failed to configure rule executor: " + std::string(e.what()));
            }
        }
    }
}

template<typename T>
inline std::unique_ptr<T> ProtobufDeserializer<T>::deserialize(
        const SerializationContext &ctx,
        const std::vector<uint8_t> &data) {
    using namespace srclient::serdes;
    using namespace srclient::serdes::protobuf;

    auto strategy     = base_->getConfig().subject_name_strategy;
    auto subject_opt  = strategy(ctx.topic, ctx.serde_type, std::nullopt);
    bool has_subject  = subject_opt.has_value();
    std::optional<srclient::rest::model::RegisteredSchema> latest_schema;

    if (has_subject) {
        latest_schema = base_->getSerde().getReaderSchema(subject_opt.value(), "serialized", base_->getConfig().use_schema);
    }

    SchemaId schema_id(SerdeFormat::Protobuf);
    size_t   bytes_read = base_->getConfig().schema_id_deserializer(data, ctx, schema_id);
    std::vector<int32_t> msg_index = schema_id.getMessageIndexes().value_or(std::vector<int32_t>{});

    std::vector<uint8_t> remaining_data(data.begin() + bytes_read, data.end());

    auto writer_schema_raw = base_->getWriterSchema(schema_id, subject_opt, "serialized");
    auto [writer_schema, pool_ptr] = serde_->getParsedSchema(writer_schema_raw, base_->getSerde().getClient());
    if (!writer_schema) {
        throw ProtobufError("Failed to parse writer schema");
    }

    const auto *writer_desc = utils::getMessageDescriptorByIndex(pool_ptr, writer_schema, msg_index);
    if (!writer_desc) {
        throw ProtobufError("Failed to get writer message descriptor");
    }

    if (!has_subject) {
        subject_opt = strategy(ctx.topic, ctx.serde_type, std::make_optional(writer_schema_raw));
        if (subject_opt) {
            latest_schema = base_->getSerde().getReaderSchema(subject_opt.value(), "serialized", base_->getConfig().use_schema);
        }
    }

    if (!subject_opt) {
        throw ProtobufError("Subject name could not be determined");
    }
    std::string subject = *subject_opt;

    std::vector<uint8_t> processed_data = remaining_data;

    // Handle encoding rules on the writer schema
    if (auto rule_set_opt = writer_schema_raw.getRuleSet(); rule_set_opt && rule_set_opt->getEncodingRules()) {
        auto bytes_val = SerdeValue::newBytes(SerdeFormat::Protobuf, processed_data);
        auto res_val   = base_->getSerde().executeRulesWithPhase(
                ctx,
                subject,
                Phase::Encoding,
                Mode::Read,
                std::nullopt,
                std::make_optional(writer_schema_raw),
                std::nullopt,
                *bytes_val,
                {});
        processed_data = res_val->asBytes();
    }

    // Determine reader schema and possible migrations
    std::vector<Migration> migrations;
    srclient::rest::model::Schema reader_schema_raw;
    const google::protobuf::FileDescriptor *reader_schema_fd;

    if (latest_schema) {
        migrations        = base_->getSerde().getMigrations(subject, writer_schema_raw, *latest_schema, std::nullopt);
        reader_schema_raw = latest_schema->toSchema();
        std::tie(reader_schema_fd, std::ignore) = serde_->getParsedSchema(reader_schema_raw, base_->getSerde().getClient());
    } else {
        reader_schema_raw   = writer_schema_raw;
        reader_schema_fd    = writer_schema;
    }

    // Determine reader descriptor
    const google::protobuf::Descriptor *reader_desc = utils::getMessageDescriptorByIndex(pool_ptr, reader_schema_fd, {0});
    if (const auto *same_name = pool_ptr->FindMessageTypeByName(writer_desc->full_name()); same_name) {
        reader_desc = same_name;
    }

    google::protobuf::DynamicMessageFactory factory(pool_ptr);
    std::unique_ptr<google::protobuf::Message> msg;

    if (!migrations.empty()) {
        // Parse writer message first
        const auto *writer_prototype = factory.GetPrototype(writer_desc);
        msg = std::unique_ptr<google::protobuf::Message>(writer_prototype->New());
        if (!msg->ParseFromArray(processed_data.data(), static_cast<int>(processed_data.size()))) {
            throw ProtobufError("Failed to parse protobuf message from binary data");
        }

        // Convert to JSON for migration
        std::string json_str;
        if (!google::protobuf::util::MessageToJsonString(*msg, &json_str).ok()) {
            throw ProtobufError("Failed to convert message to JSON during migration");
        }
        auto json_val     = nlohmann::json::parse(json_str);
        auto serde_json   = srclient::serdes::json::makeJsonValue(json_val);
        auto migrated_val = base_->getSerde().executeMigrations(ctx, subject, migrations, *serde_json);

        if (migrated_val->getFormat() != SerdeFormat::Json) {
            throw ProtobufError("Expected JSON value after migrations");
        }
        std::string migrated_json = asJson(*migrated_val).dump();

        // Parse back to reader message type
        const auto *reader_prototype = factory.GetPrototype(reader_desc);
        msg                           = std::unique_ptr<google::protobuf::Message>(reader_prototype->New());
        google::protobuf::util::JsonParseOptions opts;
        if (!google::protobuf::util::JsonStringToMessage(migrated_json, msg.get(), opts).ok()) {
            throw ProtobufError("Failed to convert migrated JSON back to protobuf");
        }
    } else {
        const auto *reader_proto = factory.GetPrototype(reader_desc);
        msg = std::unique_ptr<google::protobuf::Message>(reader_proto->New());
        if (!msg->ParseFromArray(processed_data.data(), static_cast<int>(processed_data.size()))) {
            throw ProtobufError("Failed to parse protobuf message from binary data");
        }
    }

    // Execute field-level rules
    auto field_tf = [](RuleContext &rctx, const std::string &rule_type, const SerdeValue &val) {
        return utils::transformFields(rctx, rule_type, val);
    };

    auto protobuf_val   = makeProtobufValue(*msg);
    auto protobuf_schema = std::make_shared<ProtobufSchema>(reader_schema_fd);

    auto result_val = base_->getSerde().executeRules(
            ctx,
            subject,
            Mode::Read,
            std::nullopt,
            std::make_optional(reader_schema_raw),
            std::make_optional<SerdeSchema *>(protobuf_schema.get()),
            *protobuf_val,
            {},
            std::make_shared<FieldTransformer>(field_tf));

    if (result_val->getFormat() != SerdeFormat::Protobuf) {
        throw ProtobufError("Expected protobuf value after rule execution");
    }

    // Copy final message into a newly created T instance
    google::protobuf::Message &final_msg = asProtobuf(*result_val);
    const auto *proto_proto             = factory.GetPrototype(final_msg.GetDescriptor());
    auto out_msg                        = std::unique_ptr<google::protobuf::Message>(proto_proto->New());
    out_msg->CopyFrom(final_msg);

    // Downcast to the concrete type requested by the caller, if possible.
    return std::unique_ptr<T>(dynamic_cast<T *>(out_msg.release()));
}

template<typename T>
inline void ProtobufDeserializer<T>::close() {
    serde_->clear();
}

#endif // SRCLIENT_PROTOBUF_SKIP_TEMPLATE_IMPL

} // namespace srclient::serdes::protobuf