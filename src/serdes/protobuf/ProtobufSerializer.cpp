#include "srclient/serdes/protobuf/ProtobufSerializer.h"
#include "srclient/serdes/protobuf/ProtobufUtils.h"

namespace srclient::serdes::protobuf {

using namespace utils;

// Default reference subject name strategy implementation
std::string defaultReferenceSubjectNameStrategy(const std::string& ref_name, SerdeType serde_type) {
    return ref_name;
}

// ProtobufSerde implementation
ProtobufSerde::ProtobufSerde() {}

std::pair<const google::protobuf::FileDescriptor*, const google::protobuf::DescriptorPool*> 
ProtobufSerde::getParsedSchema(const srclient::rest::model::Schema& schema, 
                              std::shared_ptr<srclient::rest::ISchemaRegistryClient> client) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Create cache key from schema content
    auto schema_str = schema.getSchema();
    std::string cache_key = schema_str.value_or("");
    
    auto it = parsed_schemas_cache_.find(cache_key);
    if (it != parsed_schemas_cache_.end()) {
        return {it->second.first.get(), it->second.second.get()};
    }
    
    // Parse new schema
    auto pool = std::make_unique<google::protobuf::DescriptorPool>();
    std::unordered_set<std::string> visited;
    
    // Resolve dependencies first
    auto references = schema.getReferences();
    if (references.has_value()) {
        for (const auto& ref : references.value()) {
            resolveNamedSchema(schema, client, pool.get(), visited);
        }
    }
    
    // Parse main schema
    auto file_desc = stringToSchema(pool.get(), "main.proto", cache_key);
    
    // Store in cache
    parsed_schemas_cache_[cache_key] = std::make_pair(
        std::unique_ptr<google::protobuf::FileDescriptor>(const_cast<google::protobuf::FileDescriptor*>(file_desc)),
        std::move(pool)
    );
    
    return {file_desc, parsed_schemas_cache_[cache_key].second.get()};
}

void ProtobufSerde::clear() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    parsed_schemas_cache_.clear();
}

void ProtobufSerde::resolveNamedSchema(const srclient::rest::model::Schema& schema,
                                      std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
                                      google::protobuf::DescriptorPool* pool,
                                      std::unordered_set<std::string>& visited) {
    // TODO: Implement dependency resolution
    // This would recursively resolve schema references
}

std::unique_ptr<SerdeValue> ProtobufSerializer::messageToSerdeValue(const google::protobuf::Message& message) {
    // Create and return the SerdeValue as unique_ptr
    return makeProtobufValue(const_cast<google::protobuf::Message&>(message));
}

std::unique_ptr<SerdeValue> ProtobufSerializer::transformValue(SerdeValue& value,
                                             const Schema& schema,
                                             const std::string& subject) {
    // Apply transformations and return as unique_ptr
    // For now, create a copy and return it
    // TODO: Implement actual transformations
    return value.clone();
}

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
                throw ProtobufError("Failed to configure rule executor: " + std::string(e.what()));
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
                throw ProtobufError("Failed to configure rule executor: " + std::string(e.what()));
            }
        }
    }
}

std::vector<uint8_t> ProtobufSerializer::serialize(
        const SerializationContext& ctx,
        const google::protobuf::Message& message
) {
    return serializeWithMessageDescriptor(ctx, message, message.GetDescriptor());
}

std::vector<uint8_t> ProtobufSerializer::serializeWithFileDescriptorSet(
        const SerializationContext& ctx,
        const google::protobuf::Message& message,
        const std::string& message_type_name,
        const google::protobuf::FileDescriptorSet& fds
) {
    // Create descriptor pool from file descriptor set
    google::protobuf::DescriptorPool pool;
    for (const auto& file_desc : fds.file()) {
        const google::protobuf::FileDescriptor* file = pool.BuildFile(file_desc);
        if (!file) {
            throw ProtobufError("Failed to build file descriptor from set");
        }
    }

    const google::protobuf::Descriptor* descriptor = pool.FindMessageTypeByName(message_type_name);
    if (!descriptor) {
        throw ProtobufError("Message descriptor " + message_type_name + " not found");
    }

    return serializeWithMessageDescriptor(ctx, message, descriptor);
}

std::vector<uint8_t> ProtobufSerializer::serializeWithMessageDescriptor(
        const SerializationContext& ctx,
        const google::protobuf::Message& message,
        const google::protobuf::Descriptor* descriptor
) {
    // Get subject using strategy
    auto strategy = base_->getConfig().subject_name_strategy;
    auto subject_opt = strategy(ctx.topic, ctx.serde_type, schema_);
    if (!subject_opt.has_value()) {
        throw ProtobufError("Subject name strategy returned no subject");
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
            throw ProtobufError("Failed to serialize protobuf message");
        }

        // Apply encoding rules if present
        auto rule_set = schema.getRuleSet();
        if (rule_set.has_value()) {
            // TODO: Implement encoding rule execution
        }
    } else {
        // Direct serialization without schema evolution
        if (!message.SerializeToString(reinterpret_cast<std::string*>(&encoded_bytes))) {
            throw ProtobufError("Failed to serialize protobuf message");
        }
    }

    // Set message indexes for nested messages
    schema_id.setMessageIndexes(toIndexArray(descriptor));

    // Serialize schema ID with message
    auto id_serializer = base_->getConfig().schema_id_serializer;
    return id_serializer(encoded_bytes, ctx, schema_id);
}

std::vector<int32_t> ProtobufSerializer::toIndexArray(const google::protobuf::Descriptor* descriptor) {
    std::vector<int32_t> indexes;

    // Build index path from file descriptor to this message type
    const google::protobuf::FileDescriptor* file = descriptor->file();

    // Find the message type index within the file
    for (int i = 0; i < file->message_type_count(); ++i) {
        if (file->message_type(i) == descriptor) {
            indexes.push_back(i);
            break;
        }
    }

    return indexes;
}

void ProtobufSerializer::validateSchema(const srclient::rest::model::Schema& schema) {
    auto schema_str = schema.getSchema();
    if (!schema_str.has_value() || schema_str->empty()) {
        throw ProtobufError("Schema content is empty");
    }

    auto schema_type = schema.getSchemaType();
    if (schema_type.has_value() && schema_type.value() != "PROTOBUF") {
        throw ProtobufError("Schema type must be PROTOBUF");
    }
}

} // namespace srclient::serdes::protobuf