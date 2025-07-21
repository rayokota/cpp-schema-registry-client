#include "srclient/serdes/protobuf/ProtobufSerializer.h"
#include "srclient/serdes/protobuf/ProtobufUtils.h"
#include "confluent/type/decimal.pb.h"
#include "confluent/meta.pb.h"
#include <google/protobuf/any.pb.h>
#include <google/protobuf/api.pb.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/empty.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <google/protobuf/source_context.pb.h>
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/type.pb.h>
#include <google/protobuf/wrappers.pb.h>

// Forward declaration for transformFields function from ProtobufUtils.cpp
namespace srclient::serdes::protobuf::utils {
    std::unique_ptr<srclient::serdes::SerdeValue> transformFields(
        srclient::serdes::RuleContext& ctx,
        const std::string& field_executor_type,
        const srclient::serdes::SerdeValue& value
    );
}

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
        return {it->second.first, it->second.second.get()};
    }
    
    // Parse new schema
    auto pool = std::make_unique<google::protobuf::DescriptorPool>();
    initPool(pool.get());
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
    
    // Store in cache with raw FileDescriptor pointer (owned by pool)
    parsed_schemas_cache_[cache_key] = std::make_pair(
        file_desc,
        std::move(pool)
    );
    
    return {file_desc, parsed_schemas_cache_[cache_key].second.get()};
}

void ProtobufSerde::addFileToPool(google::protobuf::DescriptorPool* pool, const google::protobuf::FileDescriptor* file_descriptor) {
    google::protobuf::FileDescriptorProto file_descriptor_proto;
    file_descriptor->CopyTo(&file_descriptor_proto);
    pool->BuildFile(file_descriptor_proto);
}

void ProtobufSerde::initPool(google::protobuf::DescriptorPool* pool) {
    // Add Google's well-known types to the descriptor pool using BuildFile
    addFileToPool(pool, google::protobuf::Any::descriptor()->file());
    // Source_context needed by api
    addFileToPool(pool, google::protobuf::SourceContext::descriptor()->file());
    // Type needed by api
    addFileToPool(pool, google::protobuf::Type::descriptor()->file());
    addFileToPool(pool, google::protobuf::Api::descriptor()->file());
    addFileToPool(pool, google::protobuf::DescriptorProto::descriptor()->file());
    addFileToPool(pool, google::protobuf::Duration::descriptor()->file());
    addFileToPool(pool, google::protobuf::Empty::descriptor()->file());
    addFileToPool(pool, google::protobuf::FieldMask::descriptor()->file());
    addFileToPool(pool, google::protobuf::Struct::descriptor()->file());
    addFileToPool(pool, google::protobuf::Timestamp::descriptor()->file());
    
    // Add wrapper types
    addFileToPool(pool, google::protobuf::DoubleValue::descriptor()->file());
    addFileToPool(pool, google::protobuf::FloatValue::descriptor()->file());
    addFileToPool(pool, google::protobuf::Int64Value::descriptor()->file());
    addFileToPool(pool, google::protobuf::UInt64Value::descriptor()->file());
    addFileToPool(pool, google::protobuf::Int32Value::descriptor()->file());
    addFileToPool(pool, google::protobuf::UInt32Value::descriptor()->file());
    addFileToPool(pool, google::protobuf::BoolValue::descriptor()->file());
    addFileToPool(pool, google::protobuf::StringValue::descriptor()->file());
    addFileToPool(pool, google::protobuf::BytesValue::descriptor()->file());

    addFileToPool(pool, confluent::Meta::descriptor()->file());
    addFileToPool(pool, confluent::type::Decimal::descriptor()->file());
}

void ProtobufSerde::clear() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    parsed_schemas_cache_.clear();
}

void ProtobufSerde::resolveNamedSchema(const srclient::rest::model::Schema& schema,
                                      std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
                                      google::protobuf::DescriptorPool* pool,
                                      std::unordered_set<std::string>& visited) {
    // Implement dependency resolution
    // This recursively resolves schema references
    auto references = schema.getReferences();
    if (references.has_value()) {
        for (const auto& ref : references.value()) {
            auto name = ref.getName().value_or("");
            if (isBuiltin(name) || visited.find(name) != visited.end()) {
                continue;
            }
            visited.insert(name);
            
            auto subject = ref.getSubject().value_or("");
            auto version = ref.getVersion().value_or(-1);
            
            try {
                auto ref_schema = client->getVersion(subject, version, true, "serialized");
                auto schema_obj = ref_schema.toSchema();
                resolveNamedSchema(schema_obj, client, pool, visited);
                auto schema_content = ref_schema.getSchema().value_or("");
                stringToSchema(pool, name, schema_content);
            } catch (const std::exception& e) {
                throw ProtobufError("Failed to resolve schema reference: " + name + " - " + e.what());
            }
        }
    }
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
                    executor->configure(client->getConfiguration(), config.rule_config);
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
                    executor->configure(client->getConfiguration(), config.rule_config);
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
        latest_schema = base_->getSerde().getReaderSchema(subject, "serialized", base_->getConfig().use_schema);
    } catch (const std::exception& e) {
        // Schema not found - will use provided schema
    }

    if (latest_schema.has_value()) {
        // Use latest schema from registry
        schema_id = SchemaId(SerdeFormat::Avro, latest_schema->getId(), latest_schema->getGuid(), std::nullopt);

        auto schema = latest_schema->toSchema();
        auto [fd, pool] = serde_->getParsedSchema(schema, base_->getSerde().getClient());
        
        // Create field transformer function
        auto field_transformer = [&](RuleContext& ctx, const std::string& rule_type, const SerdeValue& value) -> std::unique_ptr<SerdeValue> {
            return utils::transformFields(ctx, rule_type, value);
        };

        // Create DynamicMessage from the message descriptor
        auto message_factory = google::protobuf::DynamicMessageFactory();
        auto dynamic_msg = std::unique_ptr<google::protobuf::Message>(
            message_factory.GetPrototype(descriptor)->New()
        );
        
        // Copy data from input message to dynamic message
        dynamic_msg->CopyFrom(message);
        
        // Create SerdeValue for the protobuf message
        auto protobuf_value = protobuf::makeProtobufValue(*dynamic_msg);
        
        // Create SerdeSchema for the protobuf file descriptor
        auto protobuf_schema = protobuf::makeProtobufSchema(fd);
        
        // Execute rules synchronously
        auto serde_value = base_->getSerde().executeRules(
            ctx,
            subject,
            Mode::Write,
            std::nullopt,
            std::make_optional(schema),
            std::make_optional(protobuf_schema.get()),
            *protobuf_value,
            {},
            std::make_shared<FieldTransformer>(field_transformer)
        );
        
        // Extract the transformed message
        if (!serde_value->isProtobuf()) {
            throw ProtobufError("Unexpected serde value type after rule execution");
        }
        
        auto transformed_message = std::any_cast<google::protobuf::Message*>(serde_value->getValue());
        
        // Encode the transformed message
        if (!transformed_message->SerializeToArray(encoded_bytes.data(), encoded_bytes.size())) {
            encoded_bytes.resize(transformed_message->ByteSizeLong());
            if (!transformed_message->SerializeToArray(encoded_bytes.data(), encoded_bytes.size())) {
                throw ProtobufError("Failed to serialize protobuf message");
            }
        } else {
            encoded_bytes.resize(transformed_message->ByteSizeLong());
        }
    } else {
        // Resolve dependencies for the descriptor's file
        std::vector<srclient::rest::model::SchemaReference> references = resolveDependencies(ctx, descriptor->file());
        
        // Create schema object
        srclient::rest::model::Schema schema;
        schema.setSchemaType("PROTOBUF");
        schema.setReferences(references);
        // Convert file descriptor to protobuf schema string
        schema.setSchema(utils::schemaToString(descriptor->file()));
        
        if (base_->getConfig().auto_register_schemas) {
            auto registered_schema = base_->getSerde().getClient()->registerSchema(
                subject, 
                schema, 
                base_->getConfig().normalize_schemas
            );
            schema_id = SchemaId(SerdeFormat::Protobuf, registered_schema.getId(), 
                               registered_schema.getGuid(), std::nullopt);
        } else {
            auto registered_schema = base_->getSerde().getClient()->getBySchema(
                subject, 
                schema, 
                base_->getConfig().normalize_schemas, 
                false
            );
            schema_id = SchemaId(SerdeFormat::Protobuf, registered_schema.getId(), 
                               registered_schema.getGuid(), std::nullopt);
        }

        // Serialize the message directly
        encoded_bytes.resize(message.ByteSizeLong());
        if (!message.SerializeToArray(encoded_bytes.data(), encoded_bytes.size())) {
            throw ProtobufError("Failed to serialize protobuf message");
        }
    }

    // Apply encoding rules if present
    if (latest_schema.has_value()) {
        auto schema = latest_schema->toSchema();
        if (schema.getRuleSet().has_value()) {
            auto rule_set = schema.getRuleSet().value();
            if (rule_set.getEncodingRules().has_value()) {
                auto bytes_value = SerdeValue::newBytes(SerdeFormat::Protobuf, encoded_bytes);
                auto result = base_->getSerde().executeRulesWithPhase(
                        ctx,
                        subject,
                        Phase::Encoding,
                        Mode::Write,
                        std::nullopt,
                        std::make_optional(schema),
                        std::nullopt,
                        *bytes_value,
                        {}
                );
                encoded_bytes = std::any_cast<std::vector<uint8_t>>(result->getValue());
            }
        }
    }

    // Set message indexes for nested messages
    schema_id.setMessageIndexes(toIndexArray(descriptor));

    // Serialize schema ID with message
    auto id_serializer = base_->getConfig().schema_id_serializer;
    return id_serializer(encoded_bytes, ctx, schema_id);
}

std::vector<srclient::rest::model::SchemaReference> ProtobufSerializer::resolveDependencies(
        const SerializationContext& ctx,
        const google::protobuf::FileDescriptor* file_desc
) {
    std::vector<srclient::rest::model::SchemaReference> references;
    
    for (int i = 0; i < file_desc->dependency_count(); ++i) {
        const google::protobuf::FileDescriptor* dep = file_desc->dependency(i);
        if (isBuiltin(dep->name())) {
            continue;
        }
        
        auto dep_refs = resolveDependencies(ctx, dep);
        auto subject = reference_subject_name_strategy_(dep->name(), ctx.serde_type);
        
        srclient::rest::model::Schema schema;
        schema.setSchemaType("PROTOBUF");
        schema.setReferences(dep_refs);
        schema.setSchema(utils::schemaToString(dep));
        
        if (base_->getConfig().auto_register_schemas) {
            base_->getSerde().getClient()->registerSchema(
                subject, 
                schema, 
                base_->getConfig().normalize_schemas
            );
        }
        
        auto reference = base_->getSerde().getClient()->getBySchema(
            subject, 
            schema, 
            base_->getConfig().normalize_schemas, 
            false
        );
        
        srclient::rest::model::SchemaReference schema_ref;
        schema_ref.setName(dep->name());
        schema_ref.setSubject(subject);
        schema_ref.setVersion(reference.getVersion());
        
        references.push_back(schema_ref);
    }
    
    return references;
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