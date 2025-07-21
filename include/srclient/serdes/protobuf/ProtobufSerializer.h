#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>

#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeConfig.h"
#include "srclient/serdes/protobuf/ProtobufUtils.h"
#include "srclient/rest/ISchemaRegistryClient.h"

namespace srclient::serdes::protobuf {

// Forward declarations
class ProtobufSerializer;

class ProtobufSerde;

/**
 * Reference subject name strategy function type
 * Converts reference name and serde type to subject name
 */
using ReferenceSubjectNameStrategy = std::function<std::string(const std::string& ref_name, SerdeType serde_type)>;

/**
 * Default reference subject name strategy implementation
 */
std::string defaultReferenceSubjectNameStrategy(const std::string& ref_name, SerdeType serde_type);

/**
 * Protobuf schema caching and parsing class
 * Based on ProtobufSerde struct from protobuf.rs (converted to synchronous)
 */
class ProtobufSerde {
public:
    ProtobufSerde();
    ~ProtobufSerde() = default;

    // Schema parsing and caching
    std::pair<const google::protobuf::FileDescriptor*, const google::protobuf::DescriptorPool*> 
    getParsedSchema(const srclient::rest::model::Schema& schema, 
                   std::shared_ptr<srclient::rest::ISchemaRegistryClient> client);

    // Clear cache
    void clear();

private:
    // Cache for parsed schemas: Schema -> (FileDescriptor*, DescriptorPool)
    std::unordered_map<std::string, std::pair<
        const google::protobuf::FileDescriptor*,
        std::unique_ptr<google::protobuf::DescriptorPool>
    >> parsed_schemas_cache_;
    
    mutable std::mutex cache_mutex_;
    
    // Helper methods
    void resolveNamedSchema(const srclient::rest::model::Schema& schema,
                           std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
                           google::protobuf::DescriptorPool* pool,
                           std::unordered_set<std::string>& visited);
};

/**
 * Protobuf serializer class template
 * Based on ProtobufSerializer from protobuf.rs (converted to synchronous)
 */
class ProtobufSerializer {
public:
    /**
     * Constructor with default reference subject name strategy
     */
    ProtobufSerializer(std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
                      std::optional<srclient::rest::model::Schema> schema,
                      std::shared_ptr<RuleRegistry> rule_registry,
                      const SerializerConfig& config);

    /**
     * Constructor with custom reference subject name strategy
     */
    ProtobufSerializer(std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
                      std::optional<srclient::rest::model::Schema> schema,
                      std::shared_ptr<RuleRegistry> rule_registry,
                      const SerializerConfig& config,
                      ReferenceSubjectNameStrategy strategy);

    /**
     * Serialize a protobuf message
     */
    std::vector<uint8_t> serialize(const SerializationContext& ctx,
                                  const google::protobuf::Message& message);

    /**
     * Serialize with file descriptor set
     */
    std::vector<uint8_t> serializeWithFileDescriptorSet(const SerializationContext& ctx,
                                                        const google::protobuf::Message& message,
                                                        const std::string& message_type_name,
                                                        const google::protobuf::FileDescriptorSet& fds);

    /**
     * Serialize with message descriptor
     */
    std::vector<uint8_t> serializeWithMessageDescriptor(const SerializationContext& ctx,
                                                        const google::protobuf::Message& message,
                                                        const google::protobuf::Descriptor* descriptor);

private:
    std::optional<srclient::rest::model::Schema> schema_;
    std::shared_ptr<BaseSerializer> base_;
    std::unique_ptr<ProtobufSerde> serde_;
    ReferenceSubjectNameStrategy reference_subject_name_strategy_;

    // Helper methods
    std::vector<int32_t> toIndexArray(const google::protobuf::Descriptor* descriptor);
    
    std::vector<srclient::rest::model::SchemaReference> resolveDependencies(
        const SerializationContext& ctx,
        const google::protobuf::FileDescriptor* file_desc
    );
    
    void validateSchema(const srclient::rest::model::Schema& schema);
};

} // namespace srclient::serdes::protobuf