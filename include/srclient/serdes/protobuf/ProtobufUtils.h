#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <nlohmann/json.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/json_util.h>

#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/rest/ISchemaRegistryClient.h"
#include "srclient/rest/model/Schema.h"
#include "srclient/serdes/Serde.h"

namespace srclient::serdes::protobuf::utils {



/**
 * Convert a FileDescriptor to base64-encoded string
 */
std::string schemaToString(const google::protobuf::FileDescriptor* file_desc);

/**
 * Parse base64-encoded schema string to FileDescriptor
 */
const google::protobuf::FileDescriptor* stringToSchema(
    google::protobuf::DescriptorPool* pool,
    const std::string& name,
    const std::string& schema_string);

/**
 * Decode FileDescriptorProto with proper name handling
 */
void decodeFileDescriptorProtoWithName(
    google::protobuf::DescriptorPool* pool,
    const std::string& name,
    const std::vector<uint8_t>& bytes);

/**
 * Check if a proto file name is a built-in type
 */
bool isBuiltin(const std::string& name);

/**
 * Get field type from FieldDescriptor
 */
FieldType getFieldType(const google::protobuf::FieldDescriptor* field_desc);

/**
 * Extract inline tags from field options (confluent.field_meta)
 */
std::unordered_set<std::string> getInlineTags(const google::protobuf::FieldDescriptor* field_desc);

/**
 * Transform protobuf message fields based on rules
 */
google::protobuf::Message* transformMessage(
    RuleContext& ctx,
    const google::protobuf::Descriptor* descriptor,
    google::protobuf::Message* message,
    const std::string& field_executor_type);

/**
 * Transform a single protobuf field
 */
bool transformFieldWithContext(
    RuleContext& ctx,
    const google::protobuf::FieldDescriptor* field_desc,
    const google::protobuf::Descriptor* msg_desc,
    google::protobuf::Message* message,
    const std::string& field_executor_type);

/**
 * Protobuf Message to JSON conversion
 */
nlohmann::json messageToJson(const google::protobuf::Message& message);

/**
 * JSON to Protobuf Message conversion
 */
std::unique_ptr<google::protobuf::Message> jsonToMessage(
    const nlohmann::json& json,
    const google::protobuf::Descriptor* descriptor,
    const google::protobuf::DescriptorPool* pool);

/**
 * Copy compatible fields between protobuf messages
 */
void copyCompatibleFields(
    const google::protobuf::Message& source,
    google::protobuf::Message& target);

/**
 * Get message descriptor from pool using message index array
 */
const google::protobuf::Descriptor* getMessageDescriptorByIndex(
    const google::protobuf::DescriptorPool* pool,
    const google::protobuf::FileDescriptor* file_desc,
    const std::vector<int32_t>& msg_index);

/**
 * Create message index array for nested message types
 */
std::vector<int32_t> createMessageIndexArray(const google::protobuf::Descriptor* descriptor);

/**
 * Protobuf value transformation helpers
 */
namespace value_transform {
    
    /**
     * Transform a protobuf field value based on field type
     */
    bool transformFieldValue(
        const google::protobuf::FieldDescriptor* field_desc,
        google::protobuf::Message* message,
        const SerdeValue& transformed_value);
    
    /**
     * Extract SerdeValue from protobuf field
     */
    std::unique_ptr<SerdeValue> extractFieldValue(
        const google::protobuf::FieldDescriptor* field_desc,
        const google::protobuf::Message& message);

    /**
     * Handle repeated field transformations
     */
    bool transformRepeatedField(
        const google::protobuf::FieldDescriptor* field_desc,
        google::protobuf::Message* message,
        const std::vector<std::reference_wrapper<const SerdeValue>>& values);

    /**
     * Handle map field transformations
     */
    bool transformMapField(
        const google::protobuf::FieldDescriptor* field_desc,
        google::protobuf::Message* message,
        const std::unordered_map<std::string, std::reference_wrapper<const SerdeValue>>& map_values);
}

/**
 * Schema resolution utilities
 */
namespace schema_resolution {
    
    /**
     * Resolve named schemas recursively
     */
    void resolveNamedSchema(
        const srclient::rest::model::Schema& schema,
        std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
        google::protobuf::DescriptorPool* pool,
        std::unordered_set<std::string>& visited);
    
    /**
     * Build dependencies list for a FileDescriptor
     */
    std::vector<srclient::rest::model::SchemaReference> buildDependencies(
        const google::protobuf::FileDescriptor* file_desc,
        const std::function<std::string(const std::string&, SerdeType)>& reference_strategy,
        SerdeType serde_type);
}

/**
 * Message descriptor navigation utilities
 */
namespace descriptor_navigation {
    
    /**
     * Navigate to nested message descriptor using path
     */
    std::pair<std::string, const google::protobuf::DescriptorProto*> 
    getMessageDescriptorProtoFile(
        const std::string& path,
        const google::protobuf::FileDescriptorProto* desc,
        const std::vector<int32_t>& msg_index);
    
    /**
     * Navigate nested message descriptors
     */
    std::pair<std::string, const google::protobuf::DescriptorProto*> 
    getMessageDescriptorProtoNested(
        const std::string& path,
        const google::protobuf::DescriptorProto* desc,
        const std::vector<int32_t>& msg_index);
}

/**
 * Stream utilities for protobuf serialization
 */
namespace stream_utils {
    
    /**
     * Serialize message to byte vector
     */
    std::vector<uint8_t> serializeMessage(const google::protobuf::Message& message);
    
    /**
     * Parse message from byte vector
     */
    bool parseMessage(google::protobuf::Message& message, const std::vector<uint8_t>& data);
    
    /**
     * Parse message from byte array
     */
    bool parseMessage(google::protobuf::Message& message, const uint8_t* data, size_t size);
}

} // namespace srclient::serdes::protobuf::utils 