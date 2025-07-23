#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>

#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "srclient/rest/ISchemaRegistryClient.h"
#include "srclient/rest/model/Schema.h"
#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"

namespace srclient::serdes::protobuf::utils {

/**
 * Convert a FileDescriptor to base64-encoded string
 */
std::string schemaToString(const google::protobuf::FileDescriptor *file_desc);

/**
 * Parse base64-encoded schema string to FileDescriptor
 */
const google::protobuf::FileDescriptor *stringToSchema(
    google::protobuf::DescriptorPool *pool, const std::string &name,
    const std::string &schema_string);

/**
 * Decode FileDescriptorProto with proper name handling
 */
void decodeFileDescriptorProtoWithName(google::protobuf::DescriptorPool *pool,
                                       const std::string &name,
                                       const std::vector<uint8_t> &bytes);

/**
 * Check if a proto file name is a built-in type
 */
bool isBuiltin(const std::string &name);

/**
 * Get field type from FieldDescriptor
 */
FieldType getFieldType(const google::protobuf::FieldDescriptor *field_desc);

/**
 * Extract inline tags from field options (confluent.field_meta)
 */
std::unordered_set<std::string> getInlineTags(
    const google::protobuf::FieldDescriptor *field_desc);

/**
 * Transform protobuf fields using field execution context (synchronous version)
 * Ported from Rust async implementation
 */
std::unique_ptr<SerdeValue> transformFields(
    RuleContext &ctx, const std::string &field_executor_type,
    const SerdeValue &value);

/**
 * Protobuf Message to JSON conversion
 */
nlohmann::json messageToJson(const google::protobuf::Message &message);

/**
 * JSON to Protobuf Message conversion
 */
std::unique_ptr<google::protobuf::Message> jsonToMessage(
    const nlohmann::json &json, const google::protobuf::Descriptor *descriptor,
    const google::protobuf::DescriptorPool *pool);

/**
 * Copy compatible fields between protobuf messages
 */
void copyCompatibleFields(const google::protobuf::Message &source,
                          google::protobuf::Message &target);

/**
 * Get message descriptor from pool using message index array
 */
const google::protobuf::Descriptor *getMessageDescriptorByIndex(
    const google::protobuf::DescriptorPool *pool,
    const google::protobuf::FileDescriptor *file_desc,
    const std::vector<int32_t> &msg_index);

/**
 * Create message index array for nested message types
 */
std::vector<int32_t> createMessageIndexArray(
    const google::protobuf::Descriptor *descriptor);

/**
 * Schema resolution utilities
 */
namespace schema_resolution {

/**
 * Resolve named schemas recursively
 */
void resolveNamedSchema(
    const srclient::rest::model::Schema &schema,
    std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
    google::protobuf::DescriptorPool *pool,
    std::unordered_set<std::string> &visited);

/**
 * Build dependencies list for a FileDescriptor
 */
std::vector<srclient::rest::model::SchemaReference> buildDependencies(
    const google::protobuf::FileDescriptor *file_desc,
    const std::function<std::string(const std::string &, SerdeType)>
        &reference_strategy,
    SerdeType serde_type);
}  // namespace schema_resolution

/**
 * Message descriptor navigation utilities
 */
namespace descriptor_navigation {

/**
 * Navigate to nested message descriptor using path
 */
std::pair<std::string, const google::protobuf::DescriptorProto *>
getMessageDescriptorProtoFile(const std::string &path,
                              const google::protobuf::FileDescriptorProto *desc,
                              const std::vector<int32_t> &msg_index);

/**
 * Navigate nested message descriptors
 */
std::pair<std::string, const google::protobuf::DescriptorProto *>
getMessageDescriptorProtoNested(const std::string &path,
                                const google::protobuf::DescriptorProto *desc,
                                const std::vector<int32_t> &msg_index);
}  // namespace descriptor_navigation

/**
 * Stream utilities for protobuf serialization
 */
namespace stream_utils {

/**
 * Serialize message to byte vector
 */
std::vector<uint8_t> serializeMessage(const google::protobuf::Message &message);

/**
 * Parse message from byte vector
 */
bool parseMessage(google::protobuf::Message &message,
                  const std::vector<uint8_t> &data);

/**
 * Parse message from byte array
 */
bool parseMessage(google::protobuf::Message &message, const uint8_t *data,
                  size_t size);
}  // namespace stream_utils

}  // namespace srclient::serdes::protobuf::utils