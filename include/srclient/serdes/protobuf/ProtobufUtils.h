#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>

#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "srclient/rest/ISchemaRegistryClient.h"
#include "srclient/rest/model/Schema.h"
#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"

namespace srclient::serdes::protobuf::utils {

// Forward declaration
struct ProtobufVariantValue;

// C++ variant representing protobuf values - type alias for map keys
using MapKey = std::variant<std::string, int32_t, int64_t, uint32_t, uint64_t, bool>;

/**
 * C++ variant-based structure representing protobuf values
 * Ported from Rust Value enum to use std::variant
 */
struct ProtobufVariantValue {
    enum class ValueType {
        Bool, I32, I64, U32, U64, F32, F64, String, Bytes, EnumNumber, Message, List, Map
    };
    
    using ValueVariant = std::variant<
        bool,                                                    // Bool
        int32_t,                                                // I32
        int64_t,                                                // I64
        uint32_t,                                               // U32
        uint64_t,                                               // U64
        float,                                                  // F32
        double,                                                 // F64
        std::string,                                            // String
        std::vector<uint8_t>,                                   // Bytes
        std::unique_ptr<google::protobuf::Message>,             // Message
        std::vector<ProtobufVariantValue>,                             // List
        std::map<MapKey, ProtobufVariantValue>                         // Map
    >;
    
    ValueVariant value;
    ValueType type_;
    
    // Constructors for each type
    ProtobufVariantValue(bool v) : value(v), type_(ValueType::Bool) {}
    ProtobufVariantValue(int32_t v, ValueType type = ValueType::I32) : value(v), type_(type) {}
    ProtobufVariantValue(int64_t v) : value(v), type_(ValueType::I64) {}
    ProtobufVariantValue(uint32_t v) : value(v), type_(ValueType::U32) {}
    ProtobufVariantValue(uint64_t v) : value(v), type_(ValueType::U64) {}
    ProtobufVariantValue(float v) : value(v), type_(ValueType::F32) {}
    ProtobufVariantValue(double v) : value(v), type_(ValueType::F64) {}
    ProtobufVariantValue(const std::string& v) : value(v), type_(ValueType::String) {}
    ProtobufVariantValue(const std::vector<uint8_t>& v) : value(v), type_(ValueType::Bytes) {}
    ProtobufVariantValue(std::unique_ptr<google::protobuf::Message> v) : value(std::move(v)), type_(ValueType::Message) {}
    ProtobufVariantValue(const std::vector<ProtobufVariantValue>& v) : value(v), type_(ValueType::List) {}
    ProtobufVariantValue(const std::map<MapKey, ProtobufVariantValue>& v) : value(v), type_(ValueType::Map) {}
    
    // Special constructor for enum values
    static ProtobufVariantValue createEnum(int32_t value) {
        return ProtobufVariantValue(value, ValueType::EnumNumber);
    }
    
    // Copy constructor and assignment operator
    ProtobufVariantValue(const ProtobufVariantValue& other);
    ProtobufVariantValue& operator=(const ProtobufVariantValue& other);
    
    // Move constructor and assignment operator
    ProtobufVariantValue(ProtobufVariantValue&& other) = default;
    ProtobufVariantValue& operator=(ProtobufVariantValue&& other) = default;
    
    // Visitor helper methods
    template<typename T>
    bool is() const { return std::holds_alternative<T>(value); }
    
    template<typename T>
    const T& get() const { return std::get<T>(value); }
    
    template<typename T>
    T& get() { return std::get<T>(value); }
};

/**
 * Transform protobuf fields using field execution context (synchronous version)
 * Ported from Rust async implementation
 */
std::unique_ptr<SerdeValue> transformFields(
    RuleContext &ctx, const std::string &field_executor_type,
    const google::protobuf::Descriptor *descriptor,
    const SerdeValue &value);

/**
 * Transform protobuf values recursively (synchronous version)
 * Ported from Rust async implementation
 */
ProtobufVariantValue transformRecursive(
    RuleContext& ctx,
    const google::protobuf::Descriptor* descriptor,
    const ProtobufVariantValue& message,
    const std::string& field_executor_type);

/**
 * Transform field with rule context (synchronous version)
 * Ported from Rust async implementation
 */
std::optional<ProtobufVariantValue> transformFieldWithContext(
    RuleContext& ctx,
    const google::protobuf::FieldDescriptor* fd,
    const google::protobuf::Descriptor* desc,
    const google::protobuf::Message* message,
    const std::string& field_executor_type);

/**
 * Extract field value from protobuf message
 */
ProtobufVariantValue getMessageFieldValue(const google::protobuf::Message* message, 
                                   const google::protobuf::FieldDescriptor* fd);

/**
 * Set field value in protobuf message
 */
void setMessageField(google::protobuf::Message* message, 
                     const google::protobuf::FieldDescriptor* fd, 
                     const ProtobufVariantValue& value);

/**
 * Convert ProtobufVariantValue to SerdeValue
 */
std::unique_ptr<SerdeValue> convertVariantToSerdeValue(const ProtobufVariantValue& variant);

/**
 * Convert SerdeValue back to ProtobufVariantValue
 */
ProtobufVariantValue convertSerdeValueToProtobufValue(const SerdeValue& serde_value);

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