#include "srclient/serdes/protobuf/ProtobufUtils.h"
#include "srclient/serdes/RuleRegistry.h" // For global_registry functions
#include "absl/strings/escaping.h"
#include "confluent/meta.pb.h"
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <memory> // For std::dynamic_pointer_cast

namespace srclient::serdes::protobuf::utils {

// Base64 encoding/decoding utilities using absl
namespace {
    std::string base64_encode(const std::vector<uint8_t>& bytes) {
        std::string input(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        return absl::Base64Escape(input);
    }

    std::vector<uint8_t> base64_decode(const std::string& encoded_string) {
        std::string decoded;
        if (!absl::Base64Unescape(encoded_string, &decoded)) {
            // Return empty vector on decode failure
            return std::vector<uint8_t>();
        }
        return std::vector<uint8_t>(decoded.begin(), decoded.end());
    }
}

std::unique_ptr<SerdeValue> transformFields(
    RuleContext& ctx,
    const std::string& field_executor_type,
    const SerdeValue& value
) {
    // Check if we have a protobuf schema and value
    const auto& parsed_target = ctx.getParsedTarget();
    if (parsed_target.has_value() && parsed_target->get() && parsed_target->get()->isProtobuf()) {
        if (value.isProtobuf()) {
            // Extract the protobuf message from the SerdeValue
            auto message_ptr = std::any_cast<google::protobuf::Message*>(value.getValue());
            if (message_ptr) {
                const google::protobuf::Descriptor* descriptor = message_ptr->GetDescriptor();
                if (!descriptor) {
                    throw ProtobufError("Message descriptor not found for " + message_ptr->GetTypeName());
                }
                
                // Transform the message using the synchronous method
                auto transformed_message = transformMessage(ctx, descriptor, message_ptr, field_executor_type);
                return protobuf::makeProtobufValue(*transformed_message);
            }
        }
    }
    return value.clone();
}

google::protobuf::Message* transformMessage(
    RuleContext& ctx,
    const google::protobuf::Descriptor* descriptor,
    google::protobuf::Message* message,
    const std::string& field_executor_type
) {
    if (!message || !descriptor) {
        return message;
    }
    
    // For message types, process each field
    const google::protobuf::Reflection* reflection = message->GetReflection();
    if (!reflection) {
        return message;
    }
    
    // Iterate through all fields in the message
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const google::protobuf::FieldDescriptor* field_desc = descriptor->field(i);
        
        // Transform the field with context
        bool field_was_transformed = transformFieldWithContext(
            ctx, field_desc, descriptor, message, field_executor_type
        );
        
        // Continue processing even if individual field transformations fail
        // to allow partial processing and better error handling
    }
    
    return message;
}

bool transformFieldWithContext(
    RuleContext& ctx,
    const google::protobuf::FieldDescriptor* field_desc,
    const google::protobuf::Descriptor* msg_desc,
    google::protobuf::Message* message,
    const std::string& field_executor_type
) {
    if (!field_desc || !msg_desc || !message) {
        return false;
    }
    
    const google::protobuf::Reflection* reflection = message->GetReflection();
    if (!reflection) {
        return false;
    }
    
    // Create message value for field context
    auto message_value = protobuf::makeProtobufValue(*message);
    
    // Get field type and inline tags
    FieldType field_type = getFieldType(field_desc);
    std::unordered_set<std::string> inline_tags = getInlineTags(field_desc);
    
    // Create full field name
    std::string full_name = msg_desc->full_name() + "." + field_desc->name();
    
    // Enter field context
    ctx.enterField(*message_value, full_name, field_desc->name(), field_type, inline_tags);
    
    try {
        // Check if this is a oneof field that's not set
        if (field_desc->containing_oneof() && !reflection->HasField(*message, field_desc)) {
            ctx.exitField();
            return false; // Skip oneof fields that are not set
        }
        
        // Check if field has a value to transform
        if (!reflection->HasField(*message, field_desc) && !field_desc->is_repeated()) {
            ctx.exitField();
            return false;
        }
        
        // Apply field-level transformation logic
        auto field_ctx = ctx.currentField();
        if (field_ctx.has_value()) {
            auto rule_tags = ctx.getRule().getTags();
            std::unordered_set<std::string> rule_tags_set;
            if (rule_tags.has_value()) {
                rule_tags_set = std::unordered_set<std::string>(rule_tags->begin(), rule_tags->end());
            }
            
            // Check if rule tags overlap with field context tags (empty rule_tags means apply to all)
            bool should_apply = !rule_tags.has_value() || rule_tags_set.empty();
            if (!should_apply) {
                const auto& field_tags = field_ctx->getTags();
                for (const auto& field_tag : field_tags) {
                    if (rule_tags_set.find(field_tag) != rule_tags_set.end()) {
                        should_apply = true;
                        break;
                    }
                }
            }
            
            if (should_apply) {
                // Get field executor from rule registry
                auto rule_registry = ctx.getRuleRegistry();
                std::shared_ptr<RuleExecutor> executor;
                if (rule_registry) {
                    executor = rule_registry->getExecutor(field_executor_type);
                } else {
                    executor = global_registry::getRuleExecutor(field_executor_type);
                }
                
                if (executor) {
                    // Try to cast to field rule executor
                    auto field_executor = std::dynamic_pointer_cast<FieldRuleExecutor>(executor);
                    if (field_executor) {
                        // Extract field value
                        auto field_value = value_transform::extractFieldValue(field_desc, *message);
                        if (field_value) {
                            // Apply transformation
                            auto transformed_value = field_executor->transformField(ctx, *field_value);
                            if (transformed_value) {
                                // Apply the transformed value back to the field
                                value_transform::transformFieldValue(field_desc, message, *transformed_value);
                            }
                        }
                    }
                }
            }
        }
        
        // Check for condition rules
        auto rule_kind = ctx.getRule().getKind();
        if (rule_kind.has_value() && rule_kind.value() == Kind::Condition) {
            // For condition rules, we'd need to check if the result is boolean and handle accordingly
            // This is a simplified implementation - in practice might need more complex logic
        }
        
        ctx.exitField();
        return true;
        
    } catch (const std::exception& e) {
        ctx.exitField();
        throw; // Re-throw the exception
    }
}

std::unordered_set<std::string> getInlineTags(const google::protobuf::FieldDescriptor* field_desc) {
    std::unordered_set<std::string> tag_set;
    
    if (!field_desc) {
        return tag_set;
    }
    
    // Try to get the confluent.field_meta extension from the field options
    const google::protobuf::FieldOptions& options = field_desc->options();

    if (options.HasExtension(confluent::field_meta)) {
        auto ext = options.GetExtension(confluent::field_meta);
        const auto& tags = ext.tags();  // Call tags() method to get RepeatedPtrField
        for (const auto& tag : tags) {
            tag_set.insert(tag);
        }
    }
    
    return tag_set;
}

// Value transformation implementations
namespace value_transform {

std::unique_ptr<SerdeValue> extractFieldValue(
    const google::protobuf::FieldDescriptor* field_desc,
    const google::protobuf::Message& message) {
    
    if (!field_desc) {
        throw ProtobufError("Field descriptor is null");
    }
    
    const google::protobuf::Reflection* reflection = message.GetReflection();
    if (!reflection) {
        throw ProtobufError("Message reflection is null");
    }
    
    // For repeated fields, we'd need to handle them differently
    if (field_desc->is_repeated()) {
        // TODO: Implement repeated field handling
        throw ProtobufError("Repeated field extraction not yet implemented");
    }
    
    // Check if field is set
    if (!reflection->HasField(message, field_desc)) {
        // Return default value or null based on field type
        switch (field_desc->type()) {
            case google::protobuf::FieldDescriptor::TYPE_STRING:
                return SerdeValue::newString(SerdeFormat::Protobuf, "");
            case google::protobuf::FieldDescriptor::TYPE_BYTES:
                return SerdeValue::newBytes(SerdeFormat::Protobuf, {});
            default:
                // For other types, would need more complex default value handling
                throw ProtobufError("Field not set and default value handling not implemented for type: " + std::to_string(field_desc->type()));
        }
    }
    
    // Extract the actual field value based on type
    switch (field_desc->type()) {
        case google::protobuf::FieldDescriptor::TYPE_STRING: {
            std::string value = reflection->GetString(message, field_desc);
            return SerdeValue::newString(SerdeFormat::Protobuf, value);
        }
        case google::protobuf::FieldDescriptor::TYPE_BYTES: {
            std::string value = reflection->GetString(message, field_desc);
            std::vector<uint8_t> bytes(value.begin(), value.end());
            return SerdeValue::newBytes(SerdeFormat::Protobuf, bytes);
        }
        case google::protobuf::FieldDescriptor::TYPE_MESSAGE: {
            const google::protobuf::Message& sub_message = reflection->GetMessage(message, field_desc);
            return protobuf::makeProtobufValue(const_cast<google::protobuf::Message&>(sub_message));
        }
        default:
            // TODO: Handle other field types (int32, int64, float, double, bool, enum)
            throw ProtobufError("Field type extraction not yet implemented for type: " + std::to_string(field_desc->type()));
    }
}

bool transformFieldValue(
    const google::protobuf::FieldDescriptor* field_desc,
    google::protobuf::Message* message,
    const SerdeValue& transformed_value) {
    
    if (!field_desc || !message) {
        return false;
    }
    
    const google::protobuf::Reflection* reflection = message->GetReflection();
    if (!reflection) {
        return false;
    }
    
    // Apply the transformed value back to the field based on field type
    switch (field_desc->type()) {
        case google::protobuf::FieldDescriptor::TYPE_STRING: {
            if (transformed_value.isProtobuf() || transformed_value.isJson()) {
                std::string value = transformed_value.asString();
                reflection->SetString(message, field_desc, value);
                return true;
            }
            break;
        }
        case google::protobuf::FieldDescriptor::TYPE_BYTES: {
            if (transformed_value.isProtobuf() || transformed_value.isJson()) {
                auto bytes = transformed_value.asBytes();
                std::string value(bytes.begin(), bytes.end());
                reflection->SetString(message, field_desc, value);
                return true;
            }
            break;
        }
        case google::protobuf::FieldDescriptor::TYPE_MESSAGE: {
            if (transformed_value.isProtobuf()) {
                // TODO: Handle message field transformation
                // This would require copying the transformed message to the target field
                return false;
            }
            break;
        }
        default:
            // TODO: Handle other field types
            return false;
    }
    
    return false;
}

} // namespace value_transform

std::string schemaToString(const google::protobuf::FileDescriptor* file_desc) {
    std::string serialized;
    google::protobuf::FileDescriptorProto proto;
    file_desc->CopyTo(&proto);
    if (!proto.SerializeToString(&serialized)) {
        throw ProtobufError("Failed to serialize FileDescriptor to string");
    }
    
    // Base64 encode the serialized data
    std::vector<uint8_t> bytes(serialized.begin(), serialized.end());
    return base64_encode(bytes);
}

const google::protobuf::FileDescriptor* stringToSchema(
    google::protobuf::DescriptorPool* pool,
    const std::string& name,
    const std::string& schema_string) {

    // Base64 decode
    std::vector<uint8_t> bytes = base64_decode(schema_string);
    
    decodeFileDescriptorProtoWithName(pool, name, bytes);
    
    const google::protobuf::FileDescriptor* file_desc = pool->FindFileByName(name);
    if (!file_desc) {
        throw ProtobufError("File descriptor not found after decoding: " + name);
    }
    
    return file_desc;
}

void decodeFileDescriptorProtoWithName(
    google::protobuf::DescriptorPool* pool,
    const std::string& name,
    const std::vector<uint8_t>& data) {
    
    google::protobuf::FileDescriptorProto proto;
    if (!proto.ParseFromArray(data.data(), data.size())) {
        throw ProtobufError("Failed to parse FileDescriptorProto from data");
    }
    
    proto.set_name(name);
    
    const google::protobuf::FileDescriptor* file_desc = pool->BuildFile(proto);
    if (!file_desc) {
        throw ProtobufError("Failed to build FileDescriptor from proto");
    }
}

bool isBuiltin(const std::string& name) {
    return name.find("confluent/") == 0
           || name.find("google/protobuf/") == 0
           || name.find("google/type/") == 0;
}

nlohmann::json messageToJson(const google::protobuf::Message& message) {
    std::string json_string;
    auto status = google::protobuf::util::MessageToJsonString(message, &json_string);
    if (!status.ok()) {
        throw ProtobufError("Failed to convert message to JSON: " + status.ToString());
    }
    return nlohmann::json::parse(json_string);
}

void jsonToMessage(const nlohmann::json& json_value, google::protobuf::Message* message) {
    std::string json_string = json_value.dump();
    google::protobuf::util::JsonParseOptions options;
    auto status = google::protobuf::util::JsonStringToMessage(json_string, message, options);
    if (!status.ok()) {
        throw ProtobufError("Failed to parse JSON to message: " + status.ToString());
    }
}

FieldType getFieldType(const google::protobuf::FieldDescriptor* field_desc) {
    // Check for map fields first (like the Rust version did)
    if (field_desc->is_map()) {
        return FieldType::Map;
    }
    
    switch (field_desc->type()) {
        case google::protobuf::FieldDescriptor::TYPE_STRING:
            return FieldType::String;
        case google::protobuf::FieldDescriptor::TYPE_BYTES:
            return FieldType::Bytes;
        case google::protobuf::FieldDescriptor::TYPE_INT32:
        case google::protobuf::FieldDescriptor::TYPE_SINT32:
        case google::protobuf::FieldDescriptor::TYPE_UINT32:
        case google::protobuf::FieldDescriptor::TYPE_FIXED32:
        case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
            return FieldType::Int;
        case google::protobuf::FieldDescriptor::TYPE_INT64:
        case google::protobuf::FieldDescriptor::TYPE_SINT64:
        case google::protobuf::FieldDescriptor::TYPE_UINT64:
        case google::protobuf::FieldDescriptor::TYPE_FIXED64:
        case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
            return FieldType::Long;
        case google::protobuf::FieldDescriptor::TYPE_FLOAT:
            return FieldType::Float;
        case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
            return FieldType::Double;
        case google::protobuf::FieldDescriptor::TYPE_BOOL:
            return FieldType::Boolean;
        case google::protobuf::FieldDescriptor::TYPE_ENUM:
            return FieldType::Enum;
        case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
            return FieldType::Record;
        default:
            return FieldType::String; // Default fallback
    }
}

const google::protobuf::Descriptor* getMessageDescriptorByIndex(
    const google::protobuf::DescriptorPool* pool,
    const google::protobuf::FileDescriptor* file_desc,
    const std::vector<int32_t>& msg_index) {
    
    if (msg_index.empty() || msg_index[0] >= file_desc->message_type_count()) {
        return nullptr;
    }
    
    const google::protobuf::Descriptor* descriptor = file_desc->message_type(msg_index[0]);
    
    // Navigate nested types if there are more indexes
    for (size_t i = 1; i < msg_index.size() && descriptor; ++i) {
        if (msg_index[i] < descriptor->nested_type_count()) {
            descriptor = descriptor->nested_type(msg_index[i]);
        } else {
            return nullptr;
        }
    }
    
    return descriptor;
}

std::vector<int32_t> createMessageIndexArray(const google::protobuf::Descriptor* descriptor) {
    std::vector<int32_t> indexes;
    
    // Build index path from file descriptor to this message type
    const google::protobuf::FileDescriptor* file = descriptor->file();
    
    // Find the message type index within the file
    for (int i = 0; i < file->message_type_count(); ++i) {
        if (file->message_type(i) == descriptor) {
            indexes.push_back(i);
            return indexes;
        }
    }
    
    return indexes;
}

// extractFieldValue implementation moved to value_transform namespace above

} // namespace srclient::serdes::protobuf::utils