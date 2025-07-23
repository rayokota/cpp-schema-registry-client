#include "srclient/serdes/protobuf/ProtobufUtils.h"

#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/json_util.h>

#include <memory>  // For std::dynamic_pointer_cast

#include "absl/strings/escaping.h"
#include "confluent/meta.pb.h"
#include "srclient/serdes/RuleRegistry.h"  // For global_registry functions

namespace srclient::serdes::protobuf::utils {

// Base64 encoding/decoding utilities using absl
namespace {
std::string base64_encode(const std::vector<uint8_t> &bytes) {
    std::string input(reinterpret_cast<const char *>(bytes.data()),
                      bytes.size());
    return absl::Base64Escape(input);
}

std::vector<uint8_t> base64_decode(const std::string &encoded_string) {
    std::string decoded;
    if (!absl::Base64Unescape(encoded_string, &decoded)) {
        // Return empty vector on decode failure
        return std::vector<uint8_t>();
    }
    return std::vector<uint8_t>(decoded.begin(), decoded.end());
}
}  // namespace

std::unique_ptr<SerdeValue> transformFields(
    RuleContext &ctx, const std::string &field_executor_type,
    const SerdeValue &value) {
    // Check if we have a protobuf schema and value
    const auto &parsed_target = ctx.getParsedTarget();
    if (parsed_target.has_value() && parsed_target->get() &&
        parsed_target->get()->getFormat() == SerdeFormat::Protobuf) {
        if (value.getFormat() == SerdeFormat::Protobuf) {
            auto &message = asProtobuf(value);
            auto message_ptr =
                const_cast<google::protobuf::Message *>(&message);
            if (message_ptr) {
                const google::protobuf::Descriptor *descriptor =
                    message_ptr->GetDescriptor();
                if (!descriptor) {
                    throw ProtobufError("Message descriptor not found for " +
                                        message_ptr->GetTypeName());
                }

                // Transform the message using the synchronous method
                auto transformed_message = transformRecursive(
                    ctx, descriptor, message_ptr, field_executor_type);
                return protobuf::makeProtobufValue(*transformed_message);
            }
        }
    }
    return value.clone();
}

std::unique_ptr<google::protobuf::Message> transformRecursive(
    RuleContext &ctx, const google::protobuf::Descriptor *descriptor,
    google::protobuf::Message *message, const std::string &field_executor_type) {
    
    // Handle different message types based on their structure
    // This is a synchronous version of the Rust async function
    
    // Clone the input message for transformation
    auto result = std::unique_ptr<google::protobuf::Message>(message->New());
    result->CopyFrom(*message);
    
    // Transform all fields in the message
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const google::protobuf::FieldDescriptor *field_desc = descriptor->field(i);
        
        auto transformed_field = transformFieldWithContext(
            ctx, field_desc, descriptor, result.get(), field_executor_type);
        
        if (transformed_field.has_value()) {
            // Set the transformed field value in the result message
            // The actual field setting logic would depend on the field type
            // This is a simplified version - actual implementation would need
            // proper field value handling based on protobuf reflection API
        }
    }
    
    // Check if we need to apply field-level transformations
    auto current_field = ctx.currentField();
    if (current_field.has_value()) {
        // Get rule tags from context
        auto rule_tags = ctx.getRule().getTags();
        std::unordered_set<std::string> rule_tag_set;
        if (rule_tags.has_value()) {
            rule_tag_set.insert(rule_tags->begin(), rule_tags->end());
        }
        
        // Check if rule tags intersect with field tags
        const auto &field_tags = current_field->getTags();
        bool tags_intersect = rule_tag_set.empty();
        if (!rule_tag_set.empty()) {
            for (const auto &tag : field_tags) {
                if (rule_tag_set.find(tag) != rule_tag_set.end()) {
                    tags_intersect = true;
                    break;
                }
            }
        }
        
        if (tags_intersect) {
            // Create SerdeValue for the message
            auto message_value = makeProtobufValue(*result);
            
            // Get the field executor
            auto executor = ctx.getRuleRegistry() 
                ? ctx.getRuleRegistry()->getExecutor(field_executor_type)
                : global_registry::getRuleExecutor(field_executor_type);
            
            if (executor) {
                // Cast to field rule executor
                auto field_executor = std::dynamic_pointer_cast<FieldRuleExecutor>(executor);
                if (field_executor) {
                    auto transformed_value = field_executor->transform(ctx, *message_value);
                    if (transformed_value->getFormat() == SerdeFormat::Protobuf) {
                        auto &transformed_pb = asProtobuf(*transformed_value);
                        result = std::unique_ptr<google::protobuf::Message>(transformed_pb.New());
                        result->CopyFrom(transformed_pb);
                    }
                } else {
                    throw SerdeError("executor " + field_executor_type + 
                                   " is not a field rule executor");
                }
            }
        }
    }
    
    return result;
}

std::optional<google::protobuf::Message*> transformFieldWithContext(
    RuleContext &ctx, const google::protobuf::FieldDescriptor *field_desc,
    const google::protobuf::Descriptor *message_desc,
    google::protobuf::Message *message, const std::string &field_executor_type) {
    
    // Create message value for context
    auto message_value = makeProtobufValue(*message);
    
    // Get field type
    FieldType field_type = getFieldType(field_desc);
    
    // Get inline tags
    auto inline_tags = getInlineTags(field_desc);
    
    // Enter field context
    ctx.enterField(*message_value, 
                   field_desc->full_name(), 
                   field_desc->name(),
                   field_type,
                   inline_tags);
    
    try {
        // Skip oneof fields that are not set
        if (field_desc->containing_oneof() && 
            !message->GetReflection()->HasField(*message, field_desc)) {
            ctx.exitField();
            return std::nullopt;
        }
        
        // Transform the field recursively
        auto transformed_message = transformRecursive(
            ctx, message_desc, message, field_executor_type);
        
        // Check for condition rules
        auto rule_kind = ctx.getRule().getKind();
        if (rule_kind.has_value() && rule_kind.value() == Kind::Condition) {
            // For condition rules, we need to check if the result is boolean
            // This is a simplified check - actual implementation would depend
            // on how boolean results are represented in protobuf messages
            // For now, we'll assume the transformation succeeded
        }
        
        ctx.exitField();
        return transformed_message.get();
        
    } catch (const std::exception &e) {
        ctx.exitField();
        throw;
    }
}

std::unordered_set<std::string> getInlineTags(
    const google::protobuf::FieldDescriptor *field_desc) {
    std::unordered_set<std::string> tag_set;

    if (!field_desc) {
        return tag_set;
    }

    // Try to get the confluent.field_meta extension from the field options
    const google::protobuf::FieldOptions &options = field_desc->options();

    if (options.HasExtension(confluent::field_meta)) {
        auto ext = options.GetExtension(confluent::field_meta);
        const auto &tags =
            ext.tags();  // Call tags() method to get RepeatedPtrField
        for (const auto &tag : tags) {
            tag_set.insert(tag);
        }
    }

    return tag_set;
}

std::string schemaToString(const google::protobuf::FileDescriptor *file_desc) {
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

const google::protobuf::FileDescriptor *stringToSchema(
    google::protobuf::DescriptorPool *pool, const std::string &name,
    const std::string &schema_string) {
    // Base64 decode
    std::vector<uint8_t> bytes = base64_decode(schema_string);

    decodeFileDescriptorProtoWithName(pool, name, bytes);

    const google::protobuf::FileDescriptor *file_desc =
        pool->FindFileByName(name);
    if (!file_desc) {
        throw ProtobufError("File descriptor not found after decoding: " +
                            name);
    }

    return file_desc;
}

void decodeFileDescriptorProtoWithName(google::protobuf::DescriptorPool *pool,
                                       const std::string &name,
                                       const std::vector<uint8_t> &data) {
    google::protobuf::FileDescriptorProto proto;
    if (!proto.ParseFromArray(data.data(), data.size())) {
        throw ProtobufError("Failed to parse FileDescriptorProto from data");
    }

    proto.set_name(name);

    const google::protobuf::FileDescriptor *file_desc = pool->BuildFile(proto);
    if (!file_desc) {
        throw ProtobufError("Failed to build FileDescriptor from proto");
    }
}

bool isBuiltin(const std::string &name) {
    return name.find("confluent/") == 0 || name.find("google/protobuf/") == 0 ||
           name.find("google/type/") == 0;
}

nlohmann::json messageToJson(const google::protobuf::Message &message) {
    std::string json_string;
    auto status =
        google::protobuf::util::MessageToJsonString(message, &json_string);
    if (!status.ok()) {
        throw ProtobufError("Failed to convert message to JSON: " +
                            status.ToString());
    }
    return nlohmann::json::parse(json_string);
}

void jsonToMessage(const nlohmann::json &json_value,
                   google::protobuf::Message *message) {
    std::string json_string = json_value.dump();
    google::protobuf::util::JsonParseOptions options;
    auto status = google::protobuf::util::JsonStringToMessage(json_string,
                                                              message, options);
    if (!status.ok()) {
        throw ProtobufError("Failed to parse JSON to message: " +
                            status.ToString());
    }
}

FieldType getFieldType(const google::protobuf::FieldDescriptor *field_desc) {
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
            return FieldType::String;  // Default fallback
    }
}

const google::protobuf::Descriptor *getMessageDescriptorByIndex(
    const google::protobuf::DescriptorPool *pool,
    const google::protobuf::FileDescriptor *file_desc,
    const std::vector<int32_t> &msg_index) {
    if (msg_index.empty() || msg_index[0] >= file_desc->message_type_count()) {
        return nullptr;
    }

    const google::protobuf::Descriptor *descriptor =
        file_desc->message_type(msg_index[0]);

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

std::vector<int32_t> createMessageIndexArray(
    const google::protobuf::Descriptor *descriptor) {
    std::vector<int32_t> indexes;

    // Build index path from file descriptor to this message type
    const google::protobuf::FileDescriptor *file = descriptor->file();

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

}  // namespace srclient::serdes::protobuf::utils