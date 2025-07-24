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

// Implementation of ProtobufVariantValue methods (declaration is in header)

// Copy constructor implementation
ProtobufVariantValue::ProtobufVariantValue(const ProtobufVariantValue& other) : type_(other.type_) {
    switch (other.type_) {
        case ValueType::Message: {
            const auto& msg_ptr = std::get<std::unique_ptr<google::protobuf::Message>>(other.value);
            if (msg_ptr) {
                value = std::unique_ptr<google::protobuf::Message>(msg_ptr->New());
                std::get<std::unique_ptr<google::protobuf::Message>>(value)->CopyFrom(*msg_ptr);
            } else {
                value = std::unique_ptr<google::protobuf::Message>();
            }
            break;
        }
        case ValueType::List: {
            const auto& list = std::get<std::vector<ProtobufVariantValue>>(other.value);
            value = std::vector<ProtobufVariantValue>(list);
            break;
        }
        case ValueType::Map: {
            const auto& map = std::get<std::map<MapKey, ProtobufVariantValue>>(other.value);
            value = std::map<MapKey, ProtobufVariantValue>(map);
            break;
        }
        case ValueType::Bool: {
            value = std::get<bool>(other.value);
            break;
        }
        case ValueType::I32:
        case ValueType::EnumNumber: {
            value = std::get<int32_t>(other.value);
            break;
        }
        case ValueType::I64: {
            value = std::get<int64_t>(other.value);
            break;
        }
        case ValueType::U32: {
            value = std::get<uint32_t>(other.value);
            break;
        }
        case ValueType::U64: {
            value = std::get<uint64_t>(other.value);
            break;
        }
        case ValueType::F32: {
            value = std::get<float>(other.value);
            break;
        }
        case ValueType::F64: {
            value = std::get<double>(other.value);
            break;
        }
        case ValueType::String: {
            value = std::get<std::string>(other.value);
            break;
        }
        case ValueType::Bytes: {
            value = std::get<std::vector<uint8_t>>(other.value);
            break;
        }
    }
}

// Assignment operator implementation
ProtobufVariantValue& ProtobufVariantValue::operator=(const ProtobufVariantValue& other) {
    if (this != &other) {
        type_ = other.type_;
        switch (other.type_) {
            case ValueType::Message: {
                const auto& msg_ptr = std::get<std::unique_ptr<google::protobuf::Message>>(other.value);
                if (msg_ptr) {
                    value = std::unique_ptr<google::protobuf::Message>(msg_ptr->New());
                    std::get<std::unique_ptr<google::protobuf::Message>>(value)->CopyFrom(*msg_ptr);
                } else {
                    value = std::unique_ptr<google::protobuf::Message>();
                }
                break;
            }
            case ValueType::List: {
                const auto& list = std::get<std::vector<ProtobufVariantValue>>(other.value);
                value = std::vector<ProtobufVariantValue>(list);
                break;
            }
            case ValueType::Map: {
                const auto& map = std::get<std::map<MapKey, ProtobufVariantValue>>(other.value);
                value = std::map<MapKey, ProtobufVariantValue>(map);
                break;
            }
            case ValueType::Bool: {
                value = std::get<bool>(other.value);
                break;
            }
            case ValueType::I32:
            case ValueType::EnumNumber: {
                value = std::get<int32_t>(other.value);
                break;
            }
            case ValueType::I64: {
                value = std::get<int64_t>(other.value);
                break;
            }
            case ValueType::U32: {
                value = std::get<uint32_t>(other.value);
                break;
            }
            case ValueType::U64: {
                value = std::get<uint64_t>(other.value);
                break;
            }
            case ValueType::F32: {
                value = std::get<float>(other.value);
                break;
            }
            case ValueType::F64: {
                value = std::get<double>(other.value);
                break;
            }
            case ValueType::String: {
                value = std::get<std::string>(other.value);
                break;
            }
            case ValueType::Bytes: {
                value = std::get<std::vector<uint8_t>>(other.value);
                break;
            }
        }
    }
    return *this;
}

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
                auto message_ptr_copy = std::unique_ptr<google::protobuf::Message>(message_ptr->New());
                message_ptr_copy->CopyFrom(*message_ptr);
                ProtobufVariantValue message_variant(std::move(message_ptr_copy));
                auto transformed_message = transformRecursive(
                    ctx, descriptor, message_variant, field_executor_type);

                // Extract the transformed message and create SerdeValue
                if (transformed_message.type_ == ProtobufVariantValue::ValueType::Message &&
                    transformed_message.is<std::unique_ptr<google::protobuf::Message>>()) {
                    auto& msg_ptr = transformed_message.get<std::unique_ptr<google::protobuf::Message>>();
                    return protobuf::makeProtobufValue(*msg_ptr);
                }

                // Fallback: return original message
                return protobuf::makeProtobufValue(*message_ptr);
            }
        }
    }
    return value.clone();
}

ProtobufVariantValue transformRecursive(
    RuleContext& ctx,
    const google::protobuf::Descriptor* descriptor,
    const ProtobufVariantValue& message,
    const std::string& field_executor_type) {
    
    switch (message.type_) {
        case ProtobufVariantValue::ValueType::List: {
            // Handle List
            const auto& list = std::get<std::vector<ProtobufVariantValue>>(message.value);
            std::vector<ProtobufVariantValue> result;
            result.reserve(list.size());
            for (const auto& item : list) {
                result.push_back(transformRecursive(ctx, descriptor, item, field_executor_type));
            }
            return ProtobufVariantValue(result);
        }
        case ProtobufVariantValue::ValueType::Map: {
            // Handle Map
            const auto& map = std::get<std::map<MapKey, ProtobufVariantValue>>(message.value);
            std::map<MapKey, ProtobufVariantValue> result;
            for (const auto& [key, value] : map) {
                result.emplace(key, transformRecursive(ctx, descriptor, value, field_executor_type));
            }
            return ProtobufVariantValue(result);
        }
        case ProtobufVariantValue::ValueType::Message: {
            // Handle Message
            const auto& msg_ptr = std::get<std::unique_ptr<google::protobuf::Message>>(message.value);
            if (!msg_ptr) {
                return message; // Return original if null
            }
            
            auto result = std::unique_ptr<google::protobuf::Message>(msg_ptr->New());
            result->CopyFrom(*msg_ptr);
            
            for (int i = 0; i < descriptor->field_count(); ++i) {
                const google::protobuf::FieldDescriptor* fd = descriptor->field(i);
                auto field = transformFieldWithContext(ctx, fd, descriptor, result.get(), field_executor_type);
                if (field.has_value()) {
                    // Set the field in the message based on the transformed value
                    setMessageField(result.get(), fd, field.value());
                }
            }
            return ProtobufVariantValue(std::move(result));
        }
        default: {
            // Handle primitive types (Bool, I32, I64, U32, U64, F32, F64, String, Bytes, EnumNumber)
            // Field-level transformation logic
            auto field_ctx = ctx.currentField();
            if (field_ctx.has_value()) {
                auto rule_tags = ctx.getRule().getTags();
                std::unordered_set<std::string> rule_tags_set;
                if (rule_tags.has_value()) {
                    rule_tags_set = std::unordered_set<std::string>(
                        rule_tags->begin(), rule_tags->end());
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
                    // Create a SerdeValue from the current ProtobufVariantValue
                    auto message_value = convertVariantToSerdeValue(message);

                    // Try to get executor from context's rule registry first, then global
                    std::shared_ptr<RuleExecutor> executor;
                    if (ctx.getRuleRegistry()) {
                        executor = ctx.getRuleRegistry()->getExecutor(field_executor_type);
                    }
                    if (!executor) {
                        executor = global_registry::getRuleExecutor(field_executor_type);
                    }

                    if (executor) {
                        auto field_executor = std::dynamic_pointer_cast<FieldRuleExecutor>(executor);
                        if (!field_executor) {
                            throw ProtobufError("executor " + field_executor_type + 
                                              " is not a field rule executor");
                        }

                        auto new_value = field_executor->transformField(ctx, *message_value);
                        if (new_value && new_value->getFormat() == SerdeFormat::Protobuf) {
                            return convertSerdeValueToProtobufValue(*new_value);
                        }
                    }
                }
            }
            return message;
        }
    }
}

std::optional<ProtobufVariantValue> transformFieldWithContext(
    RuleContext& ctx,
    const google::protobuf::FieldDescriptor* fd,
    const google::protobuf::Descriptor* desc,
    const google::protobuf::Message* message,
    const std::string& field_executor_type) {
    
    // Create a simple SerdeValue for the field context (simplified implementation)
    auto temp_message = std::unique_ptr<google::protobuf::Message>(message->New());
    temp_message->CopyFrom(*message);
    auto temp_serde_value = protobuf::makeProtobufValue(*temp_message);
    
    ctx.enterField(
        *temp_serde_value,
        fd->full_name(),
        fd->name(),
        getFieldType(fd),
        getInlineTags(fd)
    );
    
    if (fd->containing_oneof() && !message->GetReflection()->HasField(*message, fd)) {
        // Skip oneof fields that are not set
        ctx.exitField();
        return std::nullopt;
    }
    
    ProtobufVariantValue value = getMessageFieldValue(message, fd);
    ProtobufVariantValue new_value = transformRecursive(ctx, desc, value, field_executor_type);
    
    // Check if this is a condition rule - simplified check
    if (new_value.type_ == ProtobufVariantValue::ValueType::Bool && new_value.is<bool>() && !new_value.get<bool>()) {
        ctx.exitField();
        // Simplified error for now - would need proper SerdeError construction
        throw std::runtime_error("Rule condition failed");
    }
    
    ctx.exitField();
    return new_value;
}

// Helper function to extract field value from protobuf message
ProtobufVariantValue getMessageFieldValue(const google::protobuf::Message* message, 
                                   const google::protobuf::FieldDescriptor* fd) {
    const google::protobuf::Reflection* reflection = message->GetReflection();
    
    if (fd->is_repeated()) {
        std::vector<ProtobufVariantValue> list_values;
        int field_size = reflection->FieldSize(*message, fd);
        
        for (int i = 0; i < field_size; ++i) {
            switch (fd->type()) {
                case google::protobuf::FieldDescriptor::TYPE_BOOL:
                    list_values.emplace_back(reflection->GetRepeatedBool(*message, fd, i));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_INT32:
                case google::protobuf::FieldDescriptor::TYPE_SINT32:
                case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
                    list_values.emplace_back(reflection->GetRepeatedInt32(*message, fd, i));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_INT64:
                case google::protobuf::FieldDescriptor::TYPE_SINT64:
                case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
                    list_values.emplace_back(reflection->GetRepeatedInt64(*message, fd, i));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_UINT32:
                case google::protobuf::FieldDescriptor::TYPE_FIXED32:
                    list_values.emplace_back(reflection->GetRepeatedUInt32(*message, fd, i));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_UINT64:
                case google::protobuf::FieldDescriptor::TYPE_FIXED64:
                    list_values.emplace_back(reflection->GetRepeatedUInt64(*message, fd, i));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_FLOAT:
                    list_values.emplace_back(reflection->GetRepeatedFloat(*message, fd, i));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
                    list_values.emplace_back(reflection->GetRepeatedDouble(*message, fd, i));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_STRING:
                    list_values.emplace_back(reflection->GetRepeatedString(*message, fd, i));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_BYTES: {
                    std::string bytes_str = reflection->GetRepeatedString(*message, fd, i);
                    std::vector<uint8_t> bytes(bytes_str.begin(), bytes_str.end());
                    list_values.emplace_back(bytes);
                    break;
                }
                case google::protobuf::FieldDescriptor::TYPE_ENUM:
                    list_values.emplace_back(ProtobufVariantValue::createEnum(reflection->GetRepeatedEnumValue(*message, fd, i)));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_MESSAGE: {
                    const google::protobuf::Message& sub_message = reflection->GetRepeatedMessage(*message, fd, i);
                    auto msg_copy = std::unique_ptr<google::protobuf::Message>(sub_message.New());
                    msg_copy->CopyFrom(sub_message);
                    list_values.emplace_back(std::move(msg_copy));
                    break;
                }
                case google::protobuf::FieldDescriptor::TYPE_GROUP: {
                    // Handle deprecated GROUP type as message
                    const google::protobuf::Message& sub_message = reflection->GetRepeatedMessage(*message, fd, i);
                    auto msg_copy = std::unique_ptr<google::protobuf::Message>(sub_message.New());
                    msg_copy->CopyFrom(sub_message);
                    list_values.emplace_back(std::move(msg_copy));
                    break;
                }
            }
        }
        return ProtobufVariantValue(list_values);
    } else {
        // Handle singular fields
        switch (fd->type()) {
            case google::protobuf::FieldDescriptor::TYPE_BOOL:
                return ProtobufVariantValue(reflection->GetBool(*message, fd));
            case google::protobuf::FieldDescriptor::TYPE_INT32:
            case google::protobuf::FieldDescriptor::TYPE_SINT32:
            case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
                return ProtobufVariantValue(reflection->GetInt32(*message, fd));
            case google::protobuf::FieldDescriptor::TYPE_INT64:
            case google::protobuf::FieldDescriptor::TYPE_SINT64:
            case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
                return ProtobufVariantValue(reflection->GetInt64(*message, fd));
            case google::protobuf::FieldDescriptor::TYPE_UINT32:
            case google::protobuf::FieldDescriptor::TYPE_FIXED32:
                return ProtobufVariantValue(reflection->GetUInt32(*message, fd));
            case google::protobuf::FieldDescriptor::TYPE_UINT64:
            case google::protobuf::FieldDescriptor::TYPE_FIXED64:
                return ProtobufVariantValue(reflection->GetUInt64(*message, fd));
            case google::protobuf::FieldDescriptor::TYPE_FLOAT:
                return ProtobufVariantValue(reflection->GetFloat(*message, fd));
            case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
                return ProtobufVariantValue(reflection->GetDouble(*message, fd));
            case google::protobuf::FieldDescriptor::TYPE_STRING:
                return ProtobufVariantValue(reflection->GetString(*message, fd));
            case google::protobuf::FieldDescriptor::TYPE_BYTES: {
                std::string bytes_str = reflection->GetString(*message, fd);
                std::vector<uint8_t> bytes(bytes_str.begin(), bytes_str.end());
                return ProtobufVariantValue(bytes);
            }
            case google::protobuf::FieldDescriptor::TYPE_ENUM:
                return ProtobufVariantValue::createEnum(reflection->GetEnumValue(*message, fd));
            case google::protobuf::FieldDescriptor::TYPE_MESSAGE: {
                const google::protobuf::Message& sub_message = reflection->GetMessage(*message, fd);
                auto msg_copy = std::unique_ptr<google::protobuf::Message>(sub_message.New());
                msg_copy->CopyFrom(sub_message);
                return ProtobufVariantValue(std::move(msg_copy));
            }
            case google::protobuf::FieldDescriptor::TYPE_GROUP:
                // Handle deprecated GROUP type as message
                {
                    const google::protobuf::Message& sub_message = reflection->GetMessage(*message, fd);
                    auto msg_copy = std::unique_ptr<google::protobuf::Message>(sub_message.New());
                    msg_copy->CopyFrom(sub_message);
                    return ProtobufVariantValue(std::move(msg_copy));
                }
            default:
                return ProtobufVariantValue(std::string("")); // Default fallback
        }
    }
}

// Helper function to set field value in protobuf message
void setMessageField(google::protobuf::Message* message, 
                     const google::protobuf::FieldDescriptor* fd, 
                     const ProtobufVariantValue& value) {
    const google::protobuf::Reflection* reflection = message->GetReflection();
    
    switch (value.type_) {
        case ProtobufVariantValue::ValueType::Bool: {
            const auto& v = std::get<bool>(value.value);
            reflection->SetBool(message, fd, v);
            break;
        }
        case ProtobufVariantValue::ValueType::I32: {
            const auto& v = std::get<int32_t>(value.value);
            reflection->SetInt32(message, fd, v);
            break;
        }
        case ProtobufVariantValue::ValueType::EnumNumber: {
            const auto& v = std::get<int32_t>(value.value);
            reflection->SetEnumValue(message, fd, v);
            break;
        }
        case ProtobufVariantValue::ValueType::I64: {
            const auto& v = std::get<int64_t>(value.value);
            reflection->SetInt64(message, fd, v);
            break;
        }
        case ProtobufVariantValue::ValueType::U32: {
            const auto& v = std::get<uint32_t>(value.value);
            reflection->SetUInt32(message, fd, v);
            break;
        }
        case ProtobufVariantValue::ValueType::U64: {
            const auto& v = std::get<uint64_t>(value.value);
            reflection->SetUInt64(message, fd, v);
            break;
        }
        case ProtobufVariantValue::ValueType::F32: {
            const auto& v = std::get<float>(value.value);
            reflection->SetFloat(message, fd, v);
            break;
        }
        case ProtobufVariantValue::ValueType::F64: {
            const auto& v = std::get<double>(value.value);
            reflection->SetDouble(message, fd, v);
            break;
        }
        case ProtobufVariantValue::ValueType::String: {
            const auto& v = std::get<std::string>(value.value);
            reflection->SetString(message, fd, v);
            break;
        }
        case ProtobufVariantValue::ValueType::Bytes: {
            const auto& v = std::get<std::vector<uint8_t>>(value.value);
            std::string bytes_str(v.begin(), v.end());
            reflection->SetString(message, fd, bytes_str);
            break;
        }
        case ProtobufVariantValue::ValueType::Message: {
            const auto& v = std::get<std::unique_ptr<google::protobuf::Message>>(value.value);
            if (v) {
                google::protobuf::Message* mutable_msg = reflection->MutableMessage(message, fd);
                mutable_msg->CopyFrom(*v);
            }
            break;
        }
        case ProtobufVariantValue::ValueType::List:
        case ProtobufVariantValue::ValueType::Map:
            // Note: List and Map handling would require more complex logic
            // for repeated and map fields, which is not implemented here for brevity
            break;
    }
}

// Helper function to convert ProtobufVariantValue to SerdeValue
std::unique_ptr<SerdeValue> convertVariantToSerdeValue(const ProtobufVariantValue& variant) {
    switch (variant.type_) {
        case ProtobufVariantValue::ValueType::Message: {
            const auto& msg_ptr = std::get<std::unique_ptr<google::protobuf::Message>>(variant.value);
            if (msg_ptr) {
                return protobuf::makeProtobufValue(*msg_ptr);
            }
            break;
        }
        default: {
            // For primitive types, create a simple message wrapper
            // This is a simplified approach - in practice you might want to create a proper wrapper
            auto dummy_message = std::make_unique<confluent::Meta>();
            return protobuf::makeProtobufValue(*dummy_message);
        }
    }
    // Fallback
    auto dummy_message = std::make_unique<confluent::Meta>();
    return protobuf::makeProtobufValue(*dummy_message);
}

// Helper function to convert SerdeValue back to ProtobufVariantValue if needed
ProtobufVariantValue convertSerdeValueToProtobufValue(const SerdeValue& serde_value) {
    if (serde_value.getFormat() == SerdeFormat::Protobuf) {
        const auto& message = asProtobuf(serde_value);
        auto msg_copy = std::unique_ptr<google::protobuf::Message>(message.New());
        msg_copy->CopyFrom(message);
        return ProtobufVariantValue(std::move(msg_copy));
    }
    // Fallback for non-protobuf values
    return ProtobufVariantValue(std::string(""));
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