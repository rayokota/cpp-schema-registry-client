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


pub enum Value {
    /// A boolean value, encoded as the `bool` protobuf type.
    Bool(bool),
    /// A 32-bit signed integer, encoded as one of the `int32`, `sint32` or `sfixed32` protobuf types.
    I32(i32),
    /// A 64-bit signed integer, encoded as one of the `int64`, `sint64` or `sfixed64` protobuf types.
    I64(i64),
    /// A 32-bit unsigned integer, encoded as one of the `uint32` or `ufixed32` protobuf types.
    U32(u32),
    /// A 64-bit unsigned integer, encoded as one of the `uint64` or `ufixed64` protobuf types.
    U64(u64),
    /// A 32-bit floating point number, encoded as the `float` protobuf type.
    F32(f32),
    /// A 64-bit floating point number, encoded as the `double` protobuf type.
    F64(f64),
    /// A string, encoded as the `string` protobuf type.
    String(String),
    /// A byte string, encoded as the `bytes` protobuf type.
    Bytes(Bytes),
    /// An enumeration value, encoded as a protobuf enum.
    EnumNumber(i32),
    /// A protobuf message.
    Message(DynamicMessage),
    /// A list of values, encoded as a protobuf repeated field.
    List(Vec<Value>),
    /// A map of values, encoded as a protobuf map field.
    Map(HashMap<MapKey, Value>),
}

async fn transform_recursive(
    ctx: &mut RuleContext,
    descriptor: &MessageDescriptor,
    message: &Value,
    field_executor_type: &str,
) -> Result<Value, SerdeError> {
    match message {
        Value::List(items) => {
            let mut result = Vec::with_capacity(items.len());
            for item in items {
                let item = transform_recursive(ctx, descriptor, item, field_executor_type).await?;
                result.push(item);
            }
            return Ok(Value::List(result));
        }
        Value::Map(map) => {
            let mut result = HashMap::new();
            for (key, value) in map {
                let value = transform_recursive(ctx, descriptor, value, field_executor_type).await?;
                result.insert(key.clone(), value);
            }
            return Ok(Value::Map(result));
        }
        Value::Message(message) => {
            let mut result = message.clone();
            for fd in descriptor.fields() {
                let field =
                    transform_field_with_ctx(ctx, &fd, descriptor, message, field_executor_type)
                        .await?;
                if let Some(field) = field {
                    result.set_field(&fd, field);
                }
            }
            return Ok(Value::Message(result));
        }
        _ => {
            if let Some(field_ctx) = ctx.current_field() {
                let rule_tags = ctx
                    .rule
                    .tags
                    .clone()
                    .map(|v| HashSet::from_iter(v.into_iter()));
                if rule_tags.is_none_or(|tags| !tags.is_disjoint(&field_ctx.tags)) {
                    let message_value = SerdeValue::Protobuf(message.clone());
                    let executor = get_executor(ctx.rule_registry.as_ref(), field_executor_type);
                    if let Some(executor) = executor {
                        let field_executor =
                            executor
                                .as_field_rule_executor()
                                .ok_or(SerdeError::Rule(format!(
                                    "executor {field_executor_type} is not a field rule executor"
                                )))?;
                        let new_value = field_executor.transform_field(ctx, &message_value).await?;
                        if let SerdeValue::Protobuf(v) = new_value {
                            return Ok(v);
                        }
                    }
                }
            }
        }
    }
    Ok(message.clone())
}

async fn transform_field_with_ctx(
    ctx: &mut RuleContext,
    fd: &FieldDescriptor,
    desc: &MessageDescriptor,
    message: &DynamicMessage,
    field_executor_type: &str,
) -> Result<Option<Value>, SerdeError> {
    let message_value = SerdeValue::Protobuf(Value::Message(message.clone()));
    ctx.enter_field(
        message_value,
        fd.full_name().to_string(),
        fd.name().to_string(),
        get_type(fd),
        get_inline_tags(fd),
    );
    if fd.containing_oneof().is_some() && !message.has_field(fd) {
        // skip oneof fields that are not set
        return Ok(None);
    }
    let value = message.get_field(fd);
    let new_value = transform_recursive(ctx, desc, &value, field_executor_type).await?;
    if let Some(Kind::Condition) = ctx.rule.kind {
        if let Value::Bool(b) = new_value {
            if !b {
                return Err(SerdeError::RuleCondition(Box::new(ctx.rule.clone())));
            }
        }
    }
    ctx.exit_field();
    Ok(Some(new_value))
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