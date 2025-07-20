#include "srclient/serdes/protobuf/ProtobufUtils.h"
#include "absl/strings/escaping.h"
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

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
    switch (field_desc->type()) {
        case google::protobuf::FieldDescriptor::TYPE_STRING:
            return FieldType::String;
        case google::protobuf::FieldDescriptor::TYPE_BYTES:
            return FieldType::Bytes;
        case google::protobuf::FieldDescriptor::TYPE_INT32:
        case google::protobuf::FieldDescriptor::TYPE_SINT32:
        case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
            return FieldType::Int;
        case google::protobuf::FieldDescriptor::TYPE_INT64:
        case google::protobuf::FieldDescriptor::TYPE_SINT64:
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

std::unique_ptr<SerdeValue> extractFieldValue(
    const google::protobuf::FieldDescriptor* field_desc,
    const google::protobuf::Message& message) {
    // TODO: Implement field value extraction
    // This should extract the value of the specified field from the message
    // and convert it to a SerdeValue
    throw std::runtime_error("extractFieldValue not yet implemented");
}

} // namespace srclient::serdes::protobuf::utils