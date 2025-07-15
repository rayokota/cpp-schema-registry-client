#include "srclient/serdes/ProtobufDeserializer.h"
#include "srclient/serdes/ProtobufUtils.h"
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/json_util.h>

namespace srclient::serdes {

// Helper method implementations for ProtobufDeserializer

template<typename ClientType>
std::unique_ptr<google::protobuf::Message> ProtobufDeserializer<ClientType>::createMessageFromDescriptor(
    const google::protobuf::Descriptor* descriptor) {
    
    google::protobuf::DynamicMessageFactory factory;
    const google::protobuf::Message* prototype = factory.GetPrototype(descriptor);
    if (!prototype) {
        throw protobuf_utils::ProtobufSerdeError("Failed to get message prototype for descriptor: " + descriptor->full_name());
    }
    
    return std::unique_ptr<google::protobuf::Message>(prototype->New());
}

template<typename ClientType>
void ProtobufDeserializer<ClientType>::transformFields(
    google::protobuf::Message* message,
    const google::protobuf::Descriptor* descriptor,
    const srclient::rest::model::Schema& writer_schema,
    const srclient::rest::model::Schema& reader_schema) {
    
    // TODO: Implement field transformations based on schema evolution rules
    // This would involve comparing field types, applying default values, etc.
}

template<typename ClientType>
SerdeValue ProtobufDeserializer<ClientType>::messageToSerdeValue(const google::protobuf::Message& message) {
    // Convert protobuf message to JSON first, then to SerdeValue
    std::string json_str;
    auto status = google::protobuf::util::MessageToJsonString(message, &json_str);
    if (!status.ok()) {
        throw protobuf_utils::ProtobufSerdeError("Failed to convert message to JSON: " + status.ToString());
    }
    nlohmann::json json_value = nlohmann::json::parse(json_str);
    return SerdeValue(json_value);
}

template<typename ClientType>
void ProtobufDeserializer<ClientType>::serdeValueToMessage(
    const SerdeValue& value, 
    google::protobuf::Message* message) {
    
    // Convert SerdeValue to JSON, then to protobuf message
    auto json_value = value.asJson();
    std::string json_str = json_value.dump();
    
    google::protobuf::util::JsonParseOptions options;
    auto status = google::protobuf::util::JsonStringToMessage(json_str, message, options);
    if (!status.ok()) {
        throw protobuf_utils::ProtobufSerdeError("Failed to parse JSON to protobuf message: " + status.ToString());
    }
}

template<typename ClientType>
bool ProtobufDeserializer<ClientType>::isEvolutionRequired(
    const srclient::rest::model::Schema& writer_schema,
    const srclient::rest::model::Schema& reader_schema) {
    
    // Compare schema content to determine if evolution is needed
    auto writer_schema_str = writer_schema.getSchema();
    auto reader_schema_str = reader_schema.getSchema();
    
    return writer_schema_str != reader_schema_str;
}

template<typename ClientType>
std::unique_ptr<google::protobuf::Message> ProtobufDeserializer<ClientType>::evolveMessage(
    const google::protobuf::Message& writer_message,
    const google::protobuf::Descriptor* reader_descriptor,
    const srclient::rest::model::Schema& writer_schema,
    const srclient::rest::model::Schema& reader_schema) {
    
    // Convert writer message to JSON for evolution
    auto writer_serde_value = messageToSerdeValue(writer_message);
    
    // Apply schema evolution transformations
    // TODO: Implement actual schema evolution logic based on migration rules
    
    // Create reader message and populate from evolved data
    auto reader_message = createMessageFromDescriptor(reader_descriptor);
    serdeValueToMessage(writer_serde_value, reader_message.get());
    
    return reader_message;
}

// Explicit template instantiation
template class ProtobufDeserializer<srclient::rest::ISchemaRegistryClient>;

} // namespace srclient::serdes 