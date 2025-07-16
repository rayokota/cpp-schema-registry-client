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
    // Return SerdeValue containing reference to the protobuf message
    // Note: We need to cast away const because reference_wrapper doesn't support const references in this context
    auto& non_const_message = const_cast<google::protobuf::Message&>(message);
    return std::reference_wrapper<google::protobuf::Message>(non_const_message);
}

template<typename ClientType>
void ProtobufDeserializer<ClientType>::serdeValueToMessage(
    const SerdeValue& value, 
    google::protobuf::Message* message) {
    
    if (isProtobuf(value)) {
        // If SerdeValue contains a protobuf message, copy it
        try {
            const auto& source_message = asProtobuf(value);
            message->CopyFrom(source_message);
        } catch (const SerdeError& e) {
            throw protobuf_utils::ProtobufSerdeError("Failed to extract protobuf from SerdeValue: " + std::string(e.what()));
        }
    } else if (isJson(value)) {
        // Convert SerdeValue to JSON, then to protobuf message (fallback)
        auto json_value = asJson(value);
        std::string json_str = json_value.dump();
        
        google::protobuf::util::JsonParseOptions options;
        auto status = google::protobuf::util::JsonStringToMessage(json_str, message, options);
        if (!status.ok()) {
            throw protobuf_utils::ProtobufSerdeError("Failed to parse JSON to protobuf message: " + status.ToString());
        }
    } else {
        throw protobuf_utils::ProtobufSerdeError("SerdeValue must be Protobuf or Json type for conversion to protobuf message");
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