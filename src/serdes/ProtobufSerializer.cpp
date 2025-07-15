#include "srclient/serdes/ProtobufSerializer.h"
#include "srclient/serdes/ProtobufUtils.h"

namespace srclient::serdes {

using namespace protobuf_utils;

// Default reference subject name strategy implementation
std::string defaultReferenceSubjectNameStrategy(const std::string& ref_name, SerdeType serde_type) {
    return ref_name;
}

// ProtobufSerde implementation
ProtobufSerde::ProtobufSerde() {}

std::pair<const google::protobuf::FileDescriptor*, const google::protobuf::DescriptorPool*> 
ProtobufSerde::getParsedSchema(const srclient::rest::model::Schema& schema, 
                              std::shared_ptr<srclient::rest::ISchemaRegistryClient> client) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Create cache key from schema content
    auto schema_str = schema.getSchema();
    std::string cache_key = schema_str.value_or("");
    
    auto it = parsed_schemas_cache_.find(cache_key);
    if (it != parsed_schemas_cache_.end()) {
        return {it->second.first.get(), it->second.second.get()};
    }
    
    // Parse new schema
    auto pool = std::make_unique<google::protobuf::DescriptorPool>();
    std::unordered_set<std::string> visited;
    
    // Resolve dependencies first
    auto references = schema.getReferences();
    if (references.has_value()) {
        for (const auto& ref : references.value()) {
            resolveNamedSchema(schema, client, pool.get(), visited);
        }
    }
    
    // Parse main schema
    auto file_desc = stringToSchema(pool.get(), "main.proto", cache_key);
    
    // Store in cache
    parsed_schemas_cache_[cache_key] = std::make_pair(
        std::unique_ptr<google::protobuf::FileDescriptor>(const_cast<google::protobuf::FileDescriptor*>(file_desc)),
        std::move(pool)
    );
    
    return {file_desc, parsed_schemas_cache_[cache_key].second.get()};
}

void ProtobufSerde::clear() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    parsed_schemas_cache_.clear();
}

void ProtobufSerde::resolveNamedSchema(const srclient::rest::model::Schema& schema,
                                      std::shared_ptr<srclient::rest::ISchemaRegistryClient> client,
                                      google::protobuf::DescriptorPool* pool,
                                      std::unordered_set<std::string>& visited) {
    // TODO: Implement dependency resolution
    // This would recursively resolve schema references
}

// Template method implementations for helper methods

template<typename ClientType>
std::vector<int32_t> ProtobufSerializer<ClientType>::toIndexArray(const google::protobuf::Descriptor* descriptor) {
    std::vector<int32_t> indexes;
    
    // Build index path from file descriptor to this message type
    const google::protobuf::FileDescriptor* file = descriptor->file();
    
    // Find the message type index within the file
    for (int i = 0; i < file->message_type_count(); ++i) {
        if (file->message_type(i) == descriptor) {
            indexes.push_back(i);
            break;
        }
    }
    
    return indexes;
}

template<typename ClientType>
void ProtobufSerializer<ClientType>::validateSchema(const srclient::rest::model::Schema& schema) {
    auto schema_str = schema.getSchema();
    if (!schema_str.has_value() || schema_str->empty()) {
        throw protobuf_utils::ProtobufSerdeError("Schema content is empty");
    }
    
    auto schema_type = schema.getSchemaType();
    if (schema_type.has_value() && schema_type.value() != "PROTOBUF") {
        throw protobuf_utils::ProtobufSerdeError("Schema type must be PROTOBUF");
    }
}

template<typename ClientType>
SerdeValue ProtobufSerializer<ClientType>::transformValue(const SerdeValue& value, 
                                                         const srclient::rest::model::Rule& rule,
                                                         const RuleContext& context) {
    // TODO: Implement value transformation based on rules
    return value;
}

// Explicit template instantiation
template class ProtobufSerializer<srclient::rest::ISchemaRegistryClient>;

} // namespace srclient::serdes 