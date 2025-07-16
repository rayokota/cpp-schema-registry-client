#pragma once

#include <memory>
#include <nlohmann/json.hpp>
#include <avro/Generic.hh>
#include <google/protobuf/message.h>

namespace srclient::serdes {

/**
 * Serialization format types
 */
enum class SerdeFormat {
    Avro,
    Json,
    Protobuf
};

/**
 * Base interface for serialization values of different formats
 */
class SerdeValue {
public:
    virtual ~SerdeValue() = default;
    
    // Type checking methods
    virtual bool isJson() const = 0;
    virtual bool isAvro() const = 0;
    virtual bool isProtobuf() const = 0;
    
    // Value access methods - these will throw if wrong type
    virtual nlohmann::json asJson() const = 0;
    virtual avro::GenericDatum asAvro() const = 0;
    virtual google::protobuf::Message& asProtobuf() const = 0;
    
    // Get the format type
    virtual SerdeFormat getFormat() const = 0;
    
    // Clone method for copying
    virtual std::unique_ptr<SerdeValue> clone() const = 0;
};

} // namespace srclient::serdes 