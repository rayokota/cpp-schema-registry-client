#pragma once

#include <memory>
#include <any>
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
    
    // Value access method - returns std::any, use std::any_cast to get the actual type
    virtual std::any getValue() const = 0;
    
    // Get the format type
    virtual SerdeFormat getFormat() const = 0;
    
    // Clone method for copying
    virtual std::unique_ptr<SerdeValue> clone() const = 0;
};

} // namespace srclient::serdes 