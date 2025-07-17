#pragma once

#include <memory>
#include <any>
#include <optional>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include <avro/Generic.hh>
#include <avro/ValidSchema.hh>
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

/**
 * Schema format enum
 */
enum class SerdeSchemaFormat {
    Avro,
    Json,
    Protobuf
};

/**
 * Base interface for schema wrappers
 * Based on SerdeSchema from serde.rs
 */
class SerdeSchema {
public:
    virtual ~SerdeSchema() = default;
    
    // Type checking methods
    virtual bool isAvro() const = 0;
    virtual bool isJson() const = 0;
    virtual bool isProtobuf() const = 0;
    
    // Format accessor
    virtual SerdeSchemaFormat getFormat() const = 0;
    
    // Schema data access methods
    virtual std::string getSchemaData() const = 0;
    virtual std::optional<std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>> getAvroSchema() const = 0;
    
    // Clone method
    virtual std::unique_ptr<SerdeSchema> clone() const = 0;
};

} // namespace srclient::serdes 