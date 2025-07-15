#pragma once

#include <stdexcept>
#include <string>
#include <memory>

// Include for Rule type
#include "srclient/rest/model/Rule.h"

namespace srclient::serdes {

/**
 * Base exception class for all serialization/deserialization errors
 * Based on the SerdeError enum from serde.rs
 */
class SerdeError : public std::runtime_error {
public:
    explicit SerdeError(const std::string& message) : std::runtime_error(message) {}
    virtual ~SerdeError() = default;
};

/**
 * Avro-specific serialization errors
 * Maps to SerdeError::Avro variant
 */
class AvroError : public SerdeError {
public:
    explicit AvroError(const std::string& message) : SerdeError("Avro error: " + message) {}
};

/**
 * JSON referencing errors
 * Maps to SerdeError::JsonReferencing variant
 */
class JsonReferencingError : public SerdeError {
public:
    explicit JsonReferencingError(const std::string& message) : SerdeError("JSON referencing error: " + message) {}
    explicit JsonReferencingError() : SerdeError("JSON referencing error") {}
};

/**
 * JSON serialization errors
 * Maps to SerdeError::Json variant
 */
class JsonError : public SerdeError {
public:
    explicit JsonError(const std::string& message) : SerdeError("JSON serde error: " + message) {}
};

/**
 * JSON validation errors
 * Maps to SerdeError::JsonValidation variant
 */
class JsonValidationError : public SerdeError {
public:
    explicit JsonValidationError(const std::string& message) : SerdeError("JSON validation error: " + message) {}
};

/**
 * Protobuf decode errors
 * Maps to SerdeError::ProtobufDecode variant
 */
class ProtobufDecodeError : public SerdeError {
public:
    explicit ProtobufDecodeError(const std::string& message) : SerdeError("Protobuf decode error: " + message) {}
};

/**
 * Protobuf encode errors
 * Maps to SerdeError::ProtobufEncode variant
 */
class ProtobufEncodeError : public SerdeError {
public:
    explicit ProtobufEncodeError(const std::string& message) : SerdeError("Protobuf encode error: " + message) {}
};

/**
 * Protobuf reflection errors
 * Maps to SerdeError::ProtobufReflect variant
 */
class ProtobufReflectError : public SerdeError {
public:
    explicit ProtobufReflectError(const std::string& message) : SerdeError("Protobuf reflect error: " + message) {}
};

/**
 * Rule execution errors
 * Maps to SerdeError::Rule variant
 */
class RuleError : public SerdeError {
public:
    explicit RuleError(const std::string& message) : SerdeError("Rule failed: " + message) {}
};

/**
 * Rule condition errors
 * Maps to SerdeError::RuleCondition variant
 */
class RuleConditionError : public SerdeError {
private:
    std::shared_ptr<srclient::rest::model::Rule> rule_;
    
public:
    explicit RuleConditionError(std::shared_ptr<srclient::rest::model::Rule> rule, const std::string& message = "Rule condition failed");
    
    std::shared_ptr<srclient::rest::model::Rule> getRule() const { return rule_; }
};

/**
 * REST API errors
 * Maps to SerdeError::Rest variant
 */
class RestError : public SerdeError {
public:
    explicit RestError(const std::string& message) : SerdeError("REST error: " + message) {}
};

/**
 * General serialization errors
 * Maps to SerdeError::Serialization variant
 */
class SerializationError : public SerdeError {
public:
    explicit SerializationError(const std::string& message) : SerdeError("Serde error: " + message) {}
};

/**
 * Cryptographic/Tink errors
 * Maps to SerdeError::Tink variant
 */
class TinkError : public SerdeError {
public:
    explicit TinkError(const std::string& message) : SerdeError("Tink error: " + message) {}
};

/**
 * UUID parsing errors
 * Based on uuid parsing errors in serde.rs
 */
class UuidError : public SerdeError {
public:
    explicit UuidError(const std::string& message) : SerdeError("UUID error: " + message) {}
};

/**
 * IO errors
 * Based on IO operations in serde.rs
 */
class IoError : public SerdeError {
public:
    explicit IoError(const std::string& message) : SerdeError("IO error: " + message) {}
};

/**
 * Utility functions for error handling
 */
namespace error_utils {
    /**
     * Convert generic exception to appropriate SerdeError
     */
    SerdeError convertException(const std::exception& e);
    
    /**
     * Create serialization error from string message
     */
    SerializationError createSerializationError(const std::string& message);
    
    /**
     * Create rule error from rule type and message
     */
    RuleError createRuleError(const std::string& rule_type, const std::string& message);
    
    /**
     * Create UUID error from parsing failure
     */
    UuidError createUuidError(const std::string& invalid_uuid);
    
    /**
     * Create IO error from operation description
     */
    IoError createIoError(const std::string& operation, const std::string& details);
}

} // namespace srclient::serdes 