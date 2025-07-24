#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>

#include <any>
#include <functional>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <variant>
#include <vector>

#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"

namespace srclient::serdes::protobuf {

// Forward declaration
struct ProtobufVariant;

// C++ variant representing protobuf values - type alias for map keys
using MapKey =
    std::variant<std::string, int32_t, int64_t, uint32_t, uint64_t, bool>;

/**
 * C++ variant-based structure representing protobuf values
 * Ported from Rust Value enum to use std::variant
 */
struct ProtobufVariant {
    enum class ValueType {
        Bool,
        I32,
        I64,
        U32,
        U64,
        F32,
        F64,
        String,
        Bytes,
        EnumNumber,
        Message,
        List,
        Map
    };

    using ValueVariant =
        std::variant<bool,                                        // Bool
                     int32_t,                                     // I32
                     int64_t,                                     // I64
                     uint32_t,                                    // U32
                     uint64_t,                                    // U64
                     float,                                       // F32
                     double,                                      // F64
                     std::string,                                 // String
                     std::vector<uint8_t>,                        // Bytes
                     std::unique_ptr<google::protobuf::Message>,  // Message
                     std::vector<ProtobufVariant>,                // List
                     std::map<MapKey, ProtobufVariant>            // Map
                     >;

    ValueVariant value;
    ValueType type_;

    // Constructors for each type
    ProtobufVariant(bool v) : value(v), type_(ValueType::Bool) {}
    ProtobufVariant(int32_t v, ValueType type = ValueType::I32)
        : value(v), type_(type) {}
    ProtobufVariant(int64_t v) : value(v), type_(ValueType::I64) {}
    ProtobufVariant(uint32_t v) : value(v), type_(ValueType::U32) {}
    ProtobufVariant(uint64_t v) : value(v), type_(ValueType::U64) {}
    ProtobufVariant(float v) : value(v), type_(ValueType::F32) {}
    ProtobufVariant(double v) : value(v), type_(ValueType::F64) {}
    ProtobufVariant(const std::string &v)
        : value(v), type_(ValueType::String) {}
    ProtobufVariant(const std::vector<uint8_t> &v)
        : value(v), type_(ValueType::Bytes) {}
    ProtobufVariant(std::unique_ptr<google::protobuf::Message> v)
        : value(std::move(v)), type_(ValueType::Message) {}
    ProtobufVariant(const std::vector<ProtobufVariant> &v)
        : value(v), type_(ValueType::List) {}
    ProtobufVariant(const std::map<MapKey, ProtobufVariant> &v)
        : value(v), type_(ValueType::Map) {}

    // Special constructor for enum values
    static ProtobufVariant createEnum(int32_t value) {
        return ProtobufVariant(value, ValueType::EnumNumber);
    }

    // Copy constructor and assignment operator
    ProtobufVariant(const ProtobufVariant &other);
    ProtobufVariant &operator=(const ProtobufVariant &other);

    // Move constructor and assignment operator
    ProtobufVariant(ProtobufVariant &&other) = default;
    ProtobufVariant &operator=(ProtobufVariant &&other) = default;

    // Visitor helper methods
    template <typename T>
    bool is() const {
        return std::holds_alternative<T>(value);
    }

    template <typename T>
    const T &get() const {
        return std::get<T>(value);
    }

    template <typename T>
    T &get() {
        return std::get<T>(value);
    }
};

/**
 * Protobuf errors
 */
class ProtobufError : public SerdeError {
  public:
    explicit ProtobufError(const std::string &message)
        : SerdeError("Protobuf error: " + message) {}
};

/**
 * Protobuf reflection errors
 * Maps to SerdeError::ProtobufReflect variant
 */
class ProtobufReflectError : public SerdeError {
  public:
    explicit ProtobufReflectError(const std::string &message)
        : SerdeError("Protobuf reflect error: " + message) {}
};

/**
 * Protobuf implementation of SerdeValue
 */
class ProtobufValue : public SerdeValue {
  private:
    ProtobufVariant value_;

  public:
    explicit ProtobufValue(ProtobufVariant value);

    // SerdeValue interface implementation
    const void *getRawObject() const override;
    void *getMutableRawObject() override;
    SerdeFormat getFormat() const override;
    const std::type_info &getType() const override;
    std::unique_ptr<SerdeValue> clone() const override;
    void moveFrom(SerdeObject &&other) override;

    // Value extraction methods
    bool asBool() const override;
    std::string asString() const override;
    std::vector<uint8_t> asBytes() const override;

    // Direct access to ProtobufVariant
    const ProtobufVariant &getProtobufVariant() const;
    ProtobufVariant &getMutableProtobufVariant();
};

/**
 * Protobuf Schema implementation
 */
class ProtobufSchema : public SerdeSchema {
  private:
    const google::protobuf::FileDescriptor *schema_;

  public:
    explicit ProtobufSchema(const google::protobuf::FileDescriptor *schema);

    // SerdeObject interface methods
    const void *getRawObject() const override;
    void *getMutableRawObject() override;
    SerdeFormat getFormat() const override;
    const std::type_info &getType() const override;
    void moveFrom(SerdeObject &&other) override;
    std::unique_ptr<SerdeSchema> clone() const override;

    // Direct access to Protobuf schema
    const google::protobuf::FileDescriptor *getProtobufSchema() const;
};

// Helper functions for creating Protobuf SerdeValue instances
inline std::unique_ptr<SerdeValue> makeProtobufValue(ProtobufVariant value) {
    return std::make_unique<ProtobufValue>(std::move(value));
}

// Helper function for creating Protobuf SerdeSchema instances
inline std::unique_ptr<SerdeSchema> makeProtobufSchema(
    const google::protobuf::FileDescriptor *file_descriptor) {
    return std::make_unique<ProtobufSchema>(file_descriptor);
}

// Utility functions for Protobuf value and schema extraction
ProtobufVariant &asProtobuf(const SerdeValue &value);

inline const google::protobuf::FileDescriptor *asProtobufSchema(
    const SerdeSchema &schema) {
    if (schema.getFormat() != SerdeFormat::Protobuf) {
        throw std::invalid_argument("Schema is not a Protobuf schema");
    }
    return schema.getSchema<const google::protobuf::FileDescriptor *>();
}

}  // namespace srclient::serdes::protobuf