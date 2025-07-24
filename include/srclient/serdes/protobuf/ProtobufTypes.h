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
using MapKey = std::variant<std::string, int32_t, int64_t, uint32_t, uint64_t, bool>;

/**
 * C++ variant-based structure representing protobuf values
 * Ported from Rust Value enum to use std::variant
 */
struct ProtobufVariant {
    enum class ValueType {
        Bool, I32, I64, U32, U64, F32, F64, String, Bytes, EnumNumber, Message, List, Map
    };
    
    using ValueVariant = std::variant<
        bool,                                                    // Bool
        int32_t,                                                // I32
        int64_t,                                                // I64
        uint32_t,                                               // U32
        uint64_t,                                               // U64
        float,                                                  // F32
        double,                                                 // F64
        std::string,                                            // String
        std::vector<uint8_t>,                                   // Bytes
        std::unique_ptr<google::protobuf::Message>,             // Message
        std::vector<ProtobufVariant>,                             // List
        std::map<MapKey, ProtobufVariant>                         // Map
    >;
    
    ValueVariant value;
    ValueType type_;
    
    // Constructors for each type
    ProtobufVariant(bool v) : value(v), type_(ValueType::Bool) {}
    ProtobufVariant(int32_t v, ValueType type = ValueType::I32) : value(v), type_(type) {}
    ProtobufVariant(int64_t v) : value(v), type_(ValueType::I64) {}
    ProtobufVariant(uint32_t v) : value(v), type_(ValueType::U32) {}
    ProtobufVariant(uint64_t v) : value(v), type_(ValueType::U64) {}
    ProtobufVariant(float v) : value(v), type_(ValueType::F32) {}
    ProtobufVariant(double v) : value(v), type_(ValueType::F64) {}
    ProtobufVariant(const std::string& v) : value(v), type_(ValueType::String) {}
    ProtobufVariant(const std::vector<uint8_t>& v) : value(v), type_(ValueType::Bytes) {}
    ProtobufVariant(std::unique_ptr<google::protobuf::Message> v) : value(std::move(v)), type_(ValueType::Message) {}
    ProtobufVariant(const std::vector<ProtobufVariant>& v) : value(v), type_(ValueType::List) {}
    ProtobufVariant(const std::map<MapKey, ProtobufVariant>& v) : value(v), type_(ValueType::Map) {}
    
    // Special constructor for enum values
    static ProtobufVariant createEnum(int32_t value) {
        return ProtobufVariant(value, ValueType::EnumNumber);
    }
    
    // Copy constructor and assignment operator
    ProtobufVariant(const ProtobufVariant& other);
    ProtobufVariant& operator=(const ProtobufVariant& other);
    
    // Move constructor and assignment operator
    ProtobufVariant(ProtobufVariant&& other) = default;
    ProtobufVariant& operator=(ProtobufVariant&& other) = default;
    
    // Visitor helper methods
    template<typename T>
    bool is() const { return std::holds_alternative<T>(value); }
    
    template<typename T>
    const T& get() const { return std::get<T>(value); }
    
    template<typename T>
    T& get() { return std::get<T>(value); }
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
    explicit ProtobufValue(ProtobufVariant value)
        : value_(std::move(value)) {}

    // SerdeValue interface implementation
    const void *getRawObject() const override {
        if (value_.type_ == ProtobufVariant::ValueType::Message) {
            return value_.get<std::unique_ptr<google::protobuf::Message>>().get();
        }
        return &value_;
    }
    
    void *getMutableRawObject() override {
        if (value_.type_ == ProtobufVariant::ValueType::Message) {
            return value_.get<std::unique_ptr<google::protobuf::Message>>().get();
        }
        return &value_;
    }
    
    SerdeFormat getFormat() const override { return SerdeFormat::Protobuf; }
    
    const std::type_info &getType() const override {
        if (value_.type_ == ProtobufVariant::ValueType::Message) {
            auto& msg_ptr = value_.get<std::unique_ptr<google::protobuf::Message>>();
            if (msg_ptr) {
                return typeid(*msg_ptr);
            }
        }
        return typeid(ProtobufVariant);
    }

    std::unique_ptr<SerdeValue> clone() const override {
        return std::make_unique<ProtobufValue>(value_);
    }

    void moveFrom(SerdeObject &&other) override {
        if (other.getFormat() == SerdeFormat::Protobuf) {
            if (auto *protobuf_value = dynamic_cast<ProtobufValue *>(&other)) {
                value_ = std::move(protobuf_value->value_);
            }
        }
    }

    // Value extraction methods
    bool asBool() const override {
        if (value_.type_ == ProtobufVariant::ValueType::Bool) {
            return value_.get<bool>();
        }
        throw ProtobufError("Protobuf SerdeValue cannot be converted to bool");
    }

    std::string asString() const override {
        switch (value_.type_) {
            case ProtobufVariant::ValueType::String:
                return value_.get<std::string>();
            case ProtobufVariant::ValueType::Message: {
                auto& msg_ptr = value_.get<std::unique_ptr<google::protobuf::Message>>();
                if (msg_ptr) {
                    std::string output;
                    google::protobuf::util::MessageToJsonString(*msg_ptr, &output)
                        .IgnoreError();
                    return output;
                }
                break;
            }
            default:
                break;
        }
        throw ProtobufError("Protobuf SerdeValue cannot be converted to string");
    }

    std::vector<uint8_t> asBytes() const override {
        switch (value_.type_) {
            case ProtobufVariant::ValueType::Bytes:
                return value_.get<std::vector<uint8_t>>();
            case ProtobufVariant::ValueType::Message: {
                auto& msg_ptr = value_.get<std::unique_ptr<google::protobuf::Message>>();
                if (msg_ptr) {
                    std::string binary;
                    if (!msg_ptr->SerializeToString(&binary)) {
                        throw ProtobufError(
                            "Failed to serialize Protobuf message to bytes");
                    }
                    return std::vector<uint8_t>(binary.begin(), binary.end());
                }
                break;
            }
            default:
                break;
        }
        throw ProtobufError("Protobuf SerdeValue cannot be converted to bytes");
    }

    // Direct access to ProtobufVariant
    const ProtobufVariant& getProtobufVariant() const { return value_; }
    ProtobufVariant& getMutableProtobufVariant() { return value_; }
};

/**
 * Protobuf Schema implementation
 */
class ProtobufSchema : public SerdeSchema {
  private:
    const google::protobuf::FileDescriptor *schema_;

  public:
    explicit ProtobufSchema(const google::protobuf::FileDescriptor *schema)
        : schema_(schema) {}

    // SerdeObject interface methods
    const void *getRawObject() const override { return &schema_; }
    void *getMutableRawObject() override {
        return const_cast<void *>(static_cast<const void *>(&schema_));
    }
    SerdeFormat getFormat() const override { return SerdeFormat::Protobuf; }
    const std::type_info &getType() const override {
        return typeid(const google::protobuf::FileDescriptor *);
    }

    void moveFrom(SerdeObject &&other) override {
        if (auto *protobuf_other = dynamic_cast<ProtobufSchema *>(&other)) {
            schema_ = std::move(protobuf_other->schema_);
        } else {
            throw std::bad_cast();
        }
    }

    std::unique_ptr<SerdeSchema> clone() const override {
        return std::make_unique<ProtobufSchema>(schema_);
    }

    // Direct access to Protobuf schema
    const google::protobuf::FileDescriptor *getProtobufSchema() const {
        return schema_;
    }
};

// Helper functions for creating Protobuf SerdeValue instances
inline std::unique_ptr<SerdeValue> makeProtobufValue(ProtobufVariant value) {
    return std::make_unique<ProtobufValue>(std::move(value));
}

// Overload for convenience – takes a reference and makes a copy
inline std::unique_ptr<SerdeValue> makeProtobufValue(const ProtobufVariant &value) {
    return std::make_unique<ProtobufValue>(value);
}

// Convenience overloads for protobuf messages
template <typename T>
inline std::unique_ptr<SerdeValue> makeProtobufValue(std::unique_ptr<T> value) {
    static_assert(std::is_base_of_v<google::protobuf::Message, T>,
                  "T must derive from google::protobuf::Message");
    return std::make_unique<ProtobufValue>(ProtobufVariant(std::move(value)));
}

template <typename T>
inline std::unique_ptr<SerdeValue> makeProtobufValue(const T &value) {
    static_assert(std::is_base_of_v<google::protobuf::Message, T>,
                  "T must derive from google::protobuf::Message");
    std::unique_ptr<T> owned_value(static_cast<T *>(value.New()));
    owned_value->CopyFrom(value);
    return std::make_unique<ProtobufValue>(ProtobufVariant(std::move(owned_value)));
}

// Helper function for creating Protobuf SerdeSchema instances
inline std::unique_ptr<SerdeSchema> makeProtobufSchema(
    const google::protobuf::FileDescriptor *file_descriptor) {
    return std::make_unique<ProtobufSchema>(file_descriptor);
}

// Utility functions for Protobuf value and schema extraction
google::protobuf::Message &asProtobuf(const SerdeValue &value);

inline const google::protobuf::FileDescriptor *asProtobufSchema(
    const SerdeSchema &schema) {
    if (schema.getFormat() != SerdeFormat::Protobuf) {
        throw std::invalid_argument("Schema is not a Protobuf schema");
    }
    return schema.getSchema<const google::protobuf::FileDescriptor *>();
}

}  // namespace srclient::serdes::protobuf