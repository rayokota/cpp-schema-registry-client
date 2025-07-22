#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>

#include <any>
#include <functional>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <type_traits>

#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"

namespace srclient::serdes::protobuf {

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
template <typename T>
class ProtobufValue : public SerdeValue {
    static_assert(std::is_base_of_v<google::protobuf::Message, T>,
                  "T must derive from google::protobuf::Message");

  private:
    std::unique_ptr<T> value_;

  public:
    explicit ProtobufValue(std::unique_ptr<T> value)
        : value_(std::move(value)) {}

    // SerdeValue interface implementation
    const void *getRawObject() const override { return value_.get(); }
    void *getMutableRawObject() override { return value_.get(); }
    SerdeFormat getFormat() const override { return SerdeFormat::Protobuf; }
    const std::type_info &getType() const override { return typeid(T); }

    std::unique_ptr<SerdeValue> clone() const override {
        std::unique_ptr<T> cloned_msg(static_cast<T *>(value_->New()));
        cloned_msg->CopyFrom(*value_);
        return std::make_unique<ProtobufValue<T>>(std::move(cloned_msg));
    }

    void moveFrom(SerdeObject &&other) override {
        if (other.getFormat() == SerdeFormat::Protobuf) {
            auto *other_msg = static_cast<google::protobuf::Message *>(
                other.getMutableRawObject());
            value_->CopyFrom(*other_msg);
        }
    }

    // Value extraction methods
    bool asBool() const override {
        throw ProtobufError("Protobuf SerdeValue cannot be converted to bool");
    }

    std::string asString() const override {
        std::string output;
        google::protobuf::util::MessageToJsonString(*value_, &output)
            .IgnoreError();
        return output;
    }

    std::vector<uint8_t> asBytes() const override {
        std::string binary;
        if (!value_->SerializeToString(&binary)) {
            throw ProtobufError(
                "Failed to serialize Protobuf message to bytes");
        }
        return std::vector<uint8_t>(binary.begin(), binary.end());
    }
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
template <typename T>
inline std::unique_ptr<SerdeValue> makeProtobufValue(std::unique_ptr<T> value) {
    static_assert(std::is_base_of_v<google::protobuf::Message, T>,
                  "T must derive from google::protobuf::Message");
    auto protobuf_value = std::make_unique<ProtobufValue<T>>(std::move(value));
    return std::unique_ptr<SerdeValue>(
        static_cast<SerdeValue *>(protobuf_value.release()));
}

// Overload for convenience – takes a reference and makes a copy
template <typename T>
inline std::unique_ptr<SerdeValue> makeProtobufValue(T &value) {
    static_assert(std::is_base_of_v<google::protobuf::Message, T>,
                  "T must derive from google::protobuf::Message");
    std::unique_ptr<T> owned_value(static_cast<T *>(value.New()));
    owned_value->CopyFrom(value);
    auto protobuf_value =
        std::make_unique<ProtobufValue<T>>(std::move(owned_value));
    return std::unique_ptr<SerdeValue>(
        static_cast<SerdeValue *>(protobuf_value.release()));
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