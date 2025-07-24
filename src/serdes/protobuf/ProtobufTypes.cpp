#include "srclient/serdes/protobuf/ProtobufTypes.h"

namespace srclient::serdes::protobuf {

// Implementation of ProtobufVariant methods

// Copy constructor implementation
ProtobufVariant::ProtobufVariant(const ProtobufVariant &other)
    : type(other.type) {
    switch (other.type) {
        case ValueType::Message: {
            const auto &msg_ptr =
                std::get<std::unique_ptr<google::protobuf::Message>>(
                    other.value);
            if (msg_ptr) {
                value =
                    std::unique_ptr<google::protobuf::Message>(msg_ptr->New());
                std::get<std::unique_ptr<google::protobuf::Message>>(value)
                    ->CopyFrom(*msg_ptr);
            } else {
                value = std::unique_ptr<google::protobuf::Message>();
            }
            break;
        }
        case ValueType::List: {
            const auto &list =
                std::get<std::vector<ProtobufVariant>>(other.value);
            value = std::vector<ProtobufVariant>(list);
            break;
        }
        case ValueType::Map: {
            const auto &map =
                std::get<std::map<MapKey, ProtobufVariant>>(other.value);
            value = std::map<MapKey, ProtobufVariant>(map);
            break;
        }
        case ValueType::Bool: {
            value = std::get<bool>(other.value);
            break;
        }
        case ValueType::I32:
        case ValueType::EnumNumber: {
            value = std::get<int32_t>(other.value);
            break;
        }
        case ValueType::I64: {
            value = std::get<int64_t>(other.value);
            break;
        }
        case ValueType::U32: {
            value = std::get<uint32_t>(other.value);
            break;
        }
        case ValueType::U64: {
            value = std::get<uint64_t>(other.value);
            break;
        }
        case ValueType::F32: {
            value = std::get<float>(other.value);
            break;
        }
        case ValueType::F64: {
            value = std::get<double>(other.value);
            break;
        }
        case ValueType::String: {
            value = std::get<std::string>(other.value);
            break;
        }
        case ValueType::Bytes: {
            value = std::get<std::vector<uint8_t>>(other.value);
            break;
        }
    }
}

// Assignment operator implementation
ProtobufVariant &ProtobufVariant::operator=(const ProtobufVariant &other) {
    if (this != &other) {
        type = other.type;
        switch (other.type) {
            case ValueType::Message: {
                const auto &msg_ptr =
                    std::get<std::unique_ptr<google::protobuf::Message>>(
                        other.value);
                if (msg_ptr) {
                    value = std::unique_ptr<google::protobuf::Message>(
                        msg_ptr->New());
                    std::get<std::unique_ptr<google::protobuf::Message>>(value)
                        ->CopyFrom(*msg_ptr);
                } else {
                    value = std::unique_ptr<google::protobuf::Message>();
                }
                break;
            }
            case ValueType::List: {
                const auto &list =
                    std::get<std::vector<ProtobufVariant>>(other.value);
                value = std::vector<ProtobufVariant>(list);
                break;
            }
            case ValueType::Map: {
                const auto &map =
                    std::get<std::map<MapKey, ProtobufVariant>>(other.value);
                value = std::map<MapKey, ProtobufVariant>(map);
                break;
            }
            case ValueType::Bool: {
                value = std::get<bool>(other.value);
                break;
            }
            case ValueType::I32:
            case ValueType::EnumNumber: {
                value = std::get<int32_t>(other.value);
                break;
            }
            case ValueType::I64: {
                value = std::get<int64_t>(other.value);
                break;
            }
            case ValueType::U32: {
                value = std::get<uint32_t>(other.value);
                break;
            }
            case ValueType::U64: {
                value = std::get<uint64_t>(other.value);
                break;
            }
            case ValueType::F32: {
                value = std::get<float>(other.value);
                break;
            }
            case ValueType::F64: {
                value = std::get<double>(other.value);
                break;
            }
            case ValueType::String: {
                value = std::get<std::string>(other.value);
                break;
            }
            case ValueType::Bytes: {
                value = std::get<std::vector<uint8_t>>(other.value);
                break;
            }
        }
    }
    return *this;
}

// ProtobufValue implementation

ProtobufValue::ProtobufValue(ProtobufVariant value)
    : value_(std::move(value)) {}

const void *ProtobufValue::getRawObject() const {
    if (value_.type == ProtobufVariant::ValueType::Message) {
        return value_.get<std::unique_ptr<google::protobuf::Message>>().get();
    }
    return &value_;
}

void *ProtobufValue::getMutableRawObject() {
    if (value_.type == ProtobufVariant::ValueType::Message) {
        return value_.get<std::unique_ptr<google::protobuf::Message>>().get();
    }
    return &value_;
}

SerdeFormat ProtobufValue::getFormat() const { return SerdeFormat::Protobuf; }

const std::type_info &ProtobufValue::getType() const {
    if (value_.type == ProtobufVariant::ValueType::Message) {
        auto &msg_ptr =
            value_.get<std::unique_ptr<google::protobuf::Message>>();
        if (msg_ptr) {
            return typeid(*msg_ptr);
        }
    }
    return typeid(ProtobufVariant);
}

std::unique_ptr<SerdeValue> ProtobufValue::clone() const {
    return std::make_unique<ProtobufValue>(value_);
}

void ProtobufValue::moveFrom(SerdeObject &&other) {
    if (other.getFormat() == SerdeFormat::Protobuf) {
        if (auto *protobuf_value = dynamic_cast<ProtobufValue *>(&other)) {
            value_ = std::move(protobuf_value->value_);
        }
    }
}

bool ProtobufValue::asBool() const {
    if (value_.type == ProtobufVariant::ValueType::Bool) {
        return value_.get<bool>();
    }
    throw ProtobufError("Protobuf SerdeValue cannot be converted to bool");
}

std::string ProtobufValue::asString() const {
    switch (value_.type) {
        case ProtobufVariant::ValueType::String:
            return value_.get<std::string>();
        case ProtobufVariant::ValueType::Message: {
            auto &msg_ptr =
                value_.get<std::unique_ptr<google::protobuf::Message>>();
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

std::vector<uint8_t> ProtobufValue::asBytes() const {
    switch (value_.type) {
        case ProtobufVariant::ValueType::Bytes:
            return value_.get<std::vector<uint8_t>>();
        case ProtobufVariant::ValueType::Message: {
            auto &msg_ptr =
                value_.get<std::unique_ptr<google::protobuf::Message>>();
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

const ProtobufVariant &ProtobufValue::getProtobufVariant() const {
    return value_;
}

ProtobufVariant &ProtobufValue::getMutableProtobufVariant() { return value_; }

// ProtobufSchema implementation

ProtobufSchema::ProtobufSchema(const google::protobuf::FileDescriptor *schema)
    : schema_(schema) {}

const void *ProtobufSchema::getRawObject() const { return &schema_; }

void *ProtobufSchema::getMutableRawObject() {
    return const_cast<void *>(static_cast<const void *>(&schema_));
}

SerdeFormat ProtobufSchema::getFormat() const { return SerdeFormat::Protobuf; }

const std::type_info &ProtobufSchema::getType() const {
    return typeid(const google::protobuf::FileDescriptor *);
}

void ProtobufSchema::moveFrom(SerdeObject &&other) {
    if (auto *protobuf_other = dynamic_cast<ProtobufSchema *>(&other)) {
        schema_ = std::move(protobuf_other->schema_);
    } else {
        throw std::bad_cast();
    }
}

std::unique_ptr<SerdeSchema> ProtobufSchema::clone() const {
    return std::make_unique<ProtobufSchema>(schema_);
}

const google::protobuf::FileDescriptor *ProtobufSchema::getProtobufSchema()
    const {
    return schema_;
}

// Utility function implementation
ProtobufVariant &asProtobuf(const SerdeValue &value) {
    if (value.getFormat() != SerdeFormat::Protobuf) {
        throw std::invalid_argument("SerdeValue is not Protobuf");
    }
    // Cast to ProtobufValue and return its internal ProtobufVariant
    auto &protobuf_value = static_cast<const ProtobufValue &>(value);
    return const_cast<ProtobufVariant &>(protobuf_value.getProtobufVariant());
}

}  // namespace srclient::serdes::protobuf