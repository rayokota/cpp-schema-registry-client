#include "srclient/serdes/protobuf/ProtobufTypes.h"

namespace srclient::serdes::protobuf {

// Implementation of ProtobufVariant methods

// Copy constructor implementation
ProtobufVariant::ProtobufVariant(const ProtobufVariant& other) : type_(other.type_) {
    switch (other.type_) {
        case ValueType::Message: {
            const auto& msg_ptr = std::get<std::unique_ptr<google::protobuf::Message>>(other.value);
            if (msg_ptr) {
                value = std::unique_ptr<google::protobuf::Message>(msg_ptr->New());
                std::get<std::unique_ptr<google::protobuf::Message>>(value)->CopyFrom(*msg_ptr);
            } else {
                value = std::unique_ptr<google::protobuf::Message>();
            }
            break;
        }
        case ValueType::List: {
            const auto& list = std::get<std::vector<ProtobufVariant>>(other.value);
            value = std::vector<ProtobufVariant>(list);
            break;
        }
        case ValueType::Map: {
            const auto& map = std::get<std::map<MapKey, ProtobufVariant>>(other.value);
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
ProtobufVariant& ProtobufVariant::operator=(const ProtobufVariant& other) {
    if (this != &other) {
        type_ = other.type_;
        switch (other.type_) {
            case ValueType::Message: {
                const auto& msg_ptr = std::get<std::unique_ptr<google::protobuf::Message>>(other.value);
                if (msg_ptr) {
                    value = std::unique_ptr<google::protobuf::Message>(msg_ptr->New());
                    std::get<std::unique_ptr<google::protobuf::Message>>(value)->CopyFrom(*msg_ptr);
                } else {
                    value = std::unique_ptr<google::protobuf::Message>();
                }
                break;
            }
            case ValueType::List: {
                const auto& list = std::get<std::vector<ProtobufVariant>>(other.value);
                value = std::vector<ProtobufVariant>(list);
                break;
            }
            case ValueType::Map: {
                const auto& map = std::get<std::map<MapKey, ProtobufVariant>>(other.value);
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

// Utility function implementation
google::protobuf::Message &asProtobuf(const SerdeValue &value) {
    if (value.getFormat() != SerdeFormat::Protobuf) {
        throw std::invalid_argument("SerdeValue is not Protobuf");
    }
    // Directly access the raw value to avoid slicing that occurs with
    // getValue<google::protobuf::Message>() The stored value is actually a
    // concrete protobuf message type, not the base class
    return *static_cast<google::protobuf::Message *>(
        const_cast<void *>(value.getRawObject()));
}

}  // namespace srclient::serdes::protobuf