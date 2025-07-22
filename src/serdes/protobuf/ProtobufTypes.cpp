#include "srclient/serdes/protobuf/ProtobufTypes.h"

namespace srclient::serdes::protobuf {

// Utility function implementation
google::protobuf::Message& asProtobuf(const SerdeValue& value) {
    if (value.getFormat() != SerdeFormat::Protobuf) {
        throw std::invalid_argument("SerdeValue is not Protobuf");
    }
    // Directly access the raw value to avoid slicing that occurs with getValue<google::protobuf::Message>()
    // The stored value is actually a concrete protobuf message type, not the base class
    return *static_cast<google::protobuf::Message*>(const_cast<void*>(value.getRawValue()));
}

} // namespace srclient::serdes::protobuf

// Explicit instantiation for the default protobuf message wrapper
template class srclient::serdes::protobuf::ProtobufValue<google::protobuf::Message>; 