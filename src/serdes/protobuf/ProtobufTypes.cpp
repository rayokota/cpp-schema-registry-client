#include "srclient/serdes/protobuf/ProtobufTypes.h"

// Explicit instantiation for the default protobuf message wrapper
template class srclient::serdes::protobuf::ProtobufValue<google::protobuf::Message>; 