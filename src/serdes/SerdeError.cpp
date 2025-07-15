#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"

namespace srclient::serdes {

// RuleConditionError implementation
RuleConditionError::RuleConditionError(std::shared_ptr<srclient::rest::model::Rule> rule, const std::string& message)
    : SerdeError(message.empty() ? "Rule condition failed" : message)
    , rule_(rule) {}

namespace error_utils {

SerdeError convertException(const std::exception& e) {
    const std::string msg = e.what();
    
    // Try to identify the type of exception based on the message
    if (msg.find("avro") != std::string::npos || msg.find("Avro") != std::string::npos) {
        return AvroError(msg);
    } else if (msg.find("json") != std::string::npos || msg.find("JSON") != std::string::npos) {
        return JsonError(msg);
    } else if (msg.find("protobuf") != std::string::npos || msg.find("Protobuf") != std::string::npos) {
        return ProtobufDecodeError(msg);
    } else if (msg.find("rule") != std::string::npos || msg.find("Rule") != std::string::npos) {
        return RuleError(msg);
    } else if (msg.find("REST") != std::string::npos || msg.find("HTTP") != std::string::npos) {
        return RestError(msg);
    } else if (msg.find("UUID") != std::string::npos || msg.find("uuid") != std::string::npos) {
        return UuidError(msg);
    } else if (msg.find("IO") != std::string::npos || msg.find("file") != std::string::npos) {
        return IoError(msg);
    }
    
    // Default to general serialization error
    return SerializationError(msg);
}

SerializationError createSerializationError(const std::string& message) {
    return SerializationError(message);
}

RuleError createRuleError(const std::string& rule_type, const std::string& message) {
    return RuleError("Rule '" + rule_type + "' failed: " + message);
}

UuidError createUuidError(const std::string& invalid_uuid) {
    return UuidError("Invalid UUID format: " + invalid_uuid);
}

IoError createIoError(const std::string& operation, const std::string& details) {
    return IoError("IO operation '" + operation + "' failed: " + details);
}

} // namespace error_utils

} // namespace srclient::serdes 