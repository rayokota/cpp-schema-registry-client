#include "srclient/serdes/SerdeError.h"

#include "srclient/serdes/SerdeTypes.h"

namespace srclient::serdes {

// RuleConditionError implementation
RuleConditionError::RuleConditionError(
    std::shared_ptr<srclient::rest::model::Rule> rule,
    const std::string &message)
    : SerdeError(message.empty() ? "Rule condition failed" : message),
      rule_(rule) {}

namespace error_utils {

SerializationError createSerializationError(const std::string &message) {
    return SerializationError(message);
}

RuleError createRuleError(const std::string &rule_type,
                          const std::string &message) {
    return RuleError("Rule '" + rule_type + "' failed: " + message);
}

UuidError createUuidError(const std::string &invalid_uuid) {
    return UuidError("Invalid UUID format: " + invalid_uuid);
}

IoError createIoError(const std::string &operation,
                      const std::string &details) {
    return IoError("IO operation '" + operation + "' failed: " + details);
}

}  // namespace error_utils

}  // namespace srclient::serdes