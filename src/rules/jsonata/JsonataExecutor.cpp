#include "schemaregistry/rules/jsonata/JsonataExecutor.h"

#include <regex>

#include "schemaregistry/serdes/RuleRegistry.h"
#include "schemaregistry/serdes/Serde.h"

namespace schemaregistry::rules::jsonata {

using namespace schemaregistry::serdes;

JsonataExecutor::JsonataExecutor() {}

// Implement the required getType method
std::string JsonataExecutor::getType() const { return "JSONATA"; }

std::unique_ptr<SerdeValue> JsonataExecutor::transform(
    schemaregistry::serdes::RuleContext &ctx, const SerdeValue &msg) {
    return nullptr;
}

void JsonataExecutor::registerExecutor() {
    // Register this executor with the global rule registry
    // This matches the Rust version:
    // crate::serdes::rule_registry::register_rule_executor(JsonataExecutor::new());
    global_registry::registerRuleExecutor(std::make_shared<JsonataExecutor>());
}

}  // namespace schemaregistry::rules::jsonata