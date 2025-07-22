#include "srclient/rules/cel/CelRules.h"

namespace srclient::rules::cel::registration {

void registerAllCelExecutors() {
    registerCelExecutor();
    registerCelFieldExecutor();
}

void registerCelExecutor() { CelExecutor::registerExecutor(); }

void registerCelFieldExecutor() { CelFieldExecutor::registerExecutor(); }

}  // namespace srclient::rules::cel::registration