#pragma once

#include "srclient/rules/encryption/EncryptionExecutor.h"
#include "srclient/rules/encryption/FieldEncryptionExecutor.h"

namespace srclient::rules::encryption {

/**
 * Convenience functions for registering all encryption rule executors
 * with the global rule registry
 */
namespace registration {

/**
 * Register all encryption rule executors (both EncryptionExecutor and
 * FieldEncryptionExecutor) Call this function during application initialization
 * to make encryption rules available for use.
 */
void registerAllEncryptionExecutors() {
    EncryptionExecutor::registerExecutor();
    FieldEncryptionExecutor::registerExecutor();
}

/**
 * Register only the main encryption executor (for message-level
 * transformations)
 */
void registerEncryptionExecutor() { EncryptionExecutor::registerExecutor(); }

/**
 * Register only the encryption field executor (for field-level transformations)
 */
void registerFieldEncryptionExecutor() {
    FieldEncryptionExecutor::registerExecutor();
}

}  // namespace registration

}  // namespace srclient::rules::encryption