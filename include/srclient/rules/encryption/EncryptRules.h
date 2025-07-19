#pragma once

#include "srclient/rules/encryption/EncryptExecutor.h"

namespace srclient::rules::encryption {

/**
 * Convenience functions for registering all encryption rule executors
 * with the global rule registry
 */
namespace registration {
    
    /**
     * Register all encryption rule executors (both EncryptionExecutor and FieldEncryptionExecutor)
     * Call this function during application initialization to make encryption
     * rules available for use.
     */
    template<typename T = DekRegistryClient>
    void registerAllEncryptionExecutors() {
        EncryptionExecutor<T>::registerExecutor();
        FieldEncryptionExecutor<T>::registerExecutor();
    }
    
    /**
     * Register only the main encryption executor (for message-level transformations)
     */
    template<typename T = DekRegistryClient>
    void registerEncryptionExecutor() {
        EncryptionExecutor<T>::registerExecutor();
    }
    
    /**
     * Register only the encryption field executor (for field-level transformations)
     */
    template<typename T = DekRegistryClient>
    void registerFieldEncryptionExecutor() {
        FieldEncryptionExecutor<T>::registerExecutor();
    }
    
} // namespace registration

} // namespace srclient::rules::encryption