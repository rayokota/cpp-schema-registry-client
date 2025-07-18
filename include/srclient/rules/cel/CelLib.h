#pragma once

#include "runtime/runtime.h"
#include "runtime/runtime_builder.h"
#include "runtime/function_registry.h"
#include "runtime/runtime_options.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace srclient::rules::cel {

/**
 * Utility class for CEL library functions and context setup
 * Ported from cel_lib.rs
 */
class CelLib {
public:
    /**
     * Create a default CEL runtime (simplified version without custom functions for now)
     */
    static absl::StatusOr<std::unique_ptr<const ::cel::Runtime>> createDefaultRuntime();
    
private:
    // Validation functions that can be used in CEL expressions
    static bool isHostname(absl::string_view hostname);
    static bool isEmail(absl::string_view email);
    static bool isIpv4(absl::string_view ip);
    static bool isIpv6(absl::string_view ip);
    static bool isUri(absl::string_view uri);
    static bool isUriRef(absl::string_view uri_ref);
    static bool isUuid(absl::string_view uuid);
};

} // namespace srclient::rules::cel 