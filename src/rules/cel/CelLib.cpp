#include "srclient/rules/cel/CelLib.h"
#include "runtime/runtime_builder_factory.h"
#include "runtime/standard/string_functions.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "google/protobuf/descriptor.h"
#include <regex>
#include <sstream>
#include <vector>

namespace srclient::rules::cel {

absl::StatusOr<std::unique_ptr<const ::cel::Runtime>> CelLib::createDefaultRuntime() {
    ::cel::RuntimeOptions options;
    auto builder_result = ::cel::CreateRuntimeBuilder(
        google::protobuf::DescriptorPool::generated_pool(), options);
    
    if (!builder_result.ok()) {
        return builder_result.status();
    }
    
    auto builder = std::move(builder_result.value());
    
    // Register standard functions only for now
    auto& function_registry = builder.function_registry();
    auto status = ::cel::RegisterStringFunctions(function_registry, options);
    if (!status.ok()) {
        return status;
    }
    
    // Return the const runtime as-is
    return std::move(builder).Build();
}

bool CelLib::isHostname(absl::string_view hostname) {
    if (hostname.empty() || hostname.length() > 253) {
        return false;
    }
    
    // Basic hostname validation - should not start or end with dot or hyphen
    if (hostname.front() == '.' || hostname.back() == '.' ||
        hostname.front() == '-' || hostname.back() == '-') {
        return false;
    }
    
    // Simple regex for hostname validation
    // This is a simplified version - production code should use a more robust library
    std::regex hostname_regex(R"(^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$)");
    return std::regex_match(std::string(hostname), hostname_regex);
}

bool CelLib::isEmail(absl::string_view email) {
    // Basic email validation - production code should use a more robust library
    std::regex email_regex(R"(^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$)");
    return std::regex_match(std::string(email), email_regex);
}

bool CelLib::isIpv4(absl::string_view ip) {
    std::regex ipv4_regex(R"(^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$)");
    return std::regex_match(std::string(ip), ipv4_regex);
}

bool CelLib::isIpv6(absl::string_view ip) {
    // Basic IPv6 validation - this is simplified
    std::regex ipv6_regex(R"(^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|^::1$|^::$)");
    return std::regex_match(std::string(ip), ipv6_regex);
}

bool CelLib::isUri(absl::string_view uri) {
    // Basic URI validation - should have scheme
    std::regex uri_regex(R"(^[a-zA-Z][a-zA-Z0-9+.-]*:.+)");
    return std::regex_match(std::string(uri), uri_regex);
}

bool CelLib::isUriRef(absl::string_view uri_ref) {
    // URI reference can be relative or absolute
    return !uri_ref.empty() && uri_ref.find('\0') == absl::string_view::npos;
}

bool CelLib::isUuid(absl::string_view uuid) {
    // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    std::regex uuid_regex(R"(^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$)");
    return std::regex_match(std::string(uuid), uuid_regex);
}

} // namespace srclient::rules::cel 