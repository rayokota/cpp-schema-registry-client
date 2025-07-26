#include "srclient/serdes/json/JsonTypes.h"

#include <algorithm>

#include "absl/strings/escaping.h"

namespace srclient::serdes::json {

// Base64 decoding utilities for JSON using absl
namespace {
std::vector<uint8_t> base64_decode(const std::string &encoded_string) {
    std::string decoded;
    if (!absl::Base64Unescape(encoded_string, &decoded)) {
        // Return empty vector on decode failure
        return std::vector<uint8_t>();
    }
    return std::vector<uint8_t>(decoded.begin(), decoded.end());
}
}  // namespace

// Implementation for JsonValue methods
bool JsonValue::asBool() const {
    if (value_.is_bool()) {
        return value_.as<bool>();
    }
    // Default to true for non-boolean types (matching Rust behavior)
    return true;
}

std::string JsonValue::asString() const {
    if (value_.is_string()) {
        return value_.as<std::string>();
    }
    // Return empty string for non-string types (matching Rust behavior)
    return "";
}

std::vector<uint8_t> JsonValue::asBytes() const {
    if (value_.is_string()) {
        std::string str_value = value_.as<std::string>();
        // Attempt to decode as base64
        try {
            return base64_decode(str_value);
        } catch (...) {
            // If base64 decode fails, return empty vector
            return std::vector<uint8_t>();
        }
    }
    // Return empty vector for non-string types (matching Rust behavior)
    return std::vector<uint8_t>();
}

std::unique_ptr<SerdeValue> makeJsonValue(const jsoncons::ojson &value) {
    return std::make_unique<JsonValue>(value);
}

std::unique_ptr<SerdeValue> makeJsonValue(jsoncons::ojson &&value) {
    return std::make_unique<JsonValue>(std::move(value));
}

// Utility functions for JSON value and schema extraction
jsoncons::ojson asJson(const SerdeValue &value) {
    if (value.getFormat() != SerdeFormat::Json) {
        throw std::invalid_argument("SerdeValue is not JSON");
    }
    return value.getValue<jsoncons::ojson>();
}

}  // namespace srclient::serdes::json