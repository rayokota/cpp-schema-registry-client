#include "schemaregistry/serdes/json/JsonTypes.h"

#include <algorithm>
#include <cctype>

namespace schemaregistry::serdes::json {

// Factory registration for JSON format
namespace {
// Simple base64 implementation - avoiding external dependencies
std::string base64_encode(const std::vector<uint8_t> &bytes) {
    const std::string base64_chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    std::string result;
    size_t i = 0;
    unsigned char char_array_3[3];
    unsigned char char_array_4[4];

    for (size_t idx = 0; idx < bytes.size(); idx++) {
        char_array_3[i++] = bytes[idx];
        if (i == 3) {
            char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
            char_array_4[1] = ((char_array_3[0] & 0x03) << 4) +
                              ((char_array_3[1] & 0xf0) >> 4);
            char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) +
                              ((char_array_3[2] & 0xc0) >> 6);
            char_array_4[3] = char_array_3[2] & 0x3f;

            for (i = 0; i < 4; i++) result += base64_chars[char_array_4[i]];
            i = 0;
        }
    }

    if (i) {
        for (size_t j = 0; j < i; j++) char_array_3[j] = char_array_3[j];

        char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
        char_array_4[1] =
            ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
        char_array_4[2] =
            ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);

        for (size_t j = 0; j < i + 1; j++)
            result += base64_chars[char_array_4[j]];

        while (i++ < 3) result += '=';
    }

    return result;
}

std::vector<uint8_t> base64_decode(const std::string &encoded_string) {
    const std::string base64_chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    auto is_base64 = [](unsigned char c) {
        return (isalnum(c) || (c == '+') || (c == '/'));
    };

    size_t in_len = encoded_string.size();
    size_t i = 0;
    size_t in = 0;
    unsigned char char_array_4[4], char_array_3[3];
    std::vector<uint8_t> result;

    while (in_len-- && (encoded_string[in] != '=') &&
           is_base64(encoded_string[in])) {
        char_array_4[i++] = encoded_string[in];
        in++;
        if (i == 4) {
            for (i = 0; i < 4; i++)
                char_array_4[i] = base64_chars.find(char_array_4[i]) & 0xff;

            char_array_3[0] =
                (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
            char_array_3[1] = ((char_array_4[1] & 0xf) << 4) +
                              ((char_array_4[2] & 0x3c) >> 2);
            char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

            for (i = 0; (i < 3); i++) result.push_back(char_array_3[i]);
            i = 0;
        }
    }

    if (i) {
        for (size_t j = 0; j < i; j++)
            char_array_4[j] = base64_chars.find(char_array_4[j]) & 0xff;

        char_array_3[0] =
            (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
        char_array_3[1] =
            ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);

        for (size_t j = 0; (j < i - 1); j++) result.push_back(char_array_3[j]);
    }

    return result;
}

// Static initialization to register JSON factories
bool json_factories_registered = []() {
    // Register string factory
    SerdeValueFactory::registerStringFactory(
        SerdeFormat::Json,
        [](const std::string &value) -> std::unique_ptr<SerdeValue> {
            nlohmann::json json_value = value;
            return std::make_unique<JsonValue>(json_value);
        });

    // Register bytes factory
    SerdeValueFactory::registerBytesFactory(
        SerdeFormat::Json,
        [](const std::vector<uint8_t> &value) -> std::unique_ptr<SerdeValue> {
            // For JSON, encode bytes as base64 string
            std::string base64_value = base64_encode(value);
            nlohmann::json json_value = base64_value;
            return std::make_unique<JsonValue>(json_value);
        });

    // Register JSON factory
    SerdeValueFactory::registerJsonFactory(
        SerdeFormat::Json,
        [](const nlohmann::json &value) -> std::unique_ptr<SerdeValue> {
            return std::make_unique<JsonValue>(value);
        });

    return true;
}();
}  // namespace

// Implementation for JsonValue methods
bool JsonValue::asBool() const {
    if (value_.is_boolean()) {
        return value_.get<bool>();
    }
    // Default to true for non-boolean types (matching Rust behavior)
    return true;
}

std::string JsonValue::asString() const {
    if (value_.is_string()) {
        return value_.get<std::string>();
    }
    // Return empty string for non-string types (matching Rust behavior)
    return "";
}

std::vector<uint8_t> JsonValue::asBytes() const {
    if (value_.is_string()) {
        std::string str_value = value_.get<std::string>();
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

nlohmann::json JsonValue::asJson() const { return value_; }

std::unique_ptr<SerdeValue> makeJsonValue(const nlohmann::json &value) {
    return std::make_unique<JsonValue>(value);
}

std::unique_ptr<SerdeValue> makeJsonValue(nlohmann::json &&value) {
    return std::make_unique<JsonValue>(std::move(value));
}

std::unique_ptr<SerdeValue> makeJsonValue(const jsoncons::ojson &value) {
    return std::make_unique<JsonValue>(
        nlohmann::json::parse(value.to_string()));
}

std::unique_ptr<SerdeValue> makeJsonValue(jsoncons::ojson &&value) {
    return std::make_unique<JsonValue>(
        nlohmann::json::parse(value.to_string()));
}

// Utility functions for JSON value and schema extraction
jsoncons::ojson asOJson(const SerdeValue &value) {
    if (value.getFormat() != SerdeFormat::Json) {
        throw std::invalid_argument("SerdeValue is not JSON");
    }
    return jsoncons::ojson::parse(value.getValue<nlohmann::json>().dump());
}

nlohmann::json asJson(const SerdeValue &value) {
    if (value.getFormat() != SerdeFormat::Json) {
        throw std::invalid_argument("SerdeValue is not JSON");
    }
    return value.getValue<nlohmann::json>();
}

}  // namespace schemaregistry::serdes::json