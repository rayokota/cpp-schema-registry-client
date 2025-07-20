#include "srclient/serdes/json/JsonTypes.h"
#include <algorithm>

namespace srclient::serdes::json {

// Base64 encoding/decoding utilities for JSON
namespace {
    const std::string base64_chars = 
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789+/";

    bool is_base64(unsigned char c) {
        return (isalnum(c) || (c == '+') || (c == '/'));
    }

    std::vector<uint8_t> base64_decode(const std::string& encoded_string) {
        size_t in_len = encoded_string.size();
        int i = 0;
        int j = 0;
        int in = 0;
        unsigned char char_array_4[4], char_array_3[3];
        std::vector<uint8_t> ret;
        
        while (in_len-- && (encoded_string[in] != '=') && is_base64(encoded_string[in])) {
            char_array_4[i++] = encoded_string[in]; in++;
            if (i == 4) {
                for (i = 0; i < 4; i++)
                    char_array_4[i] = base64_chars.find(char_array_4[i]);
                    
                char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
                char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
                char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];
                
                for (i = 0; (i < 3); i++)
                    ret.push_back(char_array_3[i]);
                i = 0;
            }
        }
        
        if (i) {
            for (j = i; j < 4; j++)
                char_array_4[j] = 0;
                
            for (j = 0; j < 4; j++)
                char_array_4[j] = base64_chars.find(char_array_4[j]);
                
            char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
            char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
            char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];
            
            for (j = 0; (j < i - 1); j++) ret.push_back(char_array_3[j]);
        }
        
        return ret;
    }
}

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

} // namespace srclient::serdes::json 