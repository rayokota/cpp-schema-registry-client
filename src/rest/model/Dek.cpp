/**
 * Dek
 * Data encryption key model
 */

#include "srclient/rest/model/Dek.h"
#include <algorithm>
#include <sstream>
#include <iomanip>

namespace srclient::rest::model {

NLOHMANN_JSON_SERIALIZE_ENUM(Algorithm, {
    {Algorithm::Aes128Gcm, "AES128_GCM"},
    {Algorithm::Aes256Gcm, "AES256_GCM"},
    {Algorithm::Aes256Siv, "AES256_SIV"}
})

// Base64 encoding/decoding utilities
namespace {
    const std::string base64_chars = 
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789+/";

    bool is_base64(unsigned char c) {
        return (isalnum(c) || (c == '+') || (c == '/'));
    }

    std::string base64_encode(const std::vector<uint8_t>& bytes) {
        std::string ret;
        int i = 0;
        int j = 0;
        unsigned char char_array_3[3];
        unsigned char char_array_4[4];
        
        for (size_t idx = 0; idx < bytes.size(); idx++) {
            char_array_3[i++] = bytes[idx];
            if (i == 3) {
                char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
                char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
                char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
                char_array_4[3] = char_array_3[2] & 0x3f;
                
                for (i = 0; (i < 4); i++)
                    ret += base64_chars[char_array_4[i]];
                i = 0;
            }
        }
        
        if (i) {
            for (j = i; j < 3; j++)
                char_array_3[j] = '\0';
                
            char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
            char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
            char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
            char_array_4[3] = char_array_3[2] & 0x3f;
            
            for (j = 0; (j < i + 1); j++)
                ret += base64_chars[char_array_4[j]];
                
            while ((i++ < 3))
                ret += '=';
        }
        
        return ret;
    }

    std::vector<uint8_t> base64_decode(const std::string& encoded_string) {
        int in_len = encoded_string.size();
        int i = 0;
        int j = 0;
        int in = 0;
        unsigned char char_array_4[4], char_array_3[3];
        std::vector<uint8_t> ret;
        
        while (in_len-- && (encoded_string[in] != '=') && is_base64(encoded_string[in])) {
            char_array_4[i++] = encoded_string[in];
            in++;
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
            
            for (j = 0; (j < i - 1); j++)
                ret.push_back(char_array_3[j]);
        }
        
        return ret;
    }
}

Dek::Dek() : m_Version(0), m_Algorithm(Algorithm::Aes256Gcm), m_Ts(0) {}

Dek::Dek(
    const std::string& kekName,
    const std::string& subject,
    int32_t version,
    Algorithm algorithm,
    const std::optional<std::string>& encryptedKeyMaterial,
    const std::optional<std::string>& keyMaterial,
    int64_t ts,
    const std::optional<bool>& deleted
) : m_KekName(kekName),
    m_Subject(subject),
    m_Version(version),
    m_Algorithm(algorithm),
    m_EncryptedKeyMaterial(encryptedKeyMaterial),
    m_KeyMaterial(keyMaterial),
    m_Ts(ts),
    m_Deleted(deleted) {}

bool Dek::operator==(const Dek& rhs) const {
    return m_KekName == rhs.m_KekName &&
           m_Subject == rhs.m_Subject &&
           m_Version == rhs.m_Version &&
           m_Algorithm == rhs.m_Algorithm &&
           m_EncryptedKeyMaterial == rhs.m_EncryptedKeyMaterial &&
           m_KeyMaterial == rhs.m_KeyMaterial &&
           m_Ts == rhs.m_Ts &&
           m_Deleted == rhs.m_Deleted;
}

bool Dek::operator!=(const Dek& rhs) const {
    return !(*this == rhs);
}

// Getters
std::string Dek::getKekName() const { return m_KekName; }
std::string Dek::getSubject() const { return m_Subject; }
int32_t Dek::getVersion() const { return m_Version; }
Algorithm Dek::getAlgorithm() const { return m_Algorithm; }
std::optional<std::string> Dek::getEncryptedKeyMaterial() const { return m_EncryptedKeyMaterial; }
std::optional<std::vector<uint8_t>> Dek::getEncryptedKeyMaterialBytes() const { return m_EncryptedKeyMaterialBytes; }
std::optional<std::string> Dek::getKeyMaterial() const { return m_KeyMaterial; }
std::optional<std::vector<uint8_t>> Dek::getKeyMaterialBytes() const { return m_KeyMaterialBytes; }
int64_t Dek::getTs() const { return m_Ts; }
std::optional<bool> Dek::getDeleted() const { return m_Deleted; }

// Setters
void Dek::setKekName(const std::string& kekName) { m_KekName = kekName; }
void Dek::setSubject(const std::string& subject) { m_Subject = subject; }
void Dek::setVersion(int32_t version) { m_Version = version; }
void Dek::setAlgorithm(Algorithm algorithm) { m_Algorithm = algorithm; }
void Dek::setEncryptedKeyMaterial(const std::optional<std::string>& encryptedKeyMaterial) { m_EncryptedKeyMaterial = encryptedKeyMaterial; }
void Dek::setEncryptedKeyMaterialBytes(const std::optional<std::vector<uint8_t>>& encryptedKeyMaterialBytes) { m_EncryptedKeyMaterialBytes = encryptedKeyMaterialBytes; }
void Dek::setKeyMaterial(const std::optional<std::string>& keyMaterial) { m_KeyMaterial = keyMaterial; }
void Dek::setKeyMaterialBytes(const std::optional<std::vector<uint8_t>>& keyMaterialBytes) { m_KeyMaterialBytes = keyMaterialBytes; }
void Dek::setTs(int64_t ts) { m_Ts = ts; }
void Dek::setDeleted(const std::optional<bool>& deleted) { m_Deleted = deleted; }

// Utility methods
void Dek::populateKeyMaterialBytes() {
    getEncryptedKeyMaterialBytesComputed();
    getKeyMaterialBytesComputed();
}

std::optional<std::vector<uint8_t>> Dek::getEncryptedKeyMaterialBytesComputed() {
    if (!m_EncryptedKeyMaterial.has_value()) {
        return std::nullopt;
    }
    
    if (!m_EncryptedKeyMaterialBytes.has_value()) {
        m_EncryptedKeyMaterialBytes = base64_decode(m_EncryptedKeyMaterial.value());
    }
    
    return m_EncryptedKeyMaterialBytes;
}

std::optional<std::vector<uint8_t>> Dek::getKeyMaterialBytesComputed() {
    if (!m_KeyMaterial.has_value()) {
        return std::nullopt;
    }
    
    if (!m_KeyMaterialBytes.has_value()) {
        m_KeyMaterialBytes = base64_decode(m_KeyMaterial.value());
    }
    
    return m_KeyMaterialBytes;
}

void Dek::setKeyMaterialFromBytes(const std::vector<uint8_t>& keyMaterialBytes) {
    m_KeyMaterial = base64_encode(keyMaterialBytes);
}

// JSON serialization
void to_json(nlohmann::json& j, const Dek& o) {
    j = nlohmann::json{
        {"kekName", o.m_KekName},
        {"subject", o.m_Subject},
        {"version", o.m_Version},
        {"algorithm", o.m_Algorithm},
        {"ts", o.m_Ts}
    };
    
    if (o.m_EncryptedKeyMaterial.has_value()) {
        j["encryptedKeyMaterial"] = o.m_EncryptedKeyMaterial.value();
    }
    
    if (o.m_KeyMaterial.has_value()) {
        j["keyMaterial"] = o.m_KeyMaterial.value();
    }
    
    if (o.m_Deleted.has_value()) {
        j["deleted"] = o.m_Deleted.value();
    }
}

void from_json(const nlohmann::json& j, Dek& o) {
    j.at("kekName").get_to(o.m_KekName);
    j.at("subject").get_to(o.m_Subject);
    j.at("version").get_to(o.m_Version);
    j.at("algorithm").get_to(o.m_Algorithm);
    j.at("ts").get_to(o.m_Ts);
    
    if (j.contains("encryptedKeyMaterial") && !j["encryptedKeyMaterial"].is_null()) {
        o.m_EncryptedKeyMaterial = j["encryptedKeyMaterial"].get<std::string>();
    }
    
    if (j.contains("keyMaterial") && !j["keyMaterial"].is_null()) {
        o.m_KeyMaterial = j["keyMaterial"].get<std::string>();
    }
    
    if (j.contains("deleted") && !j["deleted"].is_null()) {
        o.m_Deleted = j["deleted"].get<bool>();
    }
}

} // namespace srclient::rest::model