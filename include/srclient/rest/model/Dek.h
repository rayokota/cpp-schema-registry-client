/**
 * Dek
 * Data encryption key model
 */

#pragma once

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <nlohmann/json.hpp>

namespace srclient::rest::model {

/**
 * Algorithm of the dek
 */
enum class Algorithm {
    Aes128Gcm,
    Aes256Gcm,
    Aes256Siv
};

/**
 * Dek class
 */
class Dek {
public:
    Dek();
    Dek(
        const std::string& kekName,
        const std::string& subject,
        int32_t version,
        Algorithm algorithm,
        const std::optional<std::string>& encryptedKeyMaterial,
        const std::optional<std::string>& keyMaterial,
        int64_t ts,
        const std::optional<bool>& deleted
    );

    virtual ~Dek() = default;

    bool operator==(const Dek& rhs) const;
    bool operator!=(const Dek& rhs) const;

    // Getters
    std::string getKekName() const;
    std::string getSubject() const;
    int32_t getVersion() const;
    Algorithm getAlgorithm() const;
    std::optional<std::string> getEncryptedKeyMaterial() const;
    std::optional<std::vector<uint8_t>> getEncryptedKeyMaterialBytes() const;
    std::optional<std::string> getKeyMaterial() const;
    std::optional<std::vector<uint8_t>> getKeyMaterialBytes() const;
    int64_t getTs() const;
    std::optional<bool> getDeleted() const;

    // Setters
    void setKekName(const std::string& kekName);
    void setSubject(const std::string& subject);
    void setVersion(int32_t version);
    void setAlgorithm(Algorithm algorithm);
    void setEncryptedKeyMaterial(const std::optional<std::string>& encryptedKeyMaterial);
    void setEncryptedKeyMaterialBytes(const std::optional<std::vector<uint8_t>>& encryptedKeyMaterialBytes);
    void setKeyMaterial(const std::optional<std::string>& keyMaterial);
    void setKeyMaterialBytes(const std::optional<std::vector<uint8_t>>& keyMaterialBytes);
    void setTs(int64_t ts);
    void setDeleted(const std::optional<bool>& deleted);

    // Utility methods
    void populateKeyMaterialBytes();
    std::optional<std::vector<uint8_t>> getEncryptedKeyMaterialBytesComputed();
    std::optional<std::vector<uint8_t>> getKeyMaterialBytesComputed();
    void setKeyMaterialFromBytes(const std::vector<uint8_t>& keyMaterialBytes);

    friend void to_json(nlohmann::json& j, const Dek& o);
    friend void from_json(const nlohmann::json& j, Dek& o);
    friend void to_json(nlohmann::json& j, const Algorithm& o);
    friend void from_json(const nlohmann::json& j, Algorithm& o);

private:
    std::string m_KekName;
    std::string m_Subject;
    int32_t m_Version;
    Algorithm m_Algorithm;
    std::optional<std::string> m_EncryptedKeyMaterial;
    mutable std::optional<std::vector<uint8_t>> m_EncryptedKeyMaterialBytes;
    std::optional<std::string> m_KeyMaterial;
    mutable std::optional<std::vector<uint8_t>> m_KeyMaterialBytes;
    int64_t m_Ts;
    std::optional<bool> m_Deleted;
};

} // namespace srclient::rest::model 