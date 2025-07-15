/**
 * CreateDekRequest
 * Create dek request model
 */

#include "srclient/rest/model/CreateDekRequest.h"

namespace srclient::rest::model {

CreateDekRequest::CreateDekRequest() {}

CreateDekRequest::CreateDekRequest(
    const std::string& subject,
    const std::optional<int32_t>& version,
    const std::optional<Algorithm>& algorithm,
    const std::optional<std::string>& encryptedKeyMaterial
) : m_Subject(subject),
    m_Version(version),
    m_Algorithm(algorithm),
    m_EncryptedKeyMaterial(encryptedKeyMaterial) {}

bool CreateDekRequest::operator==(const CreateDekRequest& rhs) const {
    return m_Subject == rhs.m_Subject &&
           m_Version == rhs.m_Version &&
           m_Algorithm == rhs.m_Algorithm &&
           m_EncryptedKeyMaterial == rhs.m_EncryptedKeyMaterial;
}

bool CreateDekRequest::operator!=(const CreateDekRequest& rhs) const {
    return !(*this == rhs);
}

// Getters
std::string CreateDekRequest::getSubject() const { return m_Subject; }
std::optional<int32_t> CreateDekRequest::getVersion() const { return m_Version; }
std::optional<Algorithm> CreateDekRequest::getAlgorithm() const { return m_Algorithm; }
std::optional<std::string> CreateDekRequest::getEncryptedKeyMaterial() const { return m_EncryptedKeyMaterial; }

// Setters
void CreateDekRequest::setSubject(const std::string& subject) { m_Subject = subject; }
void CreateDekRequest::setVersion(const std::optional<int32_t>& version) { m_Version = version; }
void CreateDekRequest::setAlgorithm(const std::optional<Algorithm>& algorithm) { m_Algorithm = algorithm; }
void CreateDekRequest::setEncryptedKeyMaterial(const std::optional<std::string>& encryptedKeyMaterial) { m_EncryptedKeyMaterial = encryptedKeyMaterial; }

// JSON serialization
void to_json(nlohmann::json& j, const CreateDekRequest& o) {
    j = nlohmann::json{
        {"subject", o.m_Subject}
    };
    
    if (o.m_Version.has_value()) {
        j["version"] = o.m_Version.value();
    }
    
    if (o.m_Algorithm.has_value()) {
        j["algorithm"] = o.m_Algorithm.value();
    }
    
    if (o.m_EncryptedKeyMaterial.has_value()) {
        j["encryptedKeyMaterial"] = o.m_EncryptedKeyMaterial.value();
    }
}

void from_json(const nlohmann::json& j, CreateDekRequest& o) {
    j.at("subject").get_to(o.m_Subject);
    
    if (j.contains("version") && !j["version"].is_null()) {
        o.m_Version = j["version"].get<int32_t>();
    }
    
    if (j.contains("algorithm") && !j["algorithm"].is_null()) {
        o.m_Algorithm = j["algorithm"].get<Algorithm>();
    }
    
    if (j.contains("encryptedKeyMaterial") && !j["encryptedKeyMaterial"].is_null()) {
        o.m_EncryptedKeyMaterial = j["encryptedKeyMaterial"].get<std::string>();
    }
}

} // namespace srclient::rest::model 