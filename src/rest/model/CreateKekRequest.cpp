/**
 * CreateKekRequest
 * Create kek request model
 */

#include "srclient/rest/model/CreateKekRequest.h"

namespace srclient::rest::model {

CreateKekRequest::CreateKekRequest() : m_Shared(false) {}

CreateKekRequest::CreateKekRequest(
    const std::string& name,
    const std::string& kmsType,
    const std::string& kmsKeyId,
    const std::optional<std::unordered_map<std::string, std::string>>& kmsProps,
    const std::optional<std::string>& doc,
    bool shared
) : m_Name(name),
    m_KmsType(kmsType),
    m_KmsKeyId(kmsKeyId),
    m_KmsProps(kmsProps),
    m_Doc(doc),
    m_Shared(shared) {}

bool CreateKekRequest::operator==(const CreateKekRequest& rhs) const {
    return m_Name == rhs.m_Name &&
           m_KmsType == rhs.m_KmsType &&
           m_KmsKeyId == rhs.m_KmsKeyId &&
           m_KmsProps == rhs.m_KmsProps &&
           m_Doc == rhs.m_Doc &&
           m_Shared == rhs.m_Shared;
}

bool CreateKekRequest::operator!=(const CreateKekRequest& rhs) const {
    return !(*this == rhs);
}

// Getters
std::string CreateKekRequest::getName() const { return m_Name; }
std::string CreateKekRequest::getKmsType() const { return m_KmsType; }
std::string CreateKekRequest::getKmsKeyId() const { return m_KmsKeyId; }
std::optional<std::unordered_map<std::string, std::string>> CreateKekRequest::getKmsProps() const { return m_KmsProps; }
std::optional<std::string> CreateKekRequest::getDoc() const { return m_Doc; }
bool CreateKekRequest::getShared() const { return m_Shared; }

// Setters
void CreateKekRequest::setName(const std::string& name) { m_Name = name; }
void CreateKekRequest::setKmsType(const std::string& kmsType) { m_KmsType = kmsType; }
void CreateKekRequest::setKmsKeyId(const std::string& kmsKeyId) { m_KmsKeyId = kmsKeyId; }
void CreateKekRequest::setKmsProps(const std::optional<std::unordered_map<std::string, std::string>>& kmsProps) { m_KmsProps = kmsProps; }
void CreateKekRequest::setDoc(const std::optional<std::string>& doc) { m_Doc = doc; }
void CreateKekRequest::setShared(bool shared) { m_Shared = shared; }

// JSON serialization
void to_json(nlohmann::json& j, const CreateKekRequest& o) {
    j = nlohmann::json{
        {"name", o.m_Name},
        {"kmsType", o.m_KmsType},
        {"kmsKeyId", o.m_KmsKeyId},
        {"shared", o.m_Shared}
    };
    
    if (o.m_KmsProps.has_value()) {
        j["kmsProps"] = o.m_KmsProps.value();
    }
    
    if (o.m_Doc.has_value()) {
        j["doc"] = o.m_Doc.value();
    }
}

void from_json(const nlohmann::json& j, CreateKekRequest& o) {
    j.at("name").get_to(o.m_Name);
    j.at("kmsType").get_to(o.m_KmsType);
    j.at("kmsKeyId").get_to(o.m_KmsKeyId);
    j.at("shared").get_to(o.m_Shared);
    
    if (j.contains("kmsProps") && !j["kmsProps"].is_null()) {
        o.m_KmsProps = j["kmsProps"].get<std::unordered_map<std::string, std::string>>();
    }
    
    if (j.contains("doc") && !j["doc"].is_null()) {
        o.m_Doc = j["doc"].get<std::string>();
    }
}

} // namespace srclient::rest::model 