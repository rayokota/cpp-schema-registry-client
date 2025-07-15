/**
 * Kek
 * Key encryption key model
 */

#include "srclient/rest/model/Kek.h"

namespace srclient::rest::model {

Kek::Kek() : m_Shared(false), m_Ts(0) {}

Kek::Kek(
    const std::string& name,
    const std::string& kmsType,
    const std::string& kmsKeyId,
    const std::optional<std::unordered_map<std::string, std::string>>& kmsProps,
    const std::optional<std::string>& doc,
    bool shared,
    int64_t ts,
    const std::optional<bool>& deleted
) : m_Name(name),
    m_KmsType(kmsType),
    m_KmsKeyId(kmsKeyId),
    m_KmsProps(kmsProps),
    m_Doc(doc),
    m_Shared(shared),
    m_Ts(ts),
    m_Deleted(deleted) {}

bool Kek::operator==(const Kek& rhs) const {
    return m_Name == rhs.m_Name &&
           m_KmsType == rhs.m_KmsType &&
           m_KmsKeyId == rhs.m_KmsKeyId &&
           m_KmsProps == rhs.m_KmsProps &&
           m_Doc == rhs.m_Doc &&
           m_Shared == rhs.m_Shared &&
           m_Ts == rhs.m_Ts &&
           m_Deleted == rhs.m_Deleted;
}

bool Kek::operator!=(const Kek& rhs) const {
    return !(*this == rhs);
}

// Getters
std::string Kek::getName() const { return m_Name; }
std::string Kek::getKmsType() const { return m_KmsType; }
std::string Kek::getKmsKeyId() const { return m_KmsKeyId; }
std::optional<std::unordered_map<std::string, std::string>> Kek::getKmsProps() const { return m_KmsProps; }
std::optional<std::string> Kek::getDoc() const { return m_Doc; }
bool Kek::getShared() const { return m_Shared; }
int64_t Kek::getTs() const { return m_Ts; }
std::optional<bool> Kek::getDeleted() const { return m_Deleted; }

// Setters
void Kek::setName(const std::string& name) { m_Name = name; }
void Kek::setKmsType(const std::string& kmsType) { m_KmsType = kmsType; }
void Kek::setKmsKeyId(const std::string& kmsKeyId) { m_KmsKeyId = kmsKeyId; }
void Kek::setKmsProps(const std::optional<std::unordered_map<std::string, std::string>>& kmsProps) { m_KmsProps = kmsProps; }
void Kek::setDoc(const std::optional<std::string>& doc) { m_Doc = doc; }
void Kek::setShared(bool shared) { m_Shared = shared; }
void Kek::setTs(int64_t ts) { m_Ts = ts; }
void Kek::setDeleted(const std::optional<bool>& deleted) { m_Deleted = deleted; }

// JSON serialization
void to_json(nlohmann::json& j, const Kek& o) {
    j = nlohmann::json{
        {"name", o.m_Name},
        {"kmsType", o.m_KmsType},
        {"kmsKeyId", o.m_KmsKeyId},
        {"shared", o.m_Shared},
        {"ts", o.m_Ts}
    };
    
    if (o.m_KmsProps.has_value()) {
        j["kmsProps"] = o.m_KmsProps.value();
    }
    
    if (o.m_Doc.has_value()) {
        j["doc"] = o.m_Doc.value();
    }
    
    if (o.m_Deleted.has_value()) {
        j["deleted"] = o.m_Deleted.value();
    }
}

void from_json(const nlohmann::json& j, Kek& o) {
    j.at("name").get_to(o.m_Name);
    j.at("kmsType").get_to(o.m_KmsType);
    j.at("kmsKeyId").get_to(o.m_KmsKeyId);
    j.at("shared").get_to(o.m_Shared);
    j.at("ts").get_to(o.m_Ts);
    
    if (j.contains("kmsProps") && !j["kmsProps"].is_null()) {
        o.m_KmsProps = j["kmsProps"].get<std::unordered_map<std::string, std::string>>();
    }
    
    if (j.contains("doc") && !j["doc"].is_null()) {
        o.m_Doc = j["doc"].get<std::string>();
    }
    
    if (j.contains("deleted") && !j["deleted"].is_null()) {
        o.m_Deleted = j["deleted"].get<bool>();
    }
}

} // namespace srclient::rest::model 