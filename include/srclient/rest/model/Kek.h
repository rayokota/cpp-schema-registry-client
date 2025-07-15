/**
 * Kek
 * Key encryption key model
 */

#ifndef SRCLIENT_REST_MODEL_KEK_H_
#define SRCLIENT_REST_MODEL_KEK_H_

#include <string>
#include <optional>
#include <cstdint>
#include <unordered_map>
#include <nlohmann/json.hpp>

namespace srclient::rest::model {

/**
 * Kek class
 */
class Kek {
public:
    Kek();
    Kek(
        const std::string& name,
        const std::string& kmsType,
        const std::string& kmsKeyId,
        const std::optional<std::unordered_map<std::string, std::string>>& kmsProps,
        const std::optional<std::string>& doc,
        bool shared,
        int64_t ts,
        const std::optional<bool>& deleted
    );

    virtual ~Kek() = default;

    bool operator==(const Kek& rhs) const;
    bool operator!=(const Kek& rhs) const;

    // Getters
    std::string getName() const;
    std::string getKmsType() const;
    std::string getKmsKeyId() const;
    std::optional<std::unordered_map<std::string, std::string>> getKmsProps() const;
    std::optional<std::string> getDoc() const;
    bool getShared() const;
    int64_t getTs() const;
    std::optional<bool> getDeleted() const;

    // Setters
    void setName(const std::string& name);
    void setKmsType(const std::string& kmsType);
    void setKmsKeyId(const std::string& kmsKeyId);
    void setKmsProps(const std::optional<std::unordered_map<std::string, std::string>>& kmsProps);
    void setDoc(const std::optional<std::string>& doc);
    void setShared(bool shared);
    void setTs(int64_t ts);
    void setDeleted(const std::optional<bool>& deleted);

    friend void to_json(nlohmann::json& j, const Kek& o);
    friend void from_json(const nlohmann::json& j, Kek& o);

private:
    std::string m_Name;
    std::string m_KmsType;
    std::string m_KmsKeyId;
    std::optional<std::unordered_map<std::string, std::string>> m_KmsProps;
    std::optional<std::string> m_Doc;
    bool m_Shared;
    int64_t m_Ts;
    std::optional<bool> m_Deleted;
};

} // namespace srclient::rest::model

#endif // SRCLIENT_REST_MODEL_KEK_H_ 