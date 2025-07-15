/**
 * CreateKekRequest
 * Create kek request model
 */

#ifndef SRCLIENT_REST_MODEL_CREATE_KEK_REQUEST_H_
#define SRCLIENT_REST_MODEL_CREATE_KEK_REQUEST_H_

#include <string>
#include <optional>
#include <unordered_map>
#include <nlohmann/json.hpp>

namespace srclient::rest::model {

/**
 * CreateKekRequest class
 */
class CreateKekRequest {
public:
    CreateKekRequest();
    CreateKekRequest(
        const std::string& name,
        const std::string& kmsType,
        const std::string& kmsKeyId,
        const std::optional<std::unordered_map<std::string, std::string>>& kmsProps,
        const std::optional<std::string>& doc,
        bool shared
    );

    virtual ~CreateKekRequest() = default;

    bool operator==(const CreateKekRequest& rhs) const;
    bool operator!=(const CreateKekRequest& rhs) const;

    // Getters
    std::string getName() const;
    std::string getKmsType() const;
    std::string getKmsKeyId() const;
    std::optional<std::unordered_map<std::string, std::string>> getKmsProps() const;
    std::optional<std::string> getDoc() const;
    bool getShared() const;

    // Setters
    void setName(const std::string& name);
    void setKmsType(const std::string& kmsType);
    void setKmsKeyId(const std::string& kmsKeyId);
    void setKmsProps(const std::optional<std::unordered_map<std::string, std::string>>& kmsProps);
    void setDoc(const std::optional<std::string>& doc);
    void setShared(bool shared);

    friend void to_json(nlohmann::json& j, const CreateKekRequest& o);
    friend void from_json(const nlohmann::json& j, CreateKekRequest& o);

private:
    std::string m_Name;
    std::string m_KmsType;
    std::string m_KmsKeyId;
    std::optional<std::unordered_map<std::string, std::string>> m_KmsProps;
    std::optional<std::string> m_Doc;
    bool m_Shared;
};

} // namespace srclient::rest::model

#endif // SRCLIENT_REST_MODEL_CREATE_KEK_REQUEST_H_ 