/**
 * DekRegistryClient
 * Synchronous C++ implementation of Data Encryption Key (DEK) Registry Client
 */

#ifndef SRCLIENT_REST_DEK_REGISTRY_CLIENT_H_
#define SRCLIENT_REST_DEK_REGISTRY_CLIENT_H_

#include "srclient/rest/IDekRegistryClient.h"
#include "srclient/rest/RestClient.h"
#include "srclient/rest/ClientConfiguration.h"
#include "srclient/rest/RestException.h"
#include <memory>
#include <unordered_map>
#include <mutex>
#include <string>
#include <map>

namespace srclient::rest {

// Forward declarations
class DekStore;

/**
 * Key ID for KEK caching
 */
struct KekId {
    std::string name;
    bool deleted;

    bool operator==(const KekId& other) const {
        return name == other.name && deleted == other.deleted;
    }
};

/**
 * Key ID for DEK caching
 */
struct DekId {
    std::string kekName;
    std::string subject;
    int32_t version;
    srclient::rest::model::Algorithm algorithm;
    bool deleted;

    bool operator==(const DekId& other) const {
        return kekName == other.kekName &&
               subject == other.subject &&
               version == other.version &&
               algorithm == other.algorithm &&
               deleted == other.deleted;
    }
};

} // namespace srclient::rest

// Hash specializations for std::unordered_map
namespace std {
    template<>
    struct hash<srclient::rest::KekId> {
        std::size_t operator()(const srclient::rest::KekId& k) const {
            return std::hash<std::string>()(k.name) ^ (std::hash<bool>()(k.deleted) << 1);
        }
    };

    template<>
    struct hash<srclient::rest::DekId> {
        std::size_t operator()(const srclient::rest::DekId& k) const {
            return std::hash<std::string>()(k.kekName) ^
                   (std::hash<std::string>()(k.subject) << 1) ^
                   (std::hash<int32_t>()(k.version) << 2) ^
                   (std::hash<int>()(static_cast<int>(k.algorithm)) << 3) ^
                   (std::hash<bool>()(k.deleted) << 4);
        }
    };
}

namespace srclient::rest {

/**
 * Store for caching DEK and KEK data
 */
class DekStore {
public:
    DekStore();
    ~DekStore() = default;

    void setKek(const KekId& kekId, const srclient::rest::model::Kek& kek);
    void setDek(const DekId& dekId, const srclient::rest::model::Dek& dek);
    
    std::optional<srclient::rest::model::Kek> getKek(const KekId& kekId) const;
    std::optional<srclient::rest::model::Dek> getDek(const DekId& dekId) const;
    
    std::optional<srclient::rest::model::Dek> getMutDek(const DekId& dekId);
    
    void clear();

private:
    std::unordered_map<KekId, srclient::rest::model::Kek> keks;
    std::unordered_map<DekId, srclient::rest::model::Dek> deks;
};

/**
 * DEK Registry Client implementation
 */
class DekRegistryClient : public IDekRegistryClient {
public:
    explicit DekRegistryClient(std::shared_ptr<const srclient::rest::ClientConfiguration> config);
    
    virtual ~DekRegistryClient() = default;

    std::shared_ptr<const srclient::rest::ClientConfiguration> getConfiguration() const;

    // IDekRegistryClient implementation
    virtual srclient::rest::model::Kek registerKek(
        const srclient::rest::model::CreateKekRequest& request) override;

    virtual srclient::rest::model::Dek registerDek(
        const std::string& kekName,
        const srclient::rest::model::CreateDekRequest& request) override;

    virtual srclient::rest::model::Kek getKek(
        const std::string& name,
        bool deleted = false) override;

    virtual srclient::rest::model::Dek getDek(
        const std::string& kekName,
        const std::string& subject,
        const std::optional<srclient::rest::model::Algorithm>& algorithm = std::nullopt,
        const std::optional<int32_t>& version = std::nullopt,
        bool deleted = false) override;

    virtual srclient::rest::model::Dek setDekKeyMaterial(
        const std::string& kekName,
        const std::string& subject,
        const std::optional<srclient::rest::model::Algorithm>& algorithm = std::nullopt,
        const std::optional<int32_t>& version = std::nullopt,
        bool deleted = false,
        const std::vector<uint8_t>& keyMaterialBytes = {}) override;

    virtual void clearCaches() override;

    virtual void close() override;

private:
    std::shared_ptr<srclient::rest::RestClient> restClient;
    std::shared_ptr<DekStore> store;
    std::shared_ptr<std::mutex> storeMutex;

    // Helper methods
    std::string urlEncode(const std::string& str) const;
    std::string sendHttpRequest(const std::string& path, 
                               const std::string& method,
                               const std::map<std::string, std::string>& query = {},
                               const std::string& body = "") const;
    
    srclient::rest::model::Kek parseKekFromJson(const std::string& jsonStr) const;
    srclient::rest::model::Dek parseDekFromJson(const std::string& jsonStr) const;
    std::string algorithmToString(srclient::rest::model::Algorithm algorithm) const;
};

} // namespace srclient::rest

#endif // SRCLIENT_REST_DEK_REGISTRY_CLIENT_H_ 