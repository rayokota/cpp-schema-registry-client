/**
 * DekRegistryClient
 * Synchronous C++ implementation of Data Encryption Key (DEK) Registry Client
 */

#include "srclient/rest/DekRegistryClient.h"
#include "srclient/rest/MockDekRegistryClient.h"
#include <nlohmann/json.hpp>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <map>

using json = nlohmann::json;

namespace srclient::rest {

// DekStore implementation
DekStore::DekStore() {}

void DekStore::setKek(const KekId& kekId, const srclient::rest::model::Kek& kek) {
    keks[kekId] = kek;
}

void DekStore::setDek(const DekId& dekId, const srclient::rest::model::Dek& dek) {
    deks[dekId] = dek;
}

std::optional<srclient::rest::model::Kek> DekStore::getKek(const KekId& kekId) const {
    auto it = keks.find(kekId);
    if (it != keks.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<srclient::rest::model::Dek> DekStore::getDek(const DekId& dekId) const {
    auto it = deks.find(dekId);
    if (it != deks.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<srclient::rest::model::Dek> DekStore::getMutDek(const DekId& dekId) {
    auto it = deks.find(dekId);
    if (it != deks.end()) {
        return it->second;
    }
    return std::nullopt;
}

void DekStore::clear() {
    keks.clear();
    deks.clear();
}

// DekRegistryClient implementation
DekRegistryClient::DekRegistryClient(std::shared_ptr<const srclient::rest::ClientConfiguration> config)
    : restClient(std::make_shared<srclient::rest::RestClient>(config))
    , store(std::make_shared<DekStore>())
    , storeMutex(std::make_shared<std::mutex>()) {
    
    if (config->getBaseUrls().empty()) {
        throw srclient::rest::RestException("Base URL is required");
    }
}

std::shared_ptr<IDekRegistryClient> DekRegistryClient::newClient(
    std::shared_ptr<const srclient::rest::ClientConfiguration> config) {
    if (config->getBaseUrls().empty()) {
        throw srclient::rest::RestException("Base URL is required");
    }
    
    const std::string url = config->getBaseUrls()[0];
    if (url.substr(0, 7) == "mock://") {
        return std::make_shared<MockDekRegistryClient>(config);
    }
    return std::make_shared<DekRegistryClient>(config);
}

std::shared_ptr<const srclient::rest::ClientConfiguration> DekRegistryClient::getConfiguration() const {
    return restClient->getConfiguration();
}

std::string DekRegistryClient::urlEncode(const std::string& str) const {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;
    
    for (char c : str) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else {
            escaped << std::uppercase;
            escaped << '%' << std::setw(2) << int((unsigned char)c);
            escaped << std::nouppercase;
        }
    }
    
    return escaped.str();
}

std::string DekRegistryClient::sendHttpRequest(const std::string& path, 
                                              const std::string& method,
                                              const std::map<std::string, std::string>& query,
                                              const std::string& body) const {
    httplib::Headers headers;
    headers.insert(std::make_pair("Content-Type", "application/json"));
    
    // Convert map to httplib::Params
    httplib::Params params;
    for (const auto& pair : query) {
        params.insert(std::make_pair(pair.first, pair.second));
    }
    
    auto result = restClient->sendRequest(path, method, params, headers, body);
    
    if (!result) {
        throw srclient::rest::RestException("Request failed");
    }
    
    if (result->status >= 400) {
        std::string errorMsg = "HTTP Error " + std::to_string(result->status) + ": " + result->body;
        throw srclient::rest::RestException(errorMsg);
    }
    
    return result->body;
}

srclient::rest::model::Kek DekRegistryClient::parseKekFromJson(const std::string& jsonStr) const {
    try {
        json j = json::parse(jsonStr);
        srclient::rest::model::Kek kek;
        from_json(j, kek);
        return kek;
    } catch (const std::exception& e) {
        throw srclient::rest::RestException("Failed to parse KEK from JSON: " + std::string(e.what()));
    }
}

srclient::rest::model::Dek DekRegistryClient::parseDekFromJson(const std::string& jsonStr) const {
    try {
        json j = json::parse(jsonStr);
        srclient::rest::model::Dek dek;
        from_json(j, dek);
        return dek;
    } catch (const std::exception& e) {
        throw srclient::rest::RestException("Failed to parse DEK from JSON: " + std::string(e.what()));
    }
}

std::string DekRegistryClient::algorithmToString(srclient::rest::model::Algorithm algorithm) const {
    switch (algorithm) {
        case srclient::rest::model::Algorithm::Aes128Gcm:
            return "AES128_GCM";
        case srclient::rest::model::Algorithm::Aes256Gcm:
            return "AES256_GCM";
        case srclient::rest::model::Algorithm::Aes256Siv:
            return "AES256_SIV";
        default:
            return "AES256_GCM";
    }
}

srclient::rest::model::Kek DekRegistryClient::registerKek(
    const srclient::rest::model::CreateKekRequest& request) {
    
    KekId cacheKey{request.getName(), false};
    
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        auto kek = store->getKek(cacheKey);
        if (kek.has_value()) {
            return kek.value();
        }
    }
    
    // Prepare request
    std::string path = "/dek-registry/v1/keks";
    json j;
    to_json(j, request);
    std::string body = j.dump();
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "POST", {}, body);
    
    // Parse response
    srclient::rest::model::Kek kek = parseKekFromJson(responseBody);
    
    // Update cache
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        store->setKek(cacheKey, kek);
    }
    
    return kek;
}

srclient::rest::model::Dek DekRegistryClient::registerDek(
    const std::string& kek_name,
    const srclient::rest::model::CreateDekRequest& request) {
    
    DekId cacheKey{
        kek_name,
        request.getSubject(),
        request.getVersion().value_or(1),
        request.getAlgorithm().value_or(srclient::rest::model::Algorithm::Aes256Gcm),
        false
    };
    
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        auto dek = store->getDek(cacheKey);
        if (dek.has_value()) {
            return dek.value();
        }
    }
    
    // Prepare request
    std::string path = "/dek-registry/v1/keks/" + urlEncode(kek_name) + "/deks";
    json j;
    to_json(j, request);
    std::string body = j.dump();
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "POST", {}, body);
    
    // Parse response
    srclient::rest::model::Dek dek = parseDekFromJson(responseBody);
    
    // Update cache
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        store->setDek(cacheKey, dek);
    }
    
    return dek;
}

srclient::rest::model::Kek DekRegistryClient::getKek(const std::string& name, bool deleted) {
    KekId kekId{name, deleted};
    
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        auto kek = store->getKek(kekId);
        if (kek.has_value()) {
            return kek.value();
        }
    }
    
    // Prepare request
    std::string path = "/dek-registry/v1/keks/" + urlEncode(name);
    std::map<std::string, std::string> query;
    query.insert(std::make_pair("deleted", deleted ? "true" : "false"));
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "GET", query);
    
    // Parse response
    srclient::rest::model::Kek kek = parseKekFromJson(responseBody);
    
    // Update cache
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        store->setKek(kekId, kek);
    }
    
    return kek;
}

srclient::rest::model::Dek DekRegistryClient::getDek(
    const std::string& kek_name,
    const std::string& subject,
    const std::optional<srclient::rest::model::Algorithm>& algorithm,
    const std::optional<int32_t>& version,
    bool deleted) {
    
    auto alg = algorithm.value_or(srclient::rest::model::Algorithm::Aes256Gcm);
    auto ver = version.value_or(1);
    
    DekId dekId{kek_name, subject, ver, alg, deleted};
    
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        auto dek = store->getDek(dekId);
        if (dek.has_value()) {
            return dek.value();
        }
    }
    
    // Prepare request
    std::string path = "/dek-registry/v1/keks/" + urlEncode(kek_name) + 
                       "/deks/" + urlEncode(subject) + 
                       "/versions/" + std::to_string(ver);
    
    std::map<std::string, std::string> query;
    query.insert(std::make_pair("algorithm", algorithmToString(alg)));
    query.insert(std::make_pair("deleted", deleted ? "true" : "false"));
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "GET", query);
    
    // Parse response
    srclient::rest::model::Dek dek = parseDekFromJson(responseBody);
    
    // Populate key material bytes
    const_cast<srclient::rest::model::Dek&>(dek).populateKeyMaterialBytes();
    
    // Update cache
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        store->setDek(dekId, dek);
    }
    
    return dek;
}

srclient::rest::model::Dek DekRegistryClient::setDekKeyMaterial(
    const std::string& kek_name,
    const std::string& subject,
    const std::optional<srclient::rest::model::Algorithm>& algorithm,
    const std::optional<int32_t>& version,
    bool deleted,
    const std::vector<uint8_t>& key_material_bytes) {
    
    auto alg = algorithm.value_or(srclient::rest::model::Algorithm::Aes256Gcm);
    auto ver = version.value_or(1);
    
    DekId dekId{kek_name, subject, ver, alg, deleted};
    
    std::lock_guard<std::mutex> lock(*storeMutex);
    auto dek = store->getMutDek(dekId);
    
    if (dek.has_value()) {
        auto mutDek = dek.value();
        mutDek.setKeyMaterial(key_material_bytes);
        mutDek.populateKeyMaterialBytes();
        
        // Update cache
        store->setDek(dekId, mutDek);
        return mutDek;
    } else {
        throw srclient::rest::RestException("DEK not found");
    }
}

void DekRegistryClient::clearCaches() {
    std::lock_guard<std::mutex> lock(*storeMutex);
    store->clear();
}

void DekRegistryClient::close() {
    clearCaches();
}

} // namespace srclient::rest 