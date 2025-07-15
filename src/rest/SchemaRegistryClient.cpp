/**
 * Confluent Schema Registry Client
 * Synchronous C++ client implementation for interacting with Confluent Schema Registry
 */

#include "srclient/rest/SchemaRegistryClient.h"
#include <nlohmann/json.hpp>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <iomanip>

using json = nlohmann::json;

namespace srclient::rest {

SchemaRegistryClient::SchemaRegistryClient(std::shared_ptr<const srclient::rest::ClientConfiguration> config)
    : restClient(std::make_shared<srclient::rest::RestClient>(config))
    , store(std::make_shared<SchemaStore>())
    , storeMutex(std::make_shared<std::mutex>())
    , latestVersionCacheMutex(std::make_shared<std::mutex>())
    , latestWithMetadataCacheMutex(std::make_shared<std::mutex>())
    , cacheCapacity(1000)
    , cacheLatestTtl(std::chrono::seconds(300)) // 5 minutes
{
    if (config->getBaseUrls().empty()) {
        throw srclient::rest::RestException("Base URL is required");
    }
}

SchemaRegistryClient::~SchemaRegistryClient() {
    close();
}

std::shared_ptr<const srclient::rest::ClientConfiguration> SchemaRegistryClient::getConfiguration() const {
    return restClient->getConfiguration();
}

std::string SchemaRegistryClient::urlEncode(const std::string& str) const {
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

std::string SchemaRegistryClient::createMetadataKey(const std::string& subject, 
                                                   const std::unordered_map<std::string, std::string>& metadata) const {
    std::ostringstream key;
    key << subject << "|";
    
    // Sort metadata for consistent key
    std::map<std::string, std::string> sortedMetadata(metadata.begin(), metadata.end());
    for (const auto& pair : sortedMetadata) {
        key << pair.first << "=" << pair.second << "&";
    }
    
    return key.str();
}

std::string SchemaRegistryClient::sendHttpRequest(const std::string& path, 
                                                const std::string& method,
                                                const httplib::Params& query,
                                                const std::string& body) const {
    httplib::Headers headers;
    headers.insert(std::make_pair("Content-Type", "application/json"));
    
    auto result = restClient->sendRequest(path, method, query, headers, body);
    
    if (!result) {
        throw srclient::rest::RestException("Request failed: " + to_string(result.error()));
    }
    
    if (result->status >= 400) {
        std::string errorMsg = "HTTP Error " + std::to_string(result->status) + ": " + result->body;
        throw srclient::rest::RestException(errorMsg);
    }
    
    return result->body;
}

srclient::rest::model::RegisteredSchema SchemaRegistryClient::parseRegisteredSchemaFromJson(const std::string& jsonStr) const {
    try {
        json j = json::parse(jsonStr);
        srclient::rest::model::RegisteredSchema response;
        from_json(j, response);
        return response;
    } catch (const std::exception& e) {
        throw srclient::rest::RestException("Failed to parse registered schema from JSON: " + std::string(e.what()));
    }
}

srclient::rest::model::ServerConfig SchemaRegistryClient::parseConfigFromJson(const std::string& jsonStr) const {
    try {
        json j = json::parse(jsonStr);
        srclient::rest::model::ServerConfig config;
        from_json(j, config);
        return config;
    } catch (const std::exception& e) {
        throw srclient::rest::RestException("Failed to parse config from JSON: " + std::string(e.what()));
    }
}

bool SchemaRegistryClient::parseBoolFromJson(const std::string& jsonStr) const {
    try {
        json j = json::parse(jsonStr);
        return j.get<bool>();
    } catch (const std::exception& e) {
        throw srclient::rest::RestException("Failed to parse boolean from JSON: " + std::string(e.what()));
    }
}

std::vector<int32_t> SchemaRegistryClient::parseIntArrayFromJson(const std::string& jsonStr) const {
    try {
        json j = json::parse(jsonStr);
        return j.get<std::vector<int32_t>>();
    } catch (const std::exception& e) {
        throw srclient::rest::RestException("Failed to parse int array from JSON: " + std::string(e.what()));
    }
}

std::vector<std::string> SchemaRegistryClient::parseStringArrayFromJson(const std::string& jsonStr) const {
    try {
        json j = json::parse(jsonStr);
        return j.get<std::vector<std::string>>();
    } catch (const std::exception& e) {
        throw srclient::rest::RestException("Failed to parse string array from JSON: " + std::string(e.what()));
    }
}

void SchemaRegistryClient::clearLatestCaches() {
    {
        std::lock_guard<std::mutex> lock(*latestVersionCacheMutex);
        latestVersionCache.clear();
    }
    {
        std::lock_guard<std::mutex> lock(*latestWithMetadataCacheMutex);
        latestWithMetadataCache.clear();
    }
}

void SchemaRegistryClient::clearCaches() {
    clearLatestCaches();
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        store->clear();
    }
}

void SchemaRegistryClient::close() {
    clearCaches();
}

srclient::rest::model::RegisteredSchema SchemaRegistryClient::registerSchema(
    const std::string& subject,
    const srclient::rest::model::Schema& schema,
    bool normalize) {
    
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        auto registered = store->getRegisteredBySchema(subject, schema);
        if (registered.has_value()) {
            return registered.value();
        }
    }
    
    // Prepare request
    std::string path = "/subjects/" + urlEncode(subject) + "/versions";
    httplib::Params query;
    query.insert(std::make_pair("normalize", normalize ? "true" : "false"));
    
    // Serialize schema to JSON
    json j;
    to_json(j, schema);
    std::string body = j.dump();
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "POST", query, body);
    
    // Parse response
    srclient::rest::model::RegisteredSchema response = parseRegisteredSchemaFromJson(responseBody);
    
    // Update cache
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        store->setRegisteredSchema(schema, response);
    }
    
    return response;
}

srclient::rest::model::Schema SchemaRegistryClient::getBySubjectAndId(
    const std::optional<std::string>& subject,
    int32_t id,
    const std::optional<std::string>& format) {
    
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        auto result = store->getSchemaById(subject.value_or(""), id);
        if (result.has_value()) {
            return result.value().second;
        }
    }
    
    // Prepare request
    std::string path = "/schemas/ids/" + std::to_string(id);
    httplib::Params query;
    if (subject.has_value()) {
        query.insert(std::make_pair("subject", subject.value()));
    }
    if (format.has_value()) {
        query.insert(std::make_pair("format", format.value()));
    }
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "GET", query);
    
    // Parse response
    srclient::rest::model::RegisteredSchema response = parseRegisteredSchemaFromJson(responseBody);
    srclient::rest::model::Schema schema = response.toSchema();
    
    // Update cache
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        store->setSchema(subject, std::make_optional(id), response.getGuid(), schema);
    }
    
    return schema;
}

srclient::rest::model::Schema SchemaRegistryClient::getByGuid(
    const std::string& guid,
    const std::optional<std::string>& format) {
    
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        auto result = store->getSchemaByGuid(guid);
        if (result.has_value()) {
            return result.value();
        }
    }
    
    // Prepare request
    std::string path = "/schemas/guids/" + urlEncode(guid);
    httplib::Params query;
    if (format.has_value()) {
        query.insert(std::make_pair("format", format.value()));
    }
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "GET", query);
    
    // Parse response
    srclient::rest::model::RegisteredSchema response = parseRegisteredSchemaFromJson(responseBody);
    srclient::rest::model::Schema schema = response.toSchema();
    
    // Update cache
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        store->setSchema(std::nullopt, response.getId(), std::make_optional(guid), schema);
    }
    
    return schema;
}

srclient::rest::model::RegisteredSchema SchemaRegistryClient::getBySchema(
    const std::string& subject,
    const srclient::rest::model::Schema& schema,
    bool normalize,
    bool deleted) {
    
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        auto result = store->getRegisteredBySchema(subject, schema);
        if (result.has_value()) {
            return result.value();
        }
    }
    
    // Prepare request
    std::string path = "/subjects/" + urlEncode(subject);
    httplib::Params query;
    query.insert(std::make_pair("normalize", normalize ? "true" : "false"));
    query.insert(std::make_pair("deleted", deleted ? "true" : "false"));
    
    // Serialize schema to JSON
    json j;
    to_json(j, schema);
    std::string body = j.dump();
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "POST", query, body);
    
    // Parse response
    srclient::rest::model::RegisteredSchema response = parseRegisteredSchemaFromJson(responseBody);
    
    // Update cache
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        // Ensure the schema matches the input
        srclient::rest::model::RegisteredSchema rs(response.getId(), response.getGuid(), response.getSubject(), response.getVersion(), schema);
        store->setRegisteredSchema(schema, rs);
    }
    
    return response;
}

srclient::rest::model::RegisteredSchema SchemaRegistryClient::getVersion(
    const std::string& subject,
    int32_t version,
    bool deleted,
    const std::optional<std::string>& format) {
    
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        auto result = store->getRegisteredByVersion(subject, version);
        if (result.has_value()) {
            return result.value();
        }
    }
    
    // Prepare request
    std::string path = "/subjects/" + urlEncode(subject) + "/versions/" + std::to_string(version);
    httplib::Params query;
    query.insert(std::make_pair("deleted", deleted ? "true" : "false"));
    if (format.has_value()) {
        query.insert(std::make_pair("format", format.value()));
    }
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "GET", query);
    
    // Parse response
    srclient::rest::model::RegisteredSchema response = parseRegisteredSchemaFromJson(responseBody);
    
    // Update cache
    {
        std::lock_guard<std::mutex> lock(*storeMutex);
        srclient::rest::model::Schema schema = response.toSchema();
        store->setRegisteredSchema(schema, response);
    }
    
    return response;
}

srclient::rest::model::RegisteredSchema SchemaRegistryClient::getLatestVersion(
    const std::string& subject,
    const std::optional<std::string>& format) {
    
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(*latestVersionCacheMutex);
        auto it = latestVersionCache.find(subject);
        if (it != latestVersionCache.end()) {
            return it->second;
        }
    }
    
    // Prepare request
    std::string path = "/subjects/" + urlEncode(subject) + "/versions/latest";
    httplib::Params query;
    if (format.has_value()) {
        query.insert(std::make_pair("format", format.value()));
    }
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "GET", query);
    
    // Parse response
    srclient::rest::model::RegisteredSchema response = parseRegisteredSchemaFromJson(responseBody);
    
    // Update cache
    {
        std::lock_guard<std::mutex> lock(*latestVersionCacheMutex);
        latestVersionCache[subject] = response;
    }
    
    return response;
}

srclient::rest::model::RegisteredSchema SchemaRegistryClient::getLatestWithMetadata(
    const std::string& subject,
    const std::unordered_map<std::string, std::string>& metadata,
    bool deleted,
    const std::optional<std::string>& format) {
    
    // Check cache first
    std::string cacheKey = createMetadataKey(subject, metadata);
    {
        std::lock_guard<std::mutex> lock(*latestWithMetadataCacheMutex);
        auto it = latestWithMetadataCache.find(cacheKey);
        if (it != latestWithMetadataCache.end()) {
            return it->second;
        }
    }
    
    // Prepare request
    std::string path = "/subjects/" + urlEncode(subject) + "/metadata";
    httplib::Params query;
    query.insert(std::make_pair("deleted", deleted ? "true" : "false"));
    if (format.has_value()) {
        query.insert(std::make_pair("format", format.value()));
    }
    
    // Add metadata to query
    for (const auto& pair : metadata) {
        query.insert(std::make_pair("key", pair.first));
        query.insert(std::make_pair("value", pair.second));
    }
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "GET", query);
    
    // Parse response
    srclient::rest::model::RegisteredSchema response = parseRegisteredSchemaFromJson(responseBody);
    
    // Update cache
    {
        std::lock_guard<std::mutex> lock(*latestWithMetadataCacheMutex);
        latestWithMetadataCache[cacheKey] = response;
    }
    
    return response;
}

std::vector<int32_t> SchemaRegistryClient::getAllVersions(const std::string& subject) {
    // Prepare request
    std::string path = "/subjects/" + urlEncode(subject) + "/versions";
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "GET");
    
    // Parse response
    return parseIntArrayFromJson(responseBody);
}

std::vector<std::string> SchemaRegistryClient::getAllSubjects(bool deleted) {
    // Prepare request
    std::string path = "/subjects";
    httplib::Params query;
    query.insert(std::make_pair("deleted", deleted ? "true" : "false"));
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "GET", query);
    
    // Parse response
    return parseStringArrayFromJson(responseBody);
}

std::vector<int32_t> SchemaRegistryClient::deleteSubject(const std::string& subject, bool permanent) {
    // Prepare request
    std::string path = "/subjects/" + urlEncode(subject);
    httplib::Params query;
    query.insert(std::make_pair("permanent", permanent ? "true" : "false"));
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "DELETE", query);
    
    // Parse response
    return parseIntArrayFromJson(responseBody);
}

int32_t SchemaRegistryClient::deleteSubjectVersion(const std::string& subject, int32_t version, bool permanent) {
    // Prepare request
    std::string path = "/subjects/" + urlEncode(subject) + "/versions/" + std::to_string(version);
    httplib::Params query;
    query.insert(std::make_pair("permanent", permanent ? "true" : "false"));
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "DELETE", query);
    
    // Parse response
    try {
        json j = json::parse(responseBody);
        return j.get<int32_t>();
    } catch (const std::exception& e) {
        throw srclient::rest::RestException("Failed to parse version from JSON: " + std::string(e.what()));
    }
}

bool SchemaRegistryClient::testSubjectCompatibility(const std::string& subject, const srclient::rest::model::Schema& schema) {
    // Prepare request
    std::string path = "/compatibility/subjects/" + urlEncode(subject) + "/versions/latest";
    
    // Serialize schema to JSON
    json j;
    to_json(j, schema);
    std::string body = j.dump();
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "POST", {}, body);
    
    // Parse response
    try {
        json response = json::parse(responseBody);
        return response.get<bool>();
    } catch (const std::exception& e) {
        throw srclient::rest::RestException("Failed to parse compatibility response from JSON: " + std::string(e.what()));
    }
}

bool SchemaRegistryClient::testCompatibility(const std::string& subject, int32_t version, const srclient::rest::model::Schema& schema) {
    // Prepare request
    std::string path = "/compatibility/subjects/" + urlEncode(subject) + "/versions/" + std::to_string(version);
    
    // Serialize schema to JSON
    json j;
    to_json(j, schema);
    std::string body = j.dump();
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "POST", {}, body);
    
    // Parse response
    try {
        json response = json::parse(responseBody);
        return response.get<bool>();
    } catch (const std::exception& e) {
        throw srclient::rest::RestException("Failed to parse compatibility response from JSON: " + std::string(e.what()));
    }
}

srclient::rest::model::ServerConfig SchemaRegistryClient::getConfig(const std::string& subject) {
    // Prepare request
    std::string path = "/config/" + urlEncode(subject);
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "GET");
    
    // Parse response
    return parseConfigFromJson(responseBody);
}

srclient::rest::model::ServerConfig SchemaRegistryClient::updateConfig(const std::string& subject, const srclient::rest::model::ServerConfig& config) {
    // Prepare request
    std::string path = "/config/" + urlEncode(subject);
    
    // Serialize config to JSON
    json j;
    to_json(j, config);
    std::string body = j.dump();
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "PUT", {}, body);
    
    // Parse response
    return parseConfigFromJson(responseBody);
}

srclient::rest::model::ServerConfig SchemaRegistryClient::getDefaultConfig() {
    // Prepare request
    std::string path = "/config";
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "GET");
    
    // Parse response
    return parseConfigFromJson(responseBody);
}

srclient::rest::model::ServerConfig SchemaRegistryClient::updateDefaultConfig(const srclient::rest::model::ServerConfig& config) {
    // Prepare request
    std::string path = "/config";
    
    // Serialize config to JSON
    json j;
    to_json(j, config);
    std::string body = j.dump();
    
    // Send request
    std::string responseBody = sendHttpRequest(path, "PUT", {}, body);
    
    // Parse response
    return parseConfigFromJson(responseBody);
}

}