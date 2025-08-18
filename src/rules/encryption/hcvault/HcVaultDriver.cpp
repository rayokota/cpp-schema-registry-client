/**
 * HcVaultDriver implementation
 * C++ port of the Rust hcvault_driver.rs file
 */

#include "schemaregistry/rules/encryption/hcvault/HcVaultDriver.h"

#include <cstdlib>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <tuple>
#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "libvault/VaultClient.h"
#include "tink/aead.h"
#include "tink/kms_client.h"
#include "tink/util/status.h"
#include "tink/util/statusor.h"

namespace schemaregistry::rules::encryption::hcvault {

// Custom HashiCorp Vault AEAD implementation
class HcVaultAead : public crypto::tink::Aead {
  public:
    HcVaultAead(std::shared_ptr<Vault::Client> vaultClient,
                const std::string &mountPoint, const std::string &keyName)
        : vaultClient_(std::move(vaultClient)),
          mountPoint_(mountPoint),
          keyName_(keyName) {}

    crypto::tink::util::StatusOr<std::string> Encrypt(
        absl::string_view plaintext,
        absl::string_view associated_data) const override {
        try {
            // Convert plaintext to base64
            std::string plaintextBase64 =
                Vault::Base64::encode(std::string(plaintext));

            // Create encrypt request parameters
            Vault::Parameters parameters;
            parameters["plaintext"] = plaintextBase64;

            // Create path for encryption
            std::string path = mountPoint_ + "/encrypt/" + keyName_;

            // Perform encryption using Vault transit engine
            Vault::Transit transit(*vaultClient_);
            auto response = transit.encrypt(Vault::Path{path}, parameters);

            if (!response) {
                return crypto::tink::util::Status(
                    absl::StatusCode::kInternal,
                    "HashiCorp Vault encryption failed: No response received");
            }

            // Parse response and extract ciphertext
            // Note: The actual parsing depends on libvault's JSON response
            // format
            return response.value();

        } catch (const std::exception &e) {
            return crypto::tink::util::Status(
                absl::StatusCode::kInternal,
                "HashiCorp Vault encryption failed: " + std::string(e.what()));
        }
    }

    crypto::tink::util::StatusOr<std::string> Decrypt(
        absl::string_view ciphertext,
        absl::string_view associated_data) const override {
        try {
            // Create decrypt request parameters
            Vault::Parameters parameters;
            parameters["ciphertext"] = std::string(ciphertext);

            // Create path for decryption
            std::string path = mountPoint_ + "/decrypt/" + keyName_;

            // Perform decryption using Vault transit engine
            Vault::Transit transit(*vaultClient_);
            auto response = transit.decrypt(Vault::Path{path}, parameters);

            if (!response) {
                return crypto::tink::util::Status(
                    absl::StatusCode::kInternal,
                    "HashiCorp Vault decryption failed: No response received");
            }

            // Decode base64 plaintext
            std::string plaintext = Vault::Base64::decode(response.value());
            return plaintext;

        } catch (const std::exception &e) {
            return crypto::tink::util::Status(
                absl::StatusCode::kInternal,
                "HashiCorp Vault decryption failed: " + std::string(e.what()));
        }
    }

  private:
    std::shared_ptr<Vault::Client> vaultClient_;
    std::string mountPoint_;
    std::string keyName_;
};

// Custom HashiCorp Vault KMS Client that implements Tink's KmsClient interface
class HcVaultKmsClient : public crypto::tink::KmsClient {
  public:
    HcVaultKmsClient(const std::string &keyUriPrefix,
                     std::shared_ptr<Vault::Client> vaultClient)
        : keyUriPrefix_(keyUriPrefix), vaultClient_(std::move(vaultClient)) {}

    bool DoesSupport(absl::string_view key_uri) const override {
        std::string uri(key_uri);
        return uri.find(keyUriPrefix_) == 0;
    }

    crypto::tink::util::StatusOr<std::unique_ptr<crypto::tink::Aead>> GetAead(
        absl::string_view key_uri) const override {
        if (!DoesSupport(key_uri)) {
            return crypto::tink::util::Status(
                absl::StatusCode::kInvalidArgument,
                "Key URI must start with prefix " + keyUriPrefix_ +
                    ", but got " + std::string(key_uri));
        }

        try {
            // Strip the hcvault:// prefix
            std::string uri = std::string(key_uri);
            if (uri.find(HcVaultDriver::PREFIX) == 0) {
                uri = uri.substr(strlen(HcVaultDriver::PREFIX));
            }

            // Parse the URI to extract the path
            auto url = parseVaultUrl(uri);
            auto [mountPoint, keyName] = parseKeyPath(url.path);

            // Return the HashiCorp Vault AEAD implementation
            return std::make_unique<HcVaultAead>(vaultClient_, mountPoint,
                                                 keyName);

        } catch (const std::exception &e) {
            return crypto::tink::util::Status(
                absl::StatusCode::kInternal,
                "Failed to create HashiCorp Vault AEAD: " +
                    std::string(e.what()));
        }
    }

  private:
    std::string keyUriPrefix_;
    std::shared_ptr<Vault::Client> vaultClient_;

    struct VaultUrl {
        std::string scheme;
        std::string host;
        int port;
        std::string path;
    };

    static VaultUrl parseVaultUrl(const std::string &url) {
        // Parse URL like: https://vault.example.com:8200/transit/keys/my-key
        std::regex urlRegex(R"(^(https?)://([^:/]+)(?::(\d+))?(/.*)?$)");
        std::smatch matches;

        if (!std::regex_match(url, matches, urlRegex)) {
            throw std::invalid_argument("Invalid HashiCorp Vault URL format: " +
                                        url);
        }

        VaultUrl parsed;
        parsed.scheme = matches[1].str();
        parsed.host = matches[2].str();
        parsed.port = matches[3].matched
                          ? std::stoi(matches[3].str())
                          : (parsed.scheme == "https" ? 443 : 80);
        parsed.path = matches[4].matched ? matches[4].str() : "/";

        return parsed;
    }

    static std::tuple<std::string, std::string> parseKeyPath(
        const std::string &keyPath) {
        // Parse path like: /transit/keys/my-key
        // Expected format: /{mount-path}/keys/{keyName}
        std::vector<std::string> parts;
        std::stringstream ss(keyPath);
        std::string part;

        while (std::getline(ss, part, '/')) {
            if (!part.empty()) {
                parts.push_back(part);
            }
        }

        if (parts.size() < 3 || parts[parts.size() - 2] != "keys") {
            throw std::invalid_argument("Invalid key path format: " + keyPath);
        }

        std::string mountPath = parts[0];
        std::string keyName = parts[parts.size() - 1];

        return std::make_tuple(mountPath, keyName);
    }
};

// HcVaultDriver implementation

HcVaultDriver::HcVaultDriver() : prefix_(PREFIX) {}

const std::string &HcVaultDriver::getKeyUrlPrefix() const { return prefix_; }

std::shared_ptr<crypto::tink::KmsClient> HcVaultDriver::newKmsClient(
    const std::unordered_map<std::string, std::string> &conf,
    const std::string &keyUrl) {
    if (!isValidKeyUri(keyUrl)) {
        throw TinkError("Invalid HashiCorp Vault KMS key URI: " + keyUrl);
    }

    try {
        // Get token from configuration or environment
        std::string token;
        auto tokenIt = conf.find(TOKEN_ID);
        if (tokenIt != conf.end() && !tokenIt->second.empty()) {
            token = tokenIt->second;
        } else {
            const char *envToken = std::getenv("VAULT_TOKEN");
            if (envToken) {
                token = envToken;
            }
        }

        if (token.empty()) {
            throw TinkError(
                "Cannot load Vault token from configuration or environment");
        }

        // Get namespace from configuration or environment
        std::string namespace_;
        auto namespaceIt = conf.find(NAMESPACE);
        if (namespaceIt != conf.end() && !namespaceIt->second.empty()) {
            namespace_ = namespaceIt->second;
        } else {
            const char *envNamespace = std::getenv("VAULT_NAMESPACE");
            if (envNamespace) {
                namespace_ = envNamespace;
            }
        }

        // Parse the key URL to extract vault connection details
        std::string uri = keyUrl;
        if (uri.find(PREFIX) == 0) {
            uri = uri.substr(strlen(PREFIX));
        }

        auto url = parseVaultUrl(uri);

        // Create Vault client configuration
        auto configBuilder =
            Vault::ConfigBuilder()
                .withHost(Vault::Host{url.host})
                .withPort(Vault::Port{std::to_string(url.port)});

        if (url.scheme == "https") {
            configBuilder = configBuilder.withTlsEnabled(true);
        }

        if (!namespace_.empty()) {
            configBuilder =
                configBuilder.withNamespace(Vault::Namespace{namespace_});
        }

        auto config = configBuilder.build();

        // Create authentication strategy with token
        auto authStrategy = Vault::TokenStrategy{Vault::Token{token}};

        // Create the Vault client
        auto vaultClient =
            std::make_shared<Vault::Client>(config, authStrategy);

        // Create and return the HashiCorp Vault KMS client
        return std::make_shared<HcVaultKmsClient>(keyUrl, vaultClient);

    } catch (const std::exception &e) {
        throw TinkError("Error creating HashiCorp Vault KMS client: " +
                        std::string(e.what()));
    }
}

void HcVaultDriver::registerDriver() {
    // Note: This would typically register with a global registry
    // The exact implementation depends on how the KMS driver registry is
    // structured For now, this is a placeholder for the registration logic

    // Example implementation might look like:
    // KmsDriverRegistry::getInstance().registerDriver(std::make_unique<HcVaultDriver>());
}

std::tuple<std::string, std::string> HcVaultDriver::getEndpointPaths(
    const std::string &keyPath) const {
    // Parse path like: /transit/keys/my-key
    // Expected format: /{mount-path}/keys/{keyName}
    std::vector<std::string> parts;
    std::stringstream ss(keyPath);
    std::string part;

    while (std::getline(ss, part, '/')) {
        if (!part.empty()) {
            parts.push_back(part);
        }
    }

    if (parts.size() < 3 || parts[parts.size() - 2] != "keys") {
        throw TinkError("Invalid key path format: " + keyPath);
    }

    std::string mountPath = parts[0];
    std::string keyName = parts[parts.size() - 1];

    return std::make_tuple(mountPath, keyName);
}

bool HcVaultDriver::isValidKeyUri(const std::string &keyUri) const {
    if (keyUri.find(PREFIX) != 0) {
        return false;
    }

    try {
        std::string uri = keyUri.substr(strlen(PREFIX));
        auto url = parseVaultUrl(uri);
        getEndpointPaths(url.path);
        return true;
    } catch (const TinkError &) {
        return false;
    }
}

// Helper method for parsing vault URLs (used by both driver and client)
HcVaultDriver::VaultUrl HcVaultDriver::parseVaultUrl(
    const std::string &url) const {
    // Parse URL like: https://vault.example.com:8200/transit/keys/my-key
    std::regex urlRegex(R"(^(https?)://([^:/]+)(?::(\d+))?(/.*)?$)");
    std::smatch matches;

    if (!std::regex_match(url, matches, urlRegex)) {
        throw TinkError("Invalid HashiCorp Vault URL format: " + url);
    }

    VaultUrl parsed;
    parsed.scheme = matches[1].str();
    parsed.host = matches[2].str();
    parsed.port = matches[3].matched ? std::stoi(matches[3].str())
                                     : (parsed.scheme == "https" ? 443 : 80);
    parsed.path = matches[4].matched ? matches[4].str() : "/";

    return parsed;
}

}  // namespace schemaregistry::rules::encryption::hcvault
