/**
 * AzureKmsDriver implementation
 * C++ port of the Rust azure_driver.rs file
 */

#include "schemaregistry/rules/encryption/azurekms/AzureKmsDriver.h"

#include <regex>
#include <stdexcept>
#include <tuple>

#include "absl/strings/string_view.h"
#include "azure/identity.hpp"
#include "azure/keyvault/keys.hpp"
#include "azure/keyvault/keys/cryptography/cryptography_client.hpp"
#include "tink/aead.h"
#include "tink/kms_client.h"
#include "tink/util/status.h"
#include "tink/util/statusor.h"

namespace schemaregistry::rules::encryption::azurekms {

// Custom Azure AEAD implementation that wraps Azure Key Vault operations
class AzureAead : public crypto::tink::Aead {
  public:
    AzureAead(std::shared_ptr<
              Azure::Security::KeyVault::Keys::Cryptography::CryptographyClient>
                  cryptoClient)
        : cryptoClient_(std::move(cryptoClient)) {}

    crypto::tink::util::StatusOr<std::string> Encrypt(
        absl::string_view plaintext,
        absl::string_view associated_data) const override {
        try {
            // Convert plaintext to vector<uint8_t>
            std::vector<uint8_t> plaintextBytes(plaintext.begin(),
                                                plaintext.end());

            // Create encrypt parameters using static factory method
            auto params = Azure::Security::KeyVault::Keys::Cryptography::
                EncryptParameters::RsaOaep256Parameters(plaintextBytes);

            // Perform encryption
            auto response = cryptoClient_->Encrypt(params);

            // Convert result to string
            const auto &result = response.Value.Ciphertext;
            return std::string(result.begin(), result.end());

        } catch (const std::exception &e) {
            return crypto::tink::util::Status(
                absl::StatusCode::kInternal,
                "Azure KMS encryption failed: " + std::string(e.what()));
        }
    }

    crypto::tink::util::StatusOr<std::string> Decrypt(
        absl::string_view ciphertext,
        absl::string_view associated_data) const override {
        try {
            // Convert ciphertext to vector<uint8_t>
            std::vector<uint8_t> ciphertextBytes(ciphertext.begin(),
                                                 ciphertext.end());

            // Create decrypt parameters using static factory method
            auto params = Azure::Security::KeyVault::Keys::Cryptography::
                DecryptParameters::RsaOaep256Parameters(ciphertextBytes);

            // Perform decryption
            auto response = cryptoClient_->Decrypt(params);

            // Convert result to string
            const auto &result = response.Value.Plaintext;
            return std::string(result.begin(), result.end());

        } catch (const std::exception &e) {
            return crypto::tink::util::Status(
                absl::StatusCode::kInternal,
                "Azure KMS decryption failed: " + std::string(e.what()));
        }
    }

  private:
    std::shared_ptr<
        Azure::Security::KeyVault::Keys::Cryptography::CryptographyClient>
        cryptoClient_;
};

// Custom Azure KMS Client that implements Tink's KmsClient interface
class AzureKmsClient : public crypto::tink::KmsClient {
  public:
    AzureKmsClient(
        const std::string &keyUriPrefix,
        std::shared_ptr<Azure::Core::Credentials::TokenCredential> credential)
        : keyUriPrefix_(keyUriPrefix), credential_(std::move(credential)) {}

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
            // Strip the azure-kms:// prefix
            std::string uri = std::string(key_uri);
            if (uri.find(AzureKmsDriver::PREFIX) == 0) {
                uri = uri.substr(strlen(AzureKmsDriver::PREFIX));
            }

            // Parse the key information
            auto [vaultUrl, keyName, keyVersion] = parseKeyInfo(uri);

            // Create Azure Key Vault client
            auto keyClient =
                std::make_shared<Azure::Security::KeyVault::Keys::KeyClient>(
                    vaultUrl, credential_);

            // Get the cryptography client for this specific key
            auto cryptoClient =
                std::make_shared<Azure::Security::KeyVault::Keys::Cryptography::
                                     CryptographyClient>(
                    keyClient->GetCryptographyClient(keyName, keyVersion));

            // Return the Azure AEAD implementation
            return std::make_unique<AzureAead>(cryptoClient);

        } catch (const std::exception &e) {
            return crypto::tink::util::Status(
                absl::StatusCode::kInternal,
                "Failed to create Azure AEAD: " + std::string(e.what()));
        }
    }

  private:
    std::string keyUriPrefix_;
    std::shared_ptr<Azure::Core::Credentials::TokenCredential> credential_;

    static std::tuple<std::string, std::string, std::string> parseKeyInfo(
        const std::string &keyUrl) {
        // Parse URL like:
        // https://vault-name.vault.azure.net/keys/key-name/key-version
        std::regex urlRegex(R"(^(https://[^/]+)/keys/([^/]+)/([^/]+)$)");
        std::smatch matches;

        if (!std::regex_match(keyUrl, matches, urlRegex)) {
            throw std::invalid_argument("Invalid Azure Key Vault URL format: " +
                                        keyUrl);
        }

        return std::make_tuple(matches[1].str(), matches[2].str(),
                               matches[3].str());
    }
};

// AzureKmsDriver implementation

AzureKmsDriver::AzureKmsDriver() : prefix_(PREFIX) {}

const std::string &AzureKmsDriver::getKeyUrlPrefix() const { return prefix_; }

std::shared_ptr<crypto::tink::KmsClient> AzureKmsDriver::newKmsClient(
    const std::unordered_map<std::string, std::string> &conf,
    const std::string &keyUrl) {
    if (!isValidKeyUri(keyUrl)) {
        throw TinkError("Invalid Azure KMS key URI: " + keyUrl);
    }

    try {
        // Create credential based on configuration
        std::shared_ptr<Azure::Core::Credentials::TokenCredential> credential;

        auto tenantIt = conf.find(TENANT_ID);
        auto clientIt = conf.find(CLIENT_ID);
        auto secretIt = conf.find(CLIENT_SECRET);

        if (tenantIt != conf.end() && clientIt != conf.end() &&
            secretIt != conf.end() && !tenantIt->second.empty() &&
            !clientIt->second.empty() && !secretIt->second.empty()) {
            // Use client secret credential
            credential =
                std::make_shared<Azure::Identity::ClientSecretCredential>(
                    tenantIt->second, clientIt->second, secretIt->second);
        } else {
            // Use default Azure credential
            credential =
                std::make_shared<Azure::Identity::DefaultAzureCredential>();
        }

        // Create and return the Azure KMS client
        return std::make_shared<AzureKmsClient>(keyUrl, credential);

    } catch (const std::exception &e) {
        throw TinkError("Error creating Azure KMS client: " +
                        std::string(e.what()));
    }
}

void AzureKmsDriver::registerDriver() {
    // Note: This would typically register with a global registry
    // The exact implementation depends on how the KMS driver registry is
    // structured For now, this is a placeholder for the registration logic

    // Example implementation might look like:
    // KmsDriverRegistry::getInstance().registerDriver(std::make_unique<AzureKmsDriver>());
}

std::tuple<std::string, std::string, std::string> AzureKmsDriver::getKeyInfo(
    const std::string &keyUrl) const {
    // Remove azure-kms:// prefix if present
    std::string url = keyUrl;
    if (url.find(PREFIX) == 0) {
        url = url.substr(strlen(PREFIX));
    }

    // Parse URL like:
    // https://vault-name.vault.azure.net/keys/key-name/key-version
    std::regex urlRegex(R"(^(https://[^/]+)/keys/([^/]+)/([^/]+)$)");
    std::smatch matches;

    if (!std::regex_match(url, matches, urlRegex)) {
        throw TinkError("Invalid Azure Key Vault URL format: " + keyUrl);
    }

    return std::make_tuple(matches[1].str(), matches[2].str(),
                           matches[3].str());
}

bool AzureKmsDriver::isValidKeyUri(const std::string &keyUri) const {
    if (keyUri.find(PREFIX) != 0) {
        return false;
    }

    try {
        getKeyInfo(keyUri);
        return true;
    } catch (const TinkError &) {
        return false;
    }
}

}  // namespace schemaregistry::rules::encryption::azurekms
