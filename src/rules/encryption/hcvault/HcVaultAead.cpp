/**
 * HcVaultAead implementation
 * HashiCorp Vault AEAD implementation
 */

#include "schemaregistry/rules/encryption/hcvault/HcVaultAead.h"

#include "absl/status/status.h"

namespace schemaregistry::rules::encryption::hcvault {

HcVaultAead::HcVaultAead(std::shared_ptr<Vault::Client> vaultClient,
                         const std::string &mountPoint,
                         const std::string &keyName)
    : vaultClient_(std::move(vaultClient)),
      mountPoint_(mountPoint),
      keyName_(keyName) {}

crypto::tink::util::StatusOr<std::string> HcVaultAead::Encrypt(
    absl::string_view plaintext, absl::string_view associated_data) const {
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

crypto::tink::util::StatusOr<std::string> HcVaultAead::Decrypt(
    absl::string_view ciphertext, absl::string_view associated_data) const {
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

}  // namespace schemaregistry::rules::encryption::hcvault
