/**
 * AwsKmsDriver implementation
 * C++ port of the Rust aws_driver.rs file
 */

#include "schemaregistry/rules/encryption/awskms/AwsKmsDriver.h"

#include <cstdlib>
#include <sstream>
#include <vector>

#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "schemaregistry/rules/encryption/EncryptionRegistry.h"
#include "schemaregistry/rules/encryption/awskms/AwsKmsClient.h"

namespace schemaregistry::rules::encryption::awskms {

AwsKmsDriver::AwsKmsDriver() : prefix_(PREFIX) {}

const std::string &AwsKmsDriver::getKeyUrlPrefix() const { return prefix_; }

std::shared_ptr<crypto::tink::KmsClient> AwsKmsDriver::newKmsClient(
    const std::unordered_map<std::string, std::string> &conf,
    const std::string &keyUrl) {
    if (!isValidKeyUri(keyUrl)) {
        throw TinkError("Invalid AWS KMS key URI: " + keyUrl);
    }

    try {
        // Build credentials path based on configuration
        std::string credentialsPath = buildCredentialsPath(conf, keyUrl);

        // Create the AWS KMS client using Tink's AwsKmsClient
        auto statusOrClient =
            crypto::tink::integration::awskms::AwsKmsClient::New(
                keyUrl, credentialsPath);

        if (!statusOrClient.ok()) {
            throw TinkError("Failed to create AWS KMS client: " +
                            std::string(statusOrClient.status().message()));
        }

        return std::move(statusOrClient.value());

    } catch (const std::exception &e) {
        throw TinkError("Error creating AWS KMS client: " +
                        std::string(e.what()));
    }
}

std::unique_ptr<AwsKmsDriver> AwsKmsDriver::create() {
    return std::make_unique<AwsKmsDriver>();
}

void AwsKmsDriver::registerDriver() {
    auto driver = create();
    EncryptionRegistry::getInstance().registerKmsDriver(std::move(driver));
}

std::string AwsKmsDriver::buildCredentialsPath(
    const std::unordered_map<std::string, std::string> &conf,
    const std::string &keyUrl) const {
    // Check if explicit credentials path is provided
    auto it = conf.find(CREDENTIALS_PATH);
    if (it != conf.end() && !it->second.empty()) {
        return it->second;
    }

    // Return empty string to use default credential chain
    return "";
}

std::string AwsKmsDriver::keyUriToKeyArn(const std::string &keyUri) const {
    if (keyUri.find(PREFIX) != 0) {
        throw TinkError("Invalid key URI: " + keyUri);
    }
    return keyUri.substr(strlen(PREFIX));
}

std::string AwsKmsDriver::getRegionFromKeyArn(const std::string &keyArn) const {
    // An AWS key ARN is of the form:
    // arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab

    std::vector<std::string> parts = absl::StrSplit(keyArn, ':');
    if (parts.size() < 6) {
        throw TinkError("Invalid key ARN format: " + keyArn);
    }

    // The region is the 4th component (index 3)
    return parts[3];
}

bool AwsKmsDriver::isValidKeyUri(const std::string &keyUri) const {
    if (keyUri.find(PREFIX) != 0) {
        return false;
    }

    try {
        std::string keyArn = keyUriToKeyArn(keyUri);
        getRegionFromKeyArn(keyArn);
        return true;
    } catch (const TinkError &) {
        return false;
    }
}

}  // namespace schemaregistry::rules::encryption::awskms
