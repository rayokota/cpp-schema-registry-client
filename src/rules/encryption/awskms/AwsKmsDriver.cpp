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

    // For other configuration options, we'll set environment variables
    // and let the AWS SDK handle credential resolution
    configureAwsEnvironment(conf, keyUrl);

    // Return empty string to use default credential chain
    return "";
}

void AwsKmsDriver::configureAwsEnvironment(
    const std::unordered_map<std::string, std::string> &conf,
    const std::string &keyUrl) const {
    // Set access key and secret key if provided
    auto accessKeyIt = conf.find(ACCESS_KEY_ID);
    auto secretKeyIt = conf.find(SECRET_ACCESS_KEY);

    if (accessKeyIt != conf.end() && !accessKeyIt->second.empty() &&
        secretKeyIt != conf.end() && !secretKeyIt->second.empty()) {
        setenv("AWS_ACCESS_KEY_ID", accessKeyIt->second.c_str(), 1);
        setenv("AWS_SECRET_ACCESS_KEY", secretKeyIt->second.c_str(), 1);
    }

    // Set profile if provided
    auto profileIt = conf.find(PROFILE);
    if (profileIt != conf.end() && !profileIt->second.empty()) {
        setenv("AWS_PROFILE", profileIt->second.c_str(), 1);
    }

    // Set assume role configuration if provided
    auto roleArnIt = conf.find(ROLE_ARN);
    if (roleArnIt != conf.end() && !roleArnIt->second.empty()) {
        setenv("AWS_ROLE_ARN", roleArnIt->second.c_str(), 1);

        auto roleSessionIt = conf.find(ROLE_SESSION_NAME);
        if (roleSessionIt != conf.end() && !roleSessionIt->second.empty()) {
            setenv("AWS_ROLE_SESSION_NAME", roleSessionIt->second.c_str(), 1);
        }

        auto externalIdIt = conf.find(ROLE_EXTERNAL_ID);
        if (externalIdIt != conf.end() && !externalIdIt->second.empty()) {
            setenv("AWS_ROLE_EXTERNAL_ID", externalIdIt->second.c_str(), 1);
        }
    }

    // Extract and set region from key URL if not already set
    try {
        std::string keyArn = keyUriToKeyArn(keyUrl);
        std::string region = getRegionFromKeyArn(keyArn);

        if (getenv("AWS_DEFAULT_REGION") == nullptr) {
            setenv("AWS_DEFAULT_REGION", region.c_str(), 1);
        }
    } catch (const TinkError &) {
        // If we can't extract region, let AWS SDK handle it
    }
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
