#pragma once

#include "proto/tink.pb.h"
#include "srclient/rest/ClientConfiguration.h"
#include "srclient/rest/DekRegistryClient.h"
#include "srclient/rest/DekRegistryTypes.h"
#include "srclient/rest/IDekRegistryClient.h"
#include "srclient/rest/model/CreateDekRequest.h"
#include "srclient/rest/model/CreateKekRequest.h"
#include "srclient/rest/model/Dek.h"
#include "srclient/rest/model/Kek.h"
#include "srclient/rules/encryption/KmsDriver.h"
#include "srclient/serdes/Serde.h"
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"
#include "tink/aead.h"
#include "tink/aead_key_templates.h"
#include "tink/deterministic_aead.h"
#include "tink/deterministic_aead_key_templates.h"
#include "tink/keyset_handle.h"
#include "tink/kms_client.h"
#include "tink/util/statusor.h"
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace srclient::rules::encryption {

using namespace srclient::serdes;
using namespace srclient::rest;

// Constants
constexpr const char *ENCRYPT_KEK_NAME = "encrypt.kek.name";
constexpr const char *ENCRYPT_KMS_KEY_ID = "encrypt.kms.key.id";
constexpr const char *ENCRYPT_KMS_TYPE = "encrypt.kms.type";
constexpr const char *ENCRYPT_DEK_ALGORITHM = "encrypt.dek.algorithm";
constexpr const char *ENCRYPT_DEK_EXPIRY_DAYS = "encrypt.dek.expiry.days";
constexpr int64_t MILLIS_IN_DAY = 24 * 60 * 60 * 1000;
constexpr const char *EMPTY_AAD = "";

/**
 * Clock interface for time operations
 * Based on Clock trait from encrypt_executor.rs
 */
class Clock {
  public:
    virtual ~Clock() = default;
    virtual int64_t now() const = 0;
};

/**
 * System clock implementation using real time
 * Based on SystemClock from encrypt_executor.rs
 */
class SystemClock : public Clock {
  public:
    SystemClock() = default;
    int64_t now() const override;
};

/**
 * Fake clock implementation for testing
 * Based on FakeClock from encrypt_executor.rs
 */
class FakeClock : public Clock {
  private:
    int64_t time_;
    mutable std::mutex mutex_;

  public:
    explicit FakeClock(int64_t time);
    int64_t now() const override;
    void setTime(int64_t time);
};

/**
 * Cryptographic operations handler
 * Based on Cryptor from encrypt_executor.rs
 */
class Cryptor {
  private:
    srclient::rest::model::Algorithm dek_format_;
    google::crypto::tink::KeyTemplate key_template_;

  public:
    explicit Cryptor(srclient::rest::model::Algorithm dek_format);

    bool isDeterministic() const;
    std::vector<uint8_t> generateKey() const;
    std::vector<uint8_t>
    encrypt(const std::vector<uint8_t> &dek,
            const std::vector<uint8_t> &plaintext,
            const std::vector<uint8_t> &associated_data) const;
    std::vector<uint8_t>
    decrypt(const std::vector<uint8_t> &dek,
            const std::vector<uint8_t> &ciphertext,
            const std::vector<uint8_t> &associated_data) const;
};

// Forward declarations
class EncryptionExecutorTransform;

/**
 * Main encryption executor for message-level encryption
 * Based on EncryptionExecutor from encrypt_executor.rs
 */
class EncryptionExecutor : public RuleExecutor {
  private:
    mutable std::shared_ptr<IDekRegistryClient> client_;
    mutable std::mutex client_mutex_;
    std::unordered_map<std::string, std::string> config_;
    mutable std::shared_mutex config_mutex_;
    std::shared_ptr<Clock> clock_;

  public:
    explicit EncryptionExecutor(
        std::shared_ptr<Clock> clock = std::make_shared<SystemClock>());

    // RuleBase interface
    void configure(std::shared_ptr<const ClientConfiguration> client_config,
                   const std::unordered_map<std::string, std::string>
                       &rule_config) override;
    std::string getType() const override;
    void close() override;

    // RuleExecutor interface
    std::unique_ptr<SerdeValue> transform(RuleContext &ctx,
                                          const SerdeValue &msg) override;

    // Accessor
    IDekRegistryClient *getClient() const;

    // Helper methods
    std::unique_ptr<EncryptionExecutorTransform>
    newTransform(RuleContext &ctx) const;

    // Static registration
    static void registerExecutor();

  private:
    friend class EncryptionExecutorTransform;

    Cryptor getCryptor(RuleContext &ctx) const;
    std::string getKekName(RuleContext &ctx) const;
    int64_t getDekExpiryDays(RuleContext &ctx) const;
};

/**
 * Transform helper class for encryption operations
 * Based on EncryptionExecutorTransform from encrypt_executor.rs
 */
class EncryptionExecutorTransform {
  private:
    const EncryptionExecutor *executor_;
    Cryptor cryptor_;
    std::string kek_name_;
    mutable std::optional<srclient::rest::model::Kek> kek_;
    mutable std::mutex kek_mutex_;
    int64_t dek_expiry_days_;

  public:
    EncryptionExecutorTransform(const EncryptionExecutor *executor,
                                Cryptor cryptor, const std::string &kek_name,
                                int64_t dek_expiry_days);

    std::unique_ptr<SerdeValue> transform(RuleContext &ctx,
                                          FieldType field_type,
                                          const SerdeValue &field_value);

  private:
    bool isDekRotated() const;
    srclient::rest::model::Kek getKek(RuleContext &ctx);
    srclient::rest::model::Kek getOrCreateKek(RuleContext &ctx);
    std::optional<srclient::rest::model::Kek>
    retrieveKekFromRegistry(const KekId &kek_id);
    std::optional<srclient::rest::model::Kek>
    storeKekToRegistry(const KekId &kek_id, const std::string &kms_type,
                       const std::string &kms_key_id, bool shared);

    srclient::rest::model::Dek getOrCreateDek(RuleContext &ctx,
                                              std::optional<int32_t> version);
    srclient::rest::model::Dek
    createDek(const DekId &dek_id, int32_t new_version,
              const std::optional<std::vector<uint8_t>> &encrypted_dek);
    std::optional<std::vector<uint8_t>>
    encryptDek(const srclient::rest::model::Kek &kek,
               const std::vector<uint8_t> &raw_dek);
    std::vector<uint8_t> decryptDek(const srclient::rest::model::Kek &kek,
                                    const std::vector<uint8_t> &encrypted_dek);
    srclient::rest::model::Dek
    updateCachedDek(const std::string &kek_name, const std::string &subject,
                    std::optional<srclient::rest::model::Algorithm> algorithm,
                    std::optional<int32_t> version, bool deleted,
                    const std::vector<uint8_t> &key_material_bytes);
    std::optional<srclient::rest::model::Dek>
    retrieveDekFromRegistry(const DekId &dek_id);
    std::optional<srclient::rest::model::Dek> storeDekToRegistry(
        const DekId &dek_id,
        const std::optional<std::vector<uint8_t>> &encrypted_dek);

    bool isExpired(RuleContext &ctx,
                   const std::optional<srclient::rest::model::Dek> &dek) const;
    std::vector<uint8_t>
    prefixVersion(int32_t version,
                  const std::vector<uint8_t> &ciphertext) const;
    std::pair<std::optional<int32_t>, std::vector<uint8_t>>
    extractVersion(const std::vector<uint8_t> &ciphertext) const;
    std::optional<std::vector<uint8_t>> toBytes(FieldType field_type,
                                                const SerdeValue &value) const;
    std::unique_ptr<SerdeValue>
    toObject(RuleContext &ctx, FieldType field_type,
             const std::vector<uint8_t> &value) const;

    std::unique_ptr<crypto::tink::Aead>
    getAead(const std::unordered_map<std::string, std::string> &config,
            const srclient::rest::model::Kek &kek);
    std::shared_ptr<crypto::tink::KmsClient>
    getKmsClient(const std::unordered_map<std::string, std::string> &config,
                 const std::string &kek_url);
    std::shared_ptr<crypto::tink::KmsClient> registerKmsClient(
        std::shared_ptr<KmsDriver> kms_driver,
        const std::unordered_map<std::string, std::string> &config,
        const std::string &kek_url);
};

} // namespace srclient::rules::encryption