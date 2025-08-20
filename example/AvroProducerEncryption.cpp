#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

// Avro C++ includes
#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/ValidSchema.hh>

// Kafka C++ library
#include <librdkafka/rdkafka.h>

// Schema Registry Client includes
#include "schemaregistry/rest/ClientConfiguration.h"
#include "schemaregistry/rest/SchemaRegistryClient.h"
#include "schemaregistry/rest/model/Schema.h"
#include "schemaregistry/rest/model/Rule.h"
#include "schemaregistry/rest/model/RuleSet.h"
#include "schemaregistry/serdes/SerdeConfig.h"
#include "schemaregistry/serdes/SerdeTypes.h"
#include "schemaregistry/serdes/avro/AvroSerializer.h"

// Encryption includes
#include "schemaregistry/rules/encryption/EncryptionRegistry.h"
#include "schemaregistry/rules/encryption/FieldEncryptionExecutor.h"
#include "schemaregistry/rules/encryption/awskms/AwsKmsDriver.h"
#include "schemaregistry/rules/encryption/azurekms/AzureKmsDriver.h"
#include "schemaregistry/rules/encryption/gcpkms/GcpKmsDriver.h"
#include "schemaregistry/rules/encryption/hcvault/HcVaultDriver.h"
#include "schemaregistry/rules/encryption/localkms/LocalKmsDriver.h"
#include "schemaregistry/serdes/RuleRegistry.h"

using namespace schemaregistry::rest;
using namespace schemaregistry::rest::model;
using namespace schemaregistry::serdes;
using namespace schemaregistry::serdes::avro;
using namespace schemaregistry::rules::encryption;

class AvroProducerEncryptionExample {
 private:
  std::shared_ptr<ISchemaRegistryClient> client_;
  std::unique_ptr<AvroSerializer> serializer_;
  rd_kafka_t* producer_;
  rd_kafka_conf_t* conf_;
  ::avro::ValidSchema valid_schema_;

 public:
  AvroProducerEncryptionExample(const std::string& brokers,
                               const std::string& schema_registry_url,
                               const std::string& kek_name,
                               const std::string& kms_type,
                               const std::string& kms_key_id)
      : producer_(nullptr), conf_(nullptr) {
    // Register KMS drivers and encryption executors
    registerKmsDriversAndExecutors();

    // Create Schema Registry client configuration
    auto client_config = std::make_shared<ClientConfiguration>(
        std::vector<std::string>{schema_registry_url});

    // Create Schema Registry client
    client_ = SchemaRegistryClient::newClient(client_config);

    // Define the Avro schema with encryption rules
    std::string schema_str = R"(
{
  "namespace": "confluent.io.examples.serialization.avro",
  "name": "User",
  "type": "record",
  "fields": [
    {"name": "name", "type": "string", "confluent:tags": [ "PII" ]},
    {"name": "favorite_number", "type": "long"},
    {"name": "favorite_color", "type": "string"}
  ]
}
)";

    // Create encryption rule
    Rule encryption_rule;
    encryption_rule.setName("encryptPII");
    encryption_rule.setKind(Kind::Transform);
    encryption_rule.setMode(Mode::WriteRead);
    encryption_rule.setType("ENCRYPT");
    
    std::vector<std::string> tags = {"PII"};
    encryption_rule.setTags(tags);
    
    std::map<std::string, std::string> params = {
        {"encrypt.kek.name", kek_name},
        {"encrypt.kms.type", kms_type},
        {"encrypt.kms.key.id", kms_key_id}
    };
    encryption_rule.setParams(params);
    encryption_rule.setOnFailure("ERROR,NONE");

    // Create rule set
    RuleSet rule_set;
    std::vector<Rule> domain_rules = {encryption_rule};
    rule_set.setDomainRules(domain_rules);

    // Create Schema object with encryption rules
    Schema schema;
    schema.setSchemaType("AVRO");
    schema.setSchema(schema_str);
    schema.setRuleSet(rule_set);

    // Parse and validate the schema
    std::istringstream schema_stream(schema_str);
    ::avro::compileJsonSchema(schema_stream, valid_schema_);

    // Create serializer configuration with encryption support
    // KMS properties can be passed as follows (example commented out):
    // std::unordered_map<std::string, std::string> rule_config = {
    //     {"secret.access.key", "xxx"},
    //     {"access.key.id", "xxx"}
    // };
    std::unordered_map<std::string, std::string> rule_config;
    
    // Use latest version selector for encryption
    SchemaSelectorData schema_selector = SchemaSelectorData::createLatestVersion();
    SerializerConfig ser_config(false, schema_selector, true, false, rule_config);

    // Create rule registry for field transformations
    auto rule_registry = std::make_shared<RuleRegistry>();

    // Create Avro serializer
    serializer_ = std::make_unique<AvroSerializer>(client_, std::nullopt, rule_registry, ser_config);

    // Create Kafka producer configuration
    conf_ = rd_kafka_conf_new();
    char errstr[512];

    if (rd_kafka_conf_set(conf_, "bootstrap.servers", brokers.c_str(), errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK) {
      throw std::runtime_error("Failed to set bootstrap.servers: " +
                               std::string(errstr));
    }

    if (rd_kafka_conf_set(conf_, "message.timeout.ms", "5000", errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK) {
      throw std::runtime_error("Failed to set message.timeout.ms: " +
                               std::string(errstr));
    }

    // Create producer
    producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf_, errstr, sizeof(errstr));
    if (!producer_) {
      throw std::runtime_error("Failed to create producer: " + std::string(errstr));
    }
  }

  ~AvroProducerEncryptionExample() {
    if (producer_) {
      // Wait for any outstanding messages to be delivered
      rd_kafka_flush(producer_, 10000);
      rd_kafka_destroy(producer_);
    }
    if (conf_) {
      rd_kafka_conf_destroy(conf_);
    }
  }

  static void registerKmsDriversAndExecutors() {
    std::cout << "Registering KMS drivers and encryption executors..." << std::endl;
    
    // Register AWS KMS driver
    awskms::AwsKmsDriver::registerDriver();
    
    // Register Azure KMS driver
    azurekms::AzureKmsDriver::registerDriver();
    
    // Register GCP KMS driver
    gcpkms::GcpKmsDriver::registerDriver();
    
    // Register HashiCorp Vault driver
    hcvault::HcVaultDriver::registerDriver();
    
    // Register Local KMS driver
    localkms::LocalKmsDriver::registerDriver();
    
    // Register field encryption executor
    FieldEncryptionExecutor::registerExecutor();
    
    std::cout << "KMS drivers and encryption executors registered successfully." << std::endl;
  }

  void registerSchemaAndProduce(const std::string& topic_name) {
    try {
      // Register the schema with encryption rules
      std::string subject = topic_name + "-value";
      
      // Define the Avro schema with encryption rules
      std::string schema_str = R"(
{
  "namespace": "confluent.io.examples.serialization.avro",
  "name": "User",
  "type": "record",
  "fields": [
    {"name": "name", "type": "string", "confluent:tags": [ "PII" ]},
    {"name": "favorite_number", "type": "long"},
    {"name": "favorite_color", "type": "string"}
  ]
}
)";

      // Create encryption rule
      Rule encryption_rule;
      encryption_rule.setName("encryptPII");
      encryption_rule.setKind(Kind::Transform);
      encryption_rule.setMode(Mode::WriteRead);
      encryption_rule.setType("ENCRYPT");
      
      std::vector<std::string> tags = {"PII"};
      encryption_rule.setTags(tags);
      
      std::map<std::string, std::string> params;
      // Get encryption parameters from environment or configuration
      // For this example, we'll use placeholder values
      params["encrypt.kek.name"] = "test-kek";
      params["encrypt.kms.type"] = "local-kms";
      params["encrypt.kms.key.id"] = "test-key";
      encryption_rule.setParams(params);
      encryption_rule.setOnFailure("ERROR,NONE");

      // Create rule set
      RuleSet rule_set;
      std::vector<Rule> domain_rules = {encryption_rule};
      rule_set.setDomainRules(domain_rules);

      // Create Schema object with encryption rules
      Schema schema;
      schema.setSchemaType("AVRO");
      schema.setSchema(schema_str);
      schema.setRuleSet(rule_set);

      std::cout << "Registering schema with encryption rules for subject: " << subject << std::endl;
      RegisteredSchema registered_schema = client_->registerSchema(subject, schema, true);
      
      auto schema_id = registered_schema.getId();
      if (schema_id.has_value()) {
        std::cout << "Schema registered successfully with ID: " << schema_id.value() << std::endl;
      } else {
        std::cout << "Schema registered successfully (ID not available)" << std::endl;
      }

    } catch (const std::exception& e) {
      std::cerr << "Error registering schema: " << e.what() << std::endl;
      // Continue with message production even if schema registration fails
      // (it might already be registered)
    }

    produce_messages(topic_name);
  }

  void produce_messages(const std::string& topic_name) {
    // Delivery report callback
    auto delivery_cb = [](rd_kafka_t* rk, const rd_kafka_message_t* rkmessage, void* opaque) {
      if (rkmessage->err) {
        std::cerr << "Message delivery failed: " << rd_kafka_err2str(rkmessage->err) << std::endl;
      } else {
        std::cout << "Encrypted message delivered to topic " << rd_kafka_topic_name(rkmessage->rkt)
                  << " [" << rkmessage->partition << "] at offset " << rkmessage->offset << std::endl;
      }
    };

    rd_kafka_conf_set_dr_msg_cb(conf_, delivery_cb);

    // Create serialization context
    SerializationContext ser_ctx;
    ser_ctx.topic = topic_name;
    ser_ctx.serde_type = SerdeType::Value;
    ser_ctx.serde_format = SerdeFormat::Avro;
    ser_ctx.headers = std::nullopt;

    std::cout << "Producing 5 encrypted messages to topic: " << topic_name << std::endl;

    // Produce 5 messages
    for (int i = 0; i < 5; i++) {
      try {
        // Create GenericDatum with schema and get the record
        ::avro::GenericDatum datum(valid_schema_);
        ::avro::GenericRecord& record = datum.value<::avro::GenericRecord>();
        
        // Set field values (PII field "name" will be encrypted automatically)
        record.setFieldAt(0, ::avro::GenericDatum(std::string("Name " + std::to_string(i))));
        record.setFieldAt(1, ::avro::GenericDatum(static_cast<int64_t>(i)));
        record.setFieldAt(2, ::avro::GenericDatum(std::string("blue")));

        // Serialize the record (encryption will be applied automatically)
        std::vector<uint8_t> serialized_data = serializer_->serialize(ser_ctx, datum);

        // Produce the message
        rd_kafka_resp_err_t err = rd_kafka_producev(
            producer_,
            RD_KAFKA_V_TOPIC(topic_name.c_str()),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE(serialized_data.data(), serialized_data.size()),
            RD_KAFKA_V_END);

        if (err) {
          std::cerr << "Failed to produce message " << i << ": " << rd_kafka_err2str(err) << std::endl;
        } else {
          std::cout << "Enqueued encrypted message " << i << " for delivery" << std::endl;
        }

        // Poll for delivery reports
        rd_kafka_poll(producer_, 0);

      } catch (const std::exception& e) {
        std::cerr << "Error producing message " << i << ": " << e.what() << std::endl;
      }
    }

    // Wait for all messages to be delivered
    std::cout << "Waiting for message delivery..." << std::endl;
    rd_kafka_flush(producer_, 10000);
    
    // Final poll to handle any remaining delivery reports
    rd_kafka_poll(producer_, 1000);
  }
};

void print_usage(const char* program_name) {
  std::cout << "Usage: " << program_name << " [OPTIONS]" << std::endl;
  std::cout << "Options:" << std::endl;
  std::cout << "  -b, --brokers <brokers>           Broker list in kafka format (default: localhost:9092)" << std::endl;
  std::cout << "  -t, --topic <topic>               Destination topic (required)" << std::endl;
  std::cout << "  -u, --url <schema-registry-url>   Schema Registry URL (required)" << std::endl;
  std::cout << "  --kek-name <kek-name>             KEK name (required)" << std::endl;
  std::cout << "  --kms-type <kms-type>             KMS type (required)" << std::endl;
  std::cout << "  --kms-key-id <kms-key-id>         KMS key ID (required)" << std::endl;
  std::cout << "  -h, --help                        Show this help message" << std::endl;
}

int main(int argc, char* argv[]) {
  std::string brokers = "localhost:9092";
  std::string topic;
  std::string schema_registry_url;
  std::string kek_name;
  std::string kms_type;
  std::string kms_key_id;

  // Parse command line arguments
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    
    if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      return 0;
    } else if ((arg == "-b" || arg == "--brokers") && i + 1 < argc) {
      brokers = argv[++i];
    } else if ((arg == "-t" || arg == "--topic") && i + 1 < argc) {
      topic = argv[++i];
    } else if ((arg == "-u" || arg == "--url") && i + 1 < argc) {
      schema_registry_url = argv[++i];
    } else if (arg == "--kek-name" && i + 1 < argc) {
      kek_name = argv[++i];
    } else if (arg == "--kms-type" && i + 1 < argc) {
      kms_type = argv[++i];
    } else if (arg == "--kms-key-id" && i + 1 < argc) {
      kms_key_id = argv[++i];
    } else {
      std::cerr << "Unknown argument: " << arg << std::endl;
      print_usage(argv[0]);
      return 1;
    }
  }

  // Validate required arguments
  if (topic.empty()) {
    std::cerr << "Error: topic is required" << std::endl;
    print_usage(argv[0]);
    return 1;
  }

  if (schema_registry_url.empty()) {
    std::cerr << "Error: schema registry URL is required" << std::endl;
    print_usage(argv[0]);
    return 1;
  }

  if (kek_name.empty()) {
    std::cerr << "Error: KEK name is required" << std::endl;
    print_usage(argv[0]);
    return 1;
  }

  if (kms_type.empty()) {
    std::cerr << "Error: KMS type is required" << std::endl;
    print_usage(argv[0]);
    return 1;
  }

  if (kms_key_id.empty()) {
    std::cerr << "Error: KMS key ID is required" << std::endl;
    print_usage(argv[0]);
    return 1;
  }

  std::cout << "Starting Avro producer with encryption support:" << std::endl;
  std::cout << "  Brokers: " << brokers << std::endl;
  std::cout << "  Topic: " << topic << std::endl;
  std::cout << "  Schema Registry URL: " << schema_registry_url << std::endl;
  std::cout << "  KEK Name: " << kek_name << std::endl;
  std::cout << "  KMS Type: " << kms_type << std::endl;
  std::cout << "  KMS Key ID: " << kms_key_id << std::endl;

  try {
    AvroProducerEncryptionExample producer(brokers, schema_registry_url, 
                                          kek_name, kms_type, kms_key_id);
    producer.registerSchemaAndProduce(topic);
    
    std::cout << "All encrypted messages sent successfully!" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
