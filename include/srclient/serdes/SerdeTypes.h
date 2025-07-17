#pragma once

#include <any>
#include <cstdint>
#include <string>
#include <vector>
#include <optional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <mutex>
#include <functional>
#include <nlohmann/json.hpp>
#include <avro/ValidSchema.hh>
#include <avro/GenericDatum.hh>
#include <google/protobuf/message.h>

// Include actual schema model types
#include "srclient/rest/model/Schema.h"
#include "srclient/rest/model/RegisteredSchema.h"
#include "srclient/rest/model/Rule.h"
#include "srclient/rest/model/RuleSet.h"

#include "srclient/serdes/SerdeError.h"

namespace srclient::rest {
    class ClientConfiguration;
}

namespace srclient::serdes {

/**
 * Serialization format types
 */
enum class SerdeFormat {
    Avro,
    Json,
    Protobuf
};

/**
 * Base interface for serialization values of different formats
 */
class SerdeValue {
public:
    virtual ~SerdeValue() = default;
    
    // Type checking methods
    virtual bool isJson() const = 0;
    virtual bool isAvro() const = 0;
    virtual bool isProtobuf() const = 0;
    
    // Value access method - returns std::any, use std::any_cast to get the actual type
    virtual std::any getValue() const = 0;
    
    // Get the format type
    virtual SerdeFormat getFormat() const = 0;
    
    // Clone method for copying
    virtual std::unique_ptr<SerdeValue> clone() const = 0;
};

/**
 * Base interface for schema wrappers
 * Based on SerdeSchema from serde.rs
 */
class SerdeSchema {
public:
    virtual ~SerdeSchema() = default;
    
    // Type checking methods
    virtual bool isAvro() const = 0;
    virtual bool isJson() const = 0;
    virtual bool isProtobuf() const = 0;
    
    // Format accessor
    virtual SerdeFormat getFormat() const = 0;
    
    // Schema data access method - returns appropriate schema representation for the format
    virtual std::any getSchema() const = 0;
    
    // Clone method
    virtual std::unique_ptr<SerdeSchema> clone() const = 0;
};

/**
 * Helper functions for extracting schema data from SerdeSchema::getSchema()
 */
inline std::string getSchemaData(const SerdeSchema& schema) {
    if (schema.isAvro()) {
        // For Avro schemas, convert the ValidSchema to JSON string
        auto avro_schema = std::any_cast<std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>>(schema.getSchema());
        return avro_schema.first.toJson(false);
    } else {
        // For JSON and Protobuf schemas, return the string directly
        return std::any_cast<std::string>(schema.getSchema());
    }
}

inline std::optional<std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>> getAvroSchema(const SerdeSchema& schema) {
    if (!schema.isAvro()) {
        return std::nullopt;
    }
    return std::any_cast<std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>>(schema.getSchema());
}

// Backward compatibility helper functions (for easier migration)
inline bool isJson(const SerdeValue& value) {
    return value.isJson();
}

inline bool isAvro(const SerdeValue& value) {
    return value.isAvro();
}

inline bool isProtobuf(const SerdeValue& value) {
    return value.isProtobuf();
}

inline nlohmann::json asJson(const SerdeValue& value) {
    if (!value.isJson()) {
        throw SerdeError("SerdeValue is not JSON");
    }
    return std::any_cast<nlohmann::json>(value.getValue());
}

inline ::avro::GenericDatum asAvro(const SerdeValue& value) {
    if (!value.isAvro()) {
        throw SerdeError("SerdeValue is not Avro");
    }
    return std::any_cast<::avro::GenericDatum>(value.getValue());
}

inline google::protobuf::Message& asProtobuf(const SerdeValue& value) {
    if (!value.isProtobuf()) {
        throw SerdeError("SerdeValue is not Protobuf");
    }
    return std::any_cast<std::reference_wrapper<google::protobuf::Message>>(value.getValue()).get();
}

// Magic bytes for schema ID encoding (from serde.rs)
constexpr uint8_t MAGIC_BYTE_V0 = 0;
constexpr uint8_t MAGIC_BYTE_V1 = 1;

// Header keys for schema IDs (from serde.rs)
constexpr const char* KEY_SCHEMA_ID_HEADER = "__key_schema_id";
constexpr const char* VALUE_SCHEMA_ID_HEADER = "__value_schema_id";

/**
 * Type of serialization operation (from serde.rs)
 */
enum class SerdeType {
    Key,
    Value
};

/**
 * Field types for rule processing (from serde.rs)
 */
enum class FieldType {
    Record,
    Enum,
    Array,
    Map,
    Combined,
    Fixed,
    String,
    Bytes,
    Int,
    Long,
    Float,
    Double,
    Boolean,
    Null
};

/**
 * Convert FieldType to string representation
 */
std::string fieldTypeToString(FieldType type);

// Forward declarations for serdes classes
class SerdeSchema;
class SerdeHeaders;
class SchemaId;
class RuleContext;
class FieldContext;
class RuleRegistry;

// Forward declarations for rule interfaces
class RuleBase;
class RuleExecutor;
class FieldRuleExecutor;
class RuleAction;

// Type aliases for existing schema model types
using Schema = srclient::rest::model::Schema;
using RegisteredSchema = srclient::rest::model::RegisteredSchema;
using Rule = srclient::rest::model::Rule;
using RuleSet = srclient::rest::model::RuleSet;
using Mode = srclient::rest::model::Mode;
using Phase = srclient::rest::model::Phase;
using Kind = srclient::rest::model::Kind;
using ClientConfiguration = srclient::rest::ClientConfiguration;

/**
 * Schema selector options for serialization/deserialization (from config.rs)
 */
enum class SchemaSelector {
    SchemaId,
    LatestVersion,
    LatestWithMetadata
};

/**
 * Schema selector with associated data (from config.rs)
 */
struct SchemaSelectorData {
    SchemaSelector type;
    std::optional<int32_t> schema_id;
    std::unordered_map<std::string, std::string> metadata;
    
    // Static factory methods
    static SchemaSelectorData createSchemaId(int32_t id);
    static SchemaSelectorData createLatestVersion();
    static SchemaSelectorData createLatestWithMetadata(const std::unordered_map<std::string, std::string>& metadata);
};

/**
 * Rule override configuration (from rule_registry.rs)
 */
struct RuleOverride {
    std::string type;
    std::optional<std::string> on_success;
    std::optional<std::string> on_failure;
    std::optional<bool> disabled;
    
    RuleOverride() = default;
    RuleOverride(const std::string& t, 
                std::optional<std::string> success = std::nullopt,
                std::optional<std::string> failure = std::nullopt,
                std::optional<bool> dis = std::nullopt)
        : type(t), on_success(success), on_failure(failure), disabled(dis) {}
};

/**
 * Migration information for schema evolution (from serde.rs)
 */
struct Migration {
    Mode rule_mode;
    std::optional<RegisteredSchema> source;
    std::optional<RegisteredSchema> target;
    
    Migration(Mode mode, 
              std::optional<RegisteredSchema> src = std::nullopt,
              std::optional<RegisteredSchema> tgt = std::nullopt);
};

/**
 * Function type aliases for strategies and serializers (from config.rs and serde.rs)
 */
using SubjectNameStrategy = std::function<std::optional<std::string>(
    const std::string& topic,
    SerdeType serde_type,
    const std::optional<Schema>& schema
)>;

using SchemaIdSerializer = std::function<std::vector<uint8_t>(
    const std::vector<uint8_t>& payload,
    const struct SerializationContext& ser_ctx,
    const SchemaId& schema_id
)>;

using SchemaIdDeserializer = std::function<size_t(
    const std::vector<uint8_t>& payload,
    const struct SerializationContext& ser_ctx,
    SchemaId& schema_id
)>;

// Function signature for field transformation
using FieldTransformer = std::function<SerdeValue&(
    RuleContext& ctx, 
    const std::string& rule_type,
    SerdeValue& msg
)>;

/**
 * Cache for parsed schemas (from serde.rs)
 */
template<typename T>
class ParsedSchemaCache {
private:
    std::unordered_map<std::string, T> cache_;
    mutable std::mutex mutex_;
    
public:
    void set(const Schema& schema, const T& parsed_schema);
    std::optional<T> get(const Schema& schema) const;
    void clear();
    
private:
    std::string getSchemaKey(const Schema& schema) const;
};

/**
 * Utility functions for type conversion
 */
namespace type_utils {
    /**
     * Convert SerdeFormat to string
     */
    std::string formatToString(SerdeFormat format);
    
    /**
     * Convert string to SerdeFormat
     */
    SerdeFormat stringToFormat(const std::string& format);
    
    /**
     * Convert SerdeType to string
     */
    std::string typeToString(SerdeType type);
    
    /**
     * Convert Mode to string
     */
    std::string modeToString(Mode mode);
    
    /**
     * Convert Phase to string
     */
    std::string phaseToString(Phase phase);
    
    /**
     * Convert Kind to string
     */
    std::string kindToString(Kind kind);
}

} // namespace srclient::serdes 