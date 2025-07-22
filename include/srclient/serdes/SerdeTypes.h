#pragma once

#include <any>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

// Include actual schema model types
#include "srclient/rest/model/RegisteredSchema.h"
#include "srclient/rest/model/Rule.h"
#include "srclient/rest/model/RuleSet.h"
#include "srclient/rest/model/Schema.h"
#include "srclient/serdes/SerdeError.h"

namespace srclient::rest {
class ClientConfiguration;
}

namespace srclient::serdes {

// Forward declarations for specific value classes
namespace avro {
class AvroValue;
}
namespace json {
class JsonValue;
}
namespace protobuf {
template <typename T>
class ProtobufValue;
}

/**
 * Serialization format types
 */
enum class SerdeFormat { Avro, Json, Protobuf };

/**
 * Base interface for serialization objects of different formats
 */
class SerdeObject {
  public:
    virtual ~SerdeObject() = default;

    // Pure virtual method to get type-erased access to the raw value
    // Returns const void* to the underlying value
    virtual const void *getRawObject() const = 0;

    // Pure virtual method to get mutable access to the raw value
    virtual void *getMutableRawObject() = 0;

    // Get the format type
    virtual SerdeFormat getFormat() const = 0;

    // Get the type info for the contained value
    virtual const std::type_info &getType() const = 0;

    // Pure virtual methods for moving values in and out
    virtual void moveFrom(SerdeObject &&other) = 0;

    // Template method to safely cast and access the value
    template <typename T>
    const T &getObject() const {
        if (typeid(T) != getType()) {
            throw std::bad_cast();
        }
        return *static_cast<const T *>(getRawObject());
    }

    // Template method to safely cast and get mutable access
    template <typename T>
    T &getMutableObject() {
        if (typeid(T) != getType()) {
            throw std::bad_cast();
        }
        return *static_cast<T *>(getMutableRawObject());
    }

    // Template method to move a value into this SerdeObject
    template <typename T>
    void setObject(T &&value) {
        if (typeid(std::decay_t<T>) != getType()) {
            throw std::bad_cast();
        }
        *static_cast<T *>(getMutableRawObject()) = std::forward<T>(value);
    }

    // Template method to move a value out of this SerdeObject
    template <typename T>
    T moveObject() {
        if (typeid(T) != getType()) {
            throw std::bad_cast();
        }
        return std::move(*static_cast<T *>(getMutableRawObject()));
    }
};

/**
 * Base interface for serialization values of different formats
 */
class SerdeValue : public SerdeObject {
  public:
    virtual ~SerdeValue() = default;

    // Value methods that delegate to Object methods from superclass
    virtual const void *getRawValue() const { return getRawObject(); }
    virtual void *getMutableRawValue() { return getMutableRawObject(); }

    // Template methods that delegate to Object methods from superclass
    template <typename T>
    const T &getValue() const {
        return getObject<T>();
    }

    template <typename T>
    T &getMutableValue() {
        return getMutableObject<T>();
    }

    template <typename T>
    void setValue(T &&value) {
        setObject<T>(std::forward<T>(value));
    }

    template <typename T>
    T moveValue() {
        return moveObject<T>();
    }

    virtual std::unique_ptr<SerdeValue> clone() const = 0;

    // Static factory methods for creating SerdeValue instances
    static std::unique_ptr<SerdeValue> newString(SerdeFormat format,
                                                 const std::string &value);
    static std::unique_ptr<SerdeValue> newBytes(
        SerdeFormat format, const std::vector<uint8_t> &value);

    // Value extraction methods
    virtual bool asBool() const = 0;
    virtual std::string asString() const = 0;
    virtual std::vector<uint8_t> asBytes() const = 0;
};

/**
 * Base interface for schema wrappers
 * Based on SerdeSchema from serde.rs
 */
class SerdeSchema : public SerdeObject {
  public:
    virtual ~SerdeSchema() = default;

    // Schema methods that delegate to Object methods from superclass
    virtual const void *getRawSchema() const { return getRawObject(); }
    virtual void *getMutableRawSchema() { return getMutableRawObject(); }

    // Template methods that delegate to Object methods from superclass
    template <typename T>
    const T &getSchema() const {
        return getObject<T>();
    }

    template <typename T>
    T &getMutableSchema() {
        return getMutableObject<T>();
    }

    template <typename T>
    void setSchema(T &&schema) {
        setObject<T>(std::forward<T>(schema));
    }

    template <typename T>
    T moveSchema() {
        return moveObject<T>();
    }

    // Clone method
    virtual std::unique_ptr<SerdeSchema> clone() const = 0;
};

// Magic bytes for schema ID encoding (from serde.rs)
constexpr uint8_t MAGIC_BYTE_V0 = 0;
constexpr uint8_t MAGIC_BYTE_V1 = 1;

// Header keys for schema IDs (from serde.rs)
constexpr const char *KEY_SCHEMA_ID_HEADER = "__key_schema_id";
constexpr const char *VALUE_SCHEMA_ID_HEADER = "__value_schema_id";

/**
 * Type of serialization operation (from serde.rs)
 */
enum class SerdeType { Key, Value };

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
enum class SchemaSelector { SchemaId, LatestVersion, LatestWithMetadata };

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
    static SchemaSelectorData createLatestWithMetadata(
        const std::unordered_map<std::string, std::string> &metadata);
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
    RuleOverride(const std::string &t,
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

    Migration(Mode mode, std::optional<RegisteredSchema> src = std::nullopt,
              std::optional<RegisteredSchema> tgt = std::nullopt);
};

/**
 * Function type aliases for strategies and serializers (from config.rs and
 * serde.rs)
 */
using SubjectNameStrategy = std::function<std::optional<std::string>(
    const std::string &topic, SerdeType serde_type,
    const std::optional<Schema> &schema)>;

using SchemaIdSerializer = std::function<std::vector<uint8_t>(
    const std::vector<uint8_t> &payload,
    const struct SerializationContext &ser_ctx, const SchemaId &schema_id)>;

using SchemaIdDeserializer = std::function<size_t(
    const std::vector<uint8_t> &payload,
    const struct SerializationContext &ser_ctx, SchemaId &schema_id)>;

// Function signature for field transformation
using FieldTransformer = std::function<std::unique_ptr<SerdeValue>(
    RuleContext &ctx, const std::string &rule_type, const SerdeValue &msg)>;

/**
 * Cache for parsed schemas (from serde.rs)
 */
template <typename T>
class ParsedSchemaCache {
  private:
    std::unordered_map<std::string, T> cache_;
    mutable std::mutex mutex_;

  public:
    void set(const Schema &schema, const T &parsed_schema);
    std::optional<T> get(const Schema &schema) const;
    void clear();

  private:
    std::string getSchemaKey(const Schema &schema) const;
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
SerdeFormat stringToFormat(const std::string &format);

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
}  // namespace type_utils

}  // namespace srclient::serdes