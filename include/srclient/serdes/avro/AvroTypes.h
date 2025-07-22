#pragma once

#include <memory>
#include <string>
#include <any>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Compiler.hh>
#include <avro/ValidSchema.hh>
#include <avro/Generic.hh>
#include <avro/Specific.hh>
#include <avro/Exception.hh>
#include <nlohmann/json.hpp>
#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"

namespace srclient::serdes::avro {

/**
 * Avro-specific serialization errors
 */
class AvroError : public SerdeError {
public:
    explicit AvroError(const std::string& message) : SerdeError("Avro error: " + message) {}

    explicit AvroError(const ::avro::Exception& avro_ex)
        : SerdeError("Avro error: " + std::string(avro_ex.what())) {}

};

/**
 * Avro implementation of SerdeValue
 */
class AvroValue : public SerdeValue {
private:
    ::avro::GenericDatum value_;
    
public:
    explicit AvroValue(const ::avro::GenericDatum& value) : value_(value) {}
    explicit AvroValue(::avro::GenericDatum&& value) : value_(std::move(value)) {}
    
    // SerdeObject interface implementation
    const void* getRawObject() const override { return &value_; }
    void* getMutableRawObject() override { return &value_; }
    SerdeFormat getFormat() const override { return SerdeFormat::Avro; }
    const std::type_info& getType() const override { return typeid(::avro::GenericDatum); }
    
    std::unique_ptr<SerdeValue> clone() const override {
        return std::make_unique<AvroValue>(value_);
    }
    
    void moveFrom(SerdeObject&& other) override {
        if (other.getFormat() == SerdeFormat::Avro) {
            value_ = std::move(*static_cast<::avro::GenericDatum*>(other.getMutableRawObject()));
        }
    }

    // Value extraction methods
    bool asBool() const override;
    std::string asString() const override;
    std::vector<uint8_t> asBytes() const override;

};

/**
 * Avro Schema implementation
 */
class AvroSchema : public SerdeSchema {
private:
    std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>> schema_;
    
public:
    explicit AvroSchema(const std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>& schema)
        : schema_(schema) {}
    
    // SerdeObject interface methods
    const void* getRawObject() const override { return &schema_; }
    void* getMutableRawObject() override { return &schema_; }
    SerdeFormat getFormat() const override { return SerdeFormat::Avro; }
    const std::type_info& getType() const override { 
        return typeid(std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>); 
    }
    
    void moveFrom(SerdeObject&& other) override {
        if (auto* avro_other = dynamic_cast<AvroSchema*>(&other)) {
            schema_ = std::move(avro_other->schema_);
        } else {
            throw std::bad_cast();
        }
    }
    
    std::unique_ptr<SerdeSchema> clone() const override {
        return std::make_unique<AvroSchema>(schema_);
    }
    
    // Direct access to Avro schema
    const std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>& getAvroSchemaPair() const {
        return schema_;
    }
};

// Helper functions for creating Avro SerdeValue instances
inline std::unique_ptr<SerdeValue> makeAvroValue(const ::avro::GenericDatum& value) {
    return std::make_unique<AvroValue>(value);
}

inline std::unique_ptr<SerdeValue> makeAvroValue(::avro::GenericDatum&& value) {
    return std::make_unique<AvroValue>(std::move(value));
}

// Helper function for creating Avro SerdeSchema instances
inline std::unique_ptr<SerdeSchema> makeAvroSchema(const std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>& avro_schema) {
    return std::make_unique<AvroSchema>(avro_schema);
}

// Utility functions for Avro value and schema extraction
::avro::GenericDatum asAvro(const SerdeValue& value);

inline std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>> asAvroSchema(const SerdeSchema& schema) {
    if (schema.getFormat() != SerdeFormat::Avro) {
        throw std::invalid_argument("Schema is not an Avro schema");
    }
    return schema.getSchema<std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>>();
}


} // namespace srclient::serdes::avro 