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
#include "srclient/serdes/SerdeBase.h"

namespace srclient::serdes::avro {

/**
 * Exception wrapper for Avro-specific errors
 */
class AvroSerdeError : public SerdeError {
public:
    explicit AvroSerdeError(const std::string& message) 
        : SerdeError("Avro error: " + message) {}
    
    explicit AvroSerdeError(const ::avro::Exception& avro_ex)
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
    
    bool isJson() const override { return false; }
    bool isAvro() const override { return true; }
    bool isProtobuf() const override { return false; }
    
    std::any getValue() const override { return value_; }
    
    SerdeFormat getFormat() const override { return SerdeFormat::Avro; }
    
    std::unique_ptr<SerdeValue> clone() const override {
        return std::make_unique<AvroValue>(value_);
    }

};

/**
 * Avro Schema implementation
 */
class AvroSchema : public SerdeSchema {
private:
    std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>> avro_schema_;
    
public:
    explicit AvroSchema(const std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>& avro_schema)
        : avro_schema_(avro_schema) {}
    
    bool isAvro() const override { return true; }
    bool isJson() const override { return false; }
    bool isProtobuf() const override { return false; }
    
    SerdeSchemaFormat getFormat() const override { return SerdeSchemaFormat::Avro; }
    
    std::string getSchemaData() const override {
        // Convert Avro schema to string representation
        return avro_schema_.first.toJson(false);
    }
    
    std::optional<std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>> getAvroSchema() const override {
        return avro_schema_;
    }
    
    std::unique_ptr<SerdeSchema> clone() const override {
        return std::make_unique<AvroSchema>(avro_schema_);
    }
    
    // Direct access to Avro schema
    const std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>& getAvroSchemaPair() const {
        return avro_schema_;
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



} // namespace srclient::serdes::avro 