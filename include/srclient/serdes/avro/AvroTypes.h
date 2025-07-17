#pragma once

#include <memory>
#include <string>
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
    
    nlohmann::json asJson() const override { 
        throw SerdeError("SerdeValue is not JSON"); 
    }
    ::avro::GenericDatum asAvro() const override { return value_; }
    google::protobuf::Message& asProtobuf() const override { 
        throw SerdeError("SerdeValue is not Protobuf"); 
    }
    
    SerdeFormat getFormat() const override { return SerdeFormat::Avro; }
    
    std::unique_ptr<SerdeValue> clone() const override {
        return std::make_unique<AvroValue>(value_);
    }
    
    // Direct access to the Avro value
    const ::avro::GenericDatum& getValue() const { return value_; }
    ::avro::GenericDatum& getValue() { return value_; }
};

// Helper functions for creating Avro SerdeValue instances
inline std::unique_ptr<SerdeValue> makeAvroValue(const ::avro::GenericDatum& value) {
    return std::make_unique<AvroValue>(value);
}

inline std::unique_ptr<SerdeValue> makeAvroValue(::avro::GenericDatum&& value) {
    return std::make_unique<AvroValue>(std::move(value));
}



} // namespace srclient::serdes::avro 