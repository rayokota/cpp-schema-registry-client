#pragma once

#include <any>
#include <avro/Compiler.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Exception.hh>
#include <avro/Generic.hh>
#include <avro/Specific.hh>
#include <avro/ValidSchema.hh>
#include <memory>
#include <string>

#include "srclient/serdes/SerdeError.h"
#include "srclient/serdes/SerdeTypes.h"

namespace srclient::serdes::avro {

/**
 * Avro-specific serialization errors
 */
class AvroError : public SerdeError {
  public:
    explicit AvroError(const std::string &message)
        : SerdeError("Avro error: " + message) {}

    explicit AvroError(const ::avro::Exception &avro_ex)
        : SerdeError("Avro error: " + std::string(avro_ex.what())) {}
};

/**
 * Avro implementation of SerdeValue
 */
class AvroValue : public SerdeValue {
  private:
    ::avro::GenericDatum value_;

  public:
    explicit AvroValue(const ::avro::GenericDatum &value) : value_(value) {}
    explicit AvroValue(::avro::GenericDatum &&value)
        : value_(std::move(value)) {}

    // SerdeValue interface implementation
    const void *getRawValue() const override { return &value_; }
    void *getMutableRawValue() override { return &value_; }
    SerdeFormat getFormat() const override { return SerdeFormat::Avro; }
    const std::type_info &getType() const override {
        return typeid(::avro::GenericDatum);
    }

    std::unique_ptr<SerdeValue> clone() const override {
        return std::make_unique<AvroValue>(value_);
    }

    void moveFrom(SerdeValue &&other) override {
        if (other.getFormat() == SerdeFormat::Avro) {
            value_ = std::move(*static_cast<::avro::GenericDatum *>(
                other.getMutableRawValue()));
        }
    }

    // Value extraction methods
    bool asBool() const override;
    std::string asString() const override;
    std::vector<uint8_t> asBytes() const override;
};

// Helper functions for creating Avro SerdeValue instances
inline std::unique_ptr<SerdeValue> makeAvroValue(
    const ::avro::GenericDatum &value) {
    return std::make_unique<AvroValue>(value);
}

inline std::unique_ptr<SerdeValue> makeAvroValue(::avro::GenericDatum &&value) {
    return std::make_unique<AvroValue>(std::move(value));
}

// Utility functions for Avro value and schema extraction
::avro::GenericDatum asAvro(const SerdeValue &value);

}  // namespace srclient::serdes::avro