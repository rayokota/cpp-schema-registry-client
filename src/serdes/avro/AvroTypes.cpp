#include "schemaregistry/serdes/avro/AvroTypes.h"

namespace schemaregistry::serdes::avro {

// Factory registration for Avro format
namespace {
// Static initialization to register Avro factories
bool avro_factories_registered = []() {
    // Register string factory
    SerdeValueFactory::registerStringFactory(
        SerdeFormat::Avro,
        [](const std::string& value) -> std::unique_ptr<SerdeValue> {
            ::avro::GenericDatum datum(value);
            return std::make_unique<AvroValue>(datum);
        });

    // Register bytes factory
    SerdeValueFactory::registerBytesFactory(
        SerdeFormat::Avro,
        [](const std::vector<uint8_t>& value) -> std::unique_ptr<SerdeValue> {
            ::avro::GenericDatum datum(value);
            return std::make_unique<AvroValue>(datum);
        });

    return true;
}();
}  // namespace

// Implementation for AvroValue methods
bool AvroValue::asBool() const {
    if (value_.type() == ::avro::AVRO_BOOL) {
        return value_.value<bool>();
    }
    // Default to true for non-boolean types (matching Rust behavior)
    return true;
}

std::string AvroValue::asString() const {
    if (value_.type() == ::avro::AVRO_STRING) {
        return value_.value<std::string>();
    }
    // Return empty string for non-string types (matching Rust behavior)
    return "";
}

std::vector<uint8_t> AvroValue::asBytes() const {
    if (value_.type() == ::avro::AVRO_BYTES) {
        return value_.value<std::vector<uint8_t>>();
    }
    // Return empty vector for non-bytes types (matching Rust behavior)
    return std::vector<uint8_t>();
}

nlohmann::json AvroValue::asJson() const {
    throw AvroError("Avro SerdeValue cannot be converted to json");
}

// Utility function implementation
::avro::GenericDatum asAvro(const SerdeValue& value) {
    if (value.getFormat() != SerdeFormat::Avro) {
        throw std::invalid_argument("SerdeValue is not Avro");
    }
    return value.getValue<::avro::GenericDatum>();
}

}  // namespace schemaregistry::serdes::avro