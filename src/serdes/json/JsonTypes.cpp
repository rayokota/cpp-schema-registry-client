#include "schemaregistry/serdes/json/JsonTypes.h"

namespace schemaregistry::serdes::json {

// Utility functions for JSON value and schema extraction
jsoncons::ojson asOJson(const SerdeValue &value) {
    if (value.getFormat() != SerdeFormat::Json) {
        throw std::invalid_argument("SerdeValue is not JSON");
    }
    return jsoncons::ojson::parse(value.getValue<nlohmann::json>().dump());
}

nlohmann::json asJson(const SerdeValue &value) {
    if (value.getFormat() != SerdeFormat::Json) {
        throw std::invalid_argument("SerdeValue is not JSON");
    }
    return value.getValue<nlohmann::json>();
}

}  // namespace schemaregistry::serdes::json