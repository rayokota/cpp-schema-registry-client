#include "srclient/serdes/JsonDeserializer.h"
#include "srclient/serdes/JsonUtils.h"

namespace srclient::serdes {

using namespace json_utils;

// Explicit template instantiation
template class JsonDeserializer<srclient::rest::ISchemaRegistryClient>;

} // namespace srclient::serdes 