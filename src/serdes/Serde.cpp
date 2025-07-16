#include "srclient/serdes/Serde.h"
#include <google/protobuf/message.h>

namespace srclient::serdes {

SerdeSchema::SerdeSchema(Type type, const std::string& schema_data) 
    : type_(type), schema_data_(schema_data) {}

SerdeSchema::SerdeSchema(Type type, const std::pair<avro::ValidSchema, std::vector<avro::ValidSchema>>& avro_schema) 
    : type_(type), avro_schema_(avro_schema) {}

} // namespace srclient::serdes 