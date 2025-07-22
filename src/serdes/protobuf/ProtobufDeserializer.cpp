#define SRCLIENT_PROTOBUF_SKIP_TEMPLATE_IMPL
#include "srclient/serdes/protobuf/ProtobufDeserializer.h"
#include "srclient/serdes/protobuf/ProtobufUtils.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include "srclient/serdes/json/JsonTypes.h"
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/json_util.h>
#include <nlohmann/json.hpp>
#include <optional>
#include <functional>

// Forward declaration for transformFields function from ProtobufUtils.cpp
namespace srclient::serdes::protobuf::utils {
    std::unique_ptr<srclient::serdes::SerdeValue> transformFields(
        srclient::serdes::RuleContext& ctx,
        const std::string& field_executor_type,
        const srclient::serdes::SerdeValue& value
    );
}

namespace srclient::serdes::protobuf {

} // namespace srclient::serdes::protobuf