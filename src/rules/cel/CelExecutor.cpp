#include "srclient/rules/cel/CelExecutor.h"
#include "srclient/rules/cel/ExtraFunc.h"
#include "srclient/serdes/json/JsonTypes.h"
#include "srclient/serdes/avro/AvroTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include "srclient/serdes/Serde.h"
#include "parser/parser.h"
#include "runtime/activation.h"
#include "absl/strings/str_split.h"
#include "eval/public/activation.h"
#include "eval/public/builtin_func_registrar.h"
#include "eval/public/cel_expr_builder_factory.h"
#include "eval/public/string_extension_func_registrar.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include <regex>

namespace srclient::rules::cel {

using namespace srclient::serdes;

CelExecutor::CelExecutor() {
    auto runtime_result = newRuleBuilder(nullptr);
    if (!runtime_result.ok()) {
        throw SerdeError("Failed to create CEL runtime: " + std::string(runtime_result.status().message()));
    }
    runtime_ = std::move(runtime_result.value());
}

CelExecutor::CelExecutor(std::unique_ptr<const google::api::expr::runtime::CelExpressionBuilder> runtime)
    : runtime_(std::move(runtime)) {
    if (!runtime_) {
        throw SerdeError("CEL runtime cannot be null");
    }
}

absl::StatusOr<std::unique_ptr<google::api::expr::runtime::CelExpressionBuilder>> CelExecutor::newRuleBuilder(
    google::protobuf::Arena* arena) {
  google::api::expr::runtime::InterpreterOptions options;
  options.enable_qualified_type_identifiers = true;
  options.enable_timestamp_duration_overflow_errors = true;
  options.enable_heterogeneous_equality = true;
  options.enable_empty_wrapper_null_unboxing = true;
  options.enable_regex_precompilation = true;
  options.constant_folding = true;
  options.constant_arena = arena;

  std::unique_ptr<google::api::expr::runtime::CelExpressionBuilder> builder =
      google::api::expr::runtime::CreateCelExpressionBuilder(options);
  auto register_status = google::api::expr::runtime::RegisterBuiltinFunctions(builder->GetRegistry(), options);
  if (!register_status.ok()) {
    return register_status;
  }
  register_status = google::api::expr::runtime::RegisterStringExtensionFunctions(builder->GetRegistry());
  if (!register_status.ok()) {
    return register_status;
  }
  register_status = RegisterExtraFuncs(*builder->GetRegistry(), arena);
  if (!register_status.ok()) {
    return register_status;
  }
  return builder;
}


SerdeValue& CelExecutor::transform(RuleContext& ctx, SerdeValue& msg) {
    absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue> args;
    args.emplace("msg", fromSerdeValue(msg));

    auto result = execute(ctx, msg, args);
    if (result) {
        // Since the original message is non-const, we can move the result
        msg = std::move(*result);
    }
    return msg;
}

std::unique_ptr<SerdeValue> CelExecutor::execute(RuleContext& ctx, 
                               const SerdeValue& msg, 
                               const absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue>& args) {
    // TODO fix value
    std::string expr = ctx.getRule().getExpr().value();
    std::vector<absl::string_view> parts = absl::StrSplit(expr, ";");
    
    if (parts.size() > 1) {
        absl::string_view guard = parts[0];
        if (!guard.empty()) {
            auto guard_result_value = executeRule(ctx, msg, std::string(guard), args);
            if (guard_result_value) {
                auto cel_value = toSerdeValue(msg, *guard_result_value);
                 if (cel_value && cel_value->Is<google::api::expr::runtime::BoolValue>() && !cel_value->As<cel::BoolValue>().value()) {
                    return msg.clone();
                }
            }
        }
        expr = std::string(parts[1]);
    }

    return executeRule(ctx, msg, expr, args);
}

std::unique_ptr<google::api::expr::runtime::CelValue> CelExecutor::executeRule(RuleContext& ctx,
                                   const SerdeValue& msg,
                                   const std::string& expr,
                                   const absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue>& args) {
    auto parsed_expr_status = getOrCompileExpression(expr);
    if (!parsed_expr_status.ok()) {
        throw SerdeError("CEL expression compilation failed: " + std::string(parsed_expr_status.status().message()));
    }
    auto parsed_expr = parsed_expr_status.value();

    ::cel::Activation activation;
    for (const auto& pair : args) {
        activation.InsertValue(pair.first, pair.second);
    }
    // TODO reuse arena
    google::protobuf::Arena arena;
    auto eval_status = parsed_expr->Evaluate(activation, &arena);
    if (!eval_status.ok()) {
        throw SerdeError("CEL evaluation failed: " + std::string(eval_status.status().message()));
    }

    return std::make_unique<google::api::expr::runtime::CelValue>(eval_status.value());
}

absl::StatusOr<google::api::expr::runtime::CelExpression> CelExecutor::getOrCompileExpression(const std::string& expr) {
    absl::MutexLock lock(&cache_mutex_);
    
    auto it = expression_cache_.find(expr);
    if (it != expression_cache_.end()) {
        return it->second;
    }
    
    auto pexpr_or = ::cel::parser::Parse(rule.expression());
    if (!pexpr_or.ok()) {
        return pexpr_or.status();
    }
    ::cel::expr::ParsedExpr pexpr = std::move(pexpr_or).value();
    auto expr_or = runtime_.CreateExpression(&pexpr.expr(), &pexpr.source_info());
    if (!expr_or.ok()) {
        return expr_or.status();
    }
    std::unique_ptr<google::api::expr::runtime::CelExpression> expr = std::move(expr_or).value();
    expression_cache_.emplace(expr, std::move(expr));
    return expr;
}

google::api::expr::runtime::CelValue CelExecutor::fromSerdeValue(const SerdeValue& value) {
    /*
    switch (value.getFormat()) {
        case SerdeFormat::Json:
            return fromJsonValue(value.getValue());
        case SerdeFormat::Avro:
            return fromAvroValue(value.getValue());
        case SerdeFormat::Protobuf:
            return fromProtobufValue(*value.protobuf_value);
        default:
            return google::api::expr::runtime::CelValue::CreateNull();
    }
    */
    return google::api::expr::runtime::CelValue::CreateNull(); // Placeholder, implement actual conversion
}

std::unique_ptr<SerdeValue> CelExecutor::toSerdeValue(const SerdeValue& original, const google::api::expr::runtime::CelValue& cel_value) {
    auto result = std::make_unique<SerdeValue>();
    result->schema = original.schema;
    result->type = original.type;
    switch (original.type) {
        case SerdeType::JSON:
            result->json_value = toJsonValue(original.json_value, cel_value);
            break;
        case SerdeType::AVRO:
            result->avro_value = toAvroValue(original.avro_value, cel_value);
            break;
        case SerdeType::PROTOBUF:
            result->protobuf_value = toProtobufValue(*original.protobuf_value, cel_value);
            break;
    }
    return result;
}

google::api::expr::runtime::CelValue CelExecutor::fromJsonValue(const nlohmann::json& json) {
    if (json.is_null()) return google::api::expr::runtime::CelValue::CreateNull();
    if (json.is_boolean()) return google::api::expr::runtime::CelValue::CreateBool(json.get<bool>());
    if (json.is_number_integer()) return google::api::expr::runtime::CelValue::CreateInt64(json.get<int64_t>());
    if (json.is_number_unsigned()) return google::api::expr::runtime::CelValue::CreateUint64(json.get<uint64_t>());
    if (json.is_number_float()) return google::api::expr::runtime::CelValue::CreateDouble(json.get<double>());
    if (json.is_string()) return google::api::expr::runtime::CelValue::CreateString(json.get<std::string>());
    // TODO
    /*
    if (json.is_array()) {
        std::vector<google::api::expr::runtime::CelValue> vec;
        for (const auto& item : json) {
            vec.push_back(fromJsonValue(item));
        }
        auto list_value = value_manager.CreateListValue(cel::ListType(), vec);
        return list_value.value();
    }
    if (json.is_object()) {
        auto map_builder = value_manager.NewMapValueBuilder(cel::MapType());
        for (auto& el : json.items()) {
            map_builder->Add(value_manager.CreateStringValue(el.key()), fromJsonValue(el.value(), value_manager));
        }
        return std::move(*map_builder).Build();
    }
    */
    return google::api::expr::runtime::CelValue::CreateNull();
}

nlohmann::json CelExecutor::toJsonValue(const nlohmann::json& original, const google::api::expr::runtime::CelValue& cel_value) {
    // This is a simplified conversion. A full implementation would require more type checking.
    if (cel_value.Is<cel::IntValue>()) return nlohmann::json(cel_value.As<cel::IntValue>().value());
    if (cel_value.Is<cel::UintValue>()) return nlohmann::json(cel_value.As<cel::UintValue>().value());
    if (cel_value.Is<cel::DoubleValue>()) return nlohmann::json(cel_value.As<cel::DoubleValue>().value());
    if (cel_value.Is<cel::BoolValue>()) return nlohmann::json(cel_value.As<cel::BoolValue>().value());
    if (cel_value.Is<cel::StringValue>()) return nlohmann::json(std::string(cel_value.As<cel::StringValue>().value()));
    // More complex types like List and Map would need recursive conversion.
    return original;
}

google::api::expr::runtime::CelValue CelExecutor::fromAvroValue(const ::avro::GenericDatum& avro) {
    switch (avro.type()) {
        case ::avro::AVRO_BOOL: return google::api::expr::runtime::CelValue::CreateBool(avro.value<bool>());
        case ::avro::AVRO_INT: return google::api::expr::runtime::CelValue::CreateInt32(avro.value<int32_t>());
        case ::avro::AVRO_LONG: return google::api::expr::runtime::CelValue::CreateInt64(avro.value<int64_t>());
        case ::avro::AVRO_FLOAT: return google::api::expr::runtime::CelValue::CreateFloat(avro.value<float>());
        case ::avro::AVRO_DOUBLE: return google::api::expr::runtime::CelValue::CreateDouble(avro.value<double>());
        case ::avro::AVRO_STRING: return google::api::expr::runtime::CelValue::CreateString(avro.value<std::string>());
        case ::avro::AVRO_BYTES: return google::api::expr::runtime::CelValue::CreateBytes(avro.value<std::vector<uint8_t>>());
        // TODO
        /*
        case ::avro::AVRO_ARRAY: {
            const auto& arr = avro.value<::avro::GenericArray>().value();
            std::vector<google::api::expr::runtime::CelValue> vec;
            for (const auto& item : arr) {
                vec.push_back(fromAvroValue(item, value_manager));
            }
            auto list_value = value_manager.CreateListValue(cel::ListType(), vec);
            return list_value.value();
        }
        case ::avro::AVRO_MAP: {
             auto map_builder = value_manager.NewMapValueBuilder(cel::MapType());
            const auto& map = avro.value<::avro::GenericMap>().value();
            for (const auto& pair : map) {
                map_builder->Add(value_manager.CreateStringValue(pair.first), fromAvroValue(pair.second, value_manager));
            }
            return std::move(*map_builder).Build();
        }
        case ::avro::AVRO_RECORD: {
            auto map_builder = value_manager.NewMapValueBuilder(cel::MapType());
            const auto& record = avro.value<::avro::GenericRecord>();
            for (size_t i = 0; i < record.schema()->names(); ++i) {
                map_builder->Add(value_manager.CreateStringValue(record.schema()->nameAt(i)), fromAvroValue(record.fieldAt(i), value_manager));
            }
            return std::move(*map_builder).Build();
        }
        */
        case ::avro::AVRO_NULL: return google::api::expr::runtime::CelValue::CreateNull();
        default: return google::api::expr::runtime::CelValue::CreateNull();
    }
}

::avro::GenericDatum CelExecutor::toAvroValue(const ::avro::GenericDatum& original, const google::api::expr::runtime::CelValue& cel_value) {
    // Simplified conversion, similar to toJsonValue
    if (cel_value.Is<cel::IntValue>()) return ::avro::GenericDatum(cel_value.As<cel::IntValue>().value());
    if (cel_value.Is<cel::DoubleValue>()) return ::avro::GenericDatum(cel_value.As<cel::DoubleValue>().value());
    if (cel_value.Is<cel::BoolValue>()) return ::avro::GenericDatum(cel_value.As<cel::BoolValue>().value());
    if (cel_value.Is<cel::StringValue>()) return ::avro::GenericDatum(std::string(cel_value.As<cel::StringValue>().value()));
    return original;
}

std::unique_ptr<google::protobuf::Message> CelExecutor::toProtobufValue(const google::protobuf::Message& original, const google::api::expr::runtime::CelValue& cel_value) {
    auto new_msg = std::unique_ptr<google::protobuf::Message>(original.New());
    new_msg->CopyFrom(original);

    if (!cel_value.Is<cel::MapValue>()) {
        return new_msg;
    }
    // TODO: Full conversion from cel::MapValue to a protobuf message is complex and not yet implemented.
    // For now, we return a copy of the original message.
    return new_msg;
}

static google::api::expr::runtime::CelValue ConvertProtobufFieldToCel(const google::protobuf::Message& message,
                                              const google::protobuf::FieldDescriptor* field,
                                              int index = -1) {
    const auto* reflection = message.GetReflection();
    switch (field->cpp_type()) {
        case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
            return google::api::expr::runtime::CelValue::CreateInt32(index != -1 ? reflection->GetRepeatedInt32(message, field, index) : reflection->GetInt32(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
            return google::api::expr::runtime::CelValue::CreateInt64(index != -1 ? reflection->GetRepeatedInt64(message, field, index) : reflection->GetInt64(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
            return google::api::expr::runtime::CelValue::CreateUint32(index != -1 ? reflection->GetRepeatedUInt32(message, field, index) : reflection->GetUInt32(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
            return google::api::expr::runtime::CelValue::CreateUint64(index != -1 ? reflection->GetRepeatedUInt64(message, field, index) : reflection->GetUInt64(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
            return google::api::expr::runtime::CelValue::CreateFloat(index != -1 ? reflection->GetRepeatedFloat(message, field, index) : reflection->GetFloat(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
            return google::api::expr::runtime::CelValue::CreateDouble(index != -1 ? reflection->GetRepeatedDouble(message, field, index) : reflection->GetDouble(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
            return google::api::expr::runtime::CelValue::CreateBool(index != -1 ? reflection->GetRepeatedBool(message, field, index) : reflection->GetBool(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
            return google::api::expr::runtime::CelValue::CreateString(index != -1 ? reflection->GetRepeatedString(message, field, index) : reflection->GetString(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
            return google::api::expr::runtime::CelValue::CreateInt32(index != -1 ? reflection->GetRepeatedEnum(message, field, index)->number() : reflection->GetEnum(message, field)->number());
        // TODO
        /*
        case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE:
            {
                const auto& sub_message = index != -1 ? reflection->GetRepeatedMessage(message, field, index) : reflection->GetMessage(message, field);
                return CelExecutor::fromProtobufValue(sub_message, value_manager);
            }
        */
        default:
            return google::api:expr::runtime::CelValue::CreateNull();
    }
}

google::api::expr::runtime::CelValue CelExecutor::fromProtobufValue(const google::protobuf::Message& protobuf) {
    const auto* descriptor = protobuf.GetDescriptor();
    if (!descriptor) return google::api::expr::runtime::CelValue::CreateNull();
    const auto* reflection = protobuf.GetReflection();
    if (!reflection) return google::api::expr::runtime::CelValue::CreateNull();

    // TODO
    /*
    auto map_builder_status = value_manager.NewMapValueBuilder(cel::MapType());
    if (!map_builder_status.ok()) {
        return value_manager.CreateErrorValue(map_builder_status.status());
    }
    auto map_builder = std::move(map_builder_status.value());

    std::vector<const google::protobuf::FieldDescriptor*> fields;
    reflection->ListFields(protobuf, &fields);

    for (const auto* field : fields) {
        auto cel_key = value_manager.CreateStringValue(field->name());
        
        if (field->is_repeated()) {
            std::vector<google::api::expr::runtime::CelValue> vec;
            int field_size = reflection->FieldSize(protobuf, field);
            for (int i = 0; i < field_size; ++i) {
                vec.push_back(ConvertProtobufFieldToCel(protobuf, field, value_manager, i));
            }
            auto list_value_status = value_manager.CreateListValue(cel::ListType(), vec);
            if(list_value_status.ok()) {
                map_builder->Add(cel_key, list_value_status.value());
            }
        } else {
            map_builder->Add(cel_key, ConvertProtobufFieldToCel(protobuf, field, value_manager));
        }
    }
    */
    return google::api::expr::runtime::CelValue::CreateNull();
}

void CelExecutor::registerExecutor() {
    // Assuming global_registry is available
    // global_registry::registerRuleExecutor(std::make_shared<CelExecutor>());
}

} 