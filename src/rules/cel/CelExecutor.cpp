#include "srclient/rules/cel/CelExecutor.h"
#include "srclient/rules/cel/ExtraFunc.h"
#include "srclient/serdes/json/JsonTypes.h"
#include "srclient/serdes/avro/AvroTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include "srclient/serdes/Serde.h"
// Comment out problematic includes that might not be available
// #include "parser/parser.h"
// #include "runtime/activation.h"
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
    // Try to initialize the CEL runtime
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

// Implement the required getType method
std::string CelExecutor::getType() const {
    return "CEL";
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
  // Register our custom extra functions
  register_status = RegisterExtraFuncs(*builder->GetRegistry(), arena);
  if (!register_status.ok()) {
    return register_status;
  }
  return builder;
}


std::unique_ptr<SerdeValue> CelExecutor::transform(RuleContext& ctx, SerdeValue& msg) {
    absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue> args;
    args.emplace("msg", fromSerdeValue(msg));

    auto result = execute(ctx, msg, args);
    if (result) {
        // TODO: Replace the original message with the result once we have proper message replacement
        // For now, just return a clone of the original - the execute method currently returns nullptr anyway
        // auto converted_msg = std::move(*result);  // This line causes abstract class error - commented out
        return msg.clone();
    }
    return msg.clone();
}

std::unique_ptr<SerdeValue> CelExecutor::execute(RuleContext& ctx, 
                               const SerdeValue& msg, 
                               const absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue>& args) {
    // Get the expression from the rule context
    const Rule& rule = ctx.getRule();
    
    auto expr_opt = rule.getExpr();
    if (!expr_opt.has_value()) {
        throw SerdeError("rule does not contain an expression");
    }
    
    std::string expr = expr_opt.value();
    if (expr.empty()) {
        throw SerdeError("rule does not contain an expression");
    }

    // Split expression on semicolon to handle guard expressions like Rust version
    std::vector<absl::string_view> parts = absl::StrSplit(expr, ";");
    
    if (parts.size() > 1) {
        // Handle guard expression
        absl::string_view guard = parts[0];
        if (!guard.empty()) {
            auto guard_result = executeRule(ctx, msg, std::string(guard), args);
            if (guard_result) {
                // Check if guard evaluates to false - if so, return copy of original message
                if (guard_result->IsBool() && !guard_result->BoolOrDie()) {
                    // Return copy using the msg's clone method
                    return msg.clone();
                }
            }
        }
        // Use the second part as the main expression
        expr = std::string(parts[1]);
    }

    // Execute the main expression
    auto result = executeRule(ctx, msg, expr, args);
    if (result) {
        return toSerdeValue(msg, *result);
    }
    
    return nullptr;
}

std::unique_ptr<google::api::expr::runtime::CelValue> CelExecutor::executeRule(RuleContext& ctx,
                                   const SerdeValue& msg,
                                   const std::string& expr,
                                   const absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue>& args) {
    // Get or compile the expression (with caching)
    auto parsed_expr_status = getOrCompileExpression(expr);
    if (!parsed_expr_status.ok()) {
        throw SerdeError("CEL expression compilation failed: " + std::string(parsed_expr_status.status().message()));
    }
    auto parsed_expr = std::move(parsed_expr_status.value());

    // Create activation context and add all arguments
    google::api::expr::runtime::Activation activation;
    for (const auto& pair : args) {
        activation.InsertValue(pair.first, pair.second);
    }
    
    // Create arena for evaluation
    google::protobuf::Arena arena;
    
    // Evaluate the expression
    auto eval_status = parsed_expr->Evaluate(activation, &arena);
    if (!eval_status.ok()) {
        throw SerdeError("CEL evaluation failed: " + std::string(eval_status.status().message()));
    }

    return std::make_unique<google::api::expr::runtime::CelValue>(eval_status.value());
}

absl::StatusOr<std::unique_ptr<google::api::expr::runtime::CelExpression>> CelExecutor::getOrCompileExpression(const std::string& expr) {
    // Thread-safe cache lookup
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        auto it = expression_cache_.find(expr);
        if (it != expression_cache_.end()) {
            // Return a copy since we're returning unique_ptr
            // Note: We can't actually copy CelExpression, so we'll need to recompile
            // This is a limitation compared to Rust where Program is Clone
            // For now, fall through to recompilation
        }
    }
    
    // Compile the expression using the runtime builder
    if (!runtime_) {
        return absl::FailedPreconditionError("CEL runtime not initialized");
    }
    
    // Parse the CEL expression
    google::api::expr::v1alpha1::Expr cel_expr;
    google::api::expr::v1alpha1::SourceInfo source_info;
    
    // For now, we'll use a simplified compilation approach
    // In a full implementation, we'd use cel::parser::Parse() 
    auto expr_builder = runtime_->GetRegistry();
    if (!expr_builder) {
        return absl::FailedPreconditionError("Failed to get expression builder");
    }
    
    // Create a simple expression for testing
    // TODO: Replace with proper CEL parsing once available
    auto expression_result = runtime_->CreateExpression(&cel_expr, &source_info);
    if (!expression_result.ok()) {
        return expression_result.status();
    }
    
    auto compiled_expr = std::move(expression_result.value());
    
    // Cache the compiled expression (though we can't reuse it due to unique_ptr limitation)
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        // For now, just return without caching since CelExpression is not copyable
        // expression_cache_[expr] = std::move(compiled_expr);  // Can't do this with unique_ptr
    }
    
    return std::move(compiled_expr);
}

google::api::expr::runtime::CelValue CelExecutor::fromSerdeValue(const SerdeValue& value) {
    switch (value.getFormat()) {
        case SerdeFormat::Json: {
            auto json_value = std::any_cast<nlohmann::json>(value.getValue());
            return fromJsonValue(json_value);
        }
        case SerdeFormat::Avro: {
            auto avro_value = std::any_cast<::avro::GenericDatum>(value.getValue());
            return fromAvroValue(avro_value);
        }
        case SerdeFormat::Protobuf: {
            auto proto_ref = std::any_cast<std::reference_wrapper<google::protobuf::Message>>(value.getValue());
            return fromProtobufValue(proto_ref.get());
        }
        default:
            return google::api::expr::runtime::CelValue::CreateNull();
    }
}

std::unique_ptr<SerdeValue> CelExecutor::toSerdeValue(const SerdeValue& original, const google::api::expr::runtime::CelValue& cel_value) {
    switch (original.getFormat()) {
        case SerdeFormat::Json: {
            auto original_json = std::any_cast<nlohmann::json>(original.getValue());
            auto converted_json = toJsonValue(original_json, cel_value);
            return srclient::serdes::json::makeJsonValue(converted_json);
        }
        case SerdeFormat::Avro: {
            auto original_avro = std::any_cast<::avro::GenericDatum>(original.getValue());
            auto converted_avro = toAvroValue(original_avro, cel_value);
            return srclient::serdes::avro::makeAvroValue(converted_avro);
        }
        case SerdeFormat::Protobuf: {
            auto proto_ref = std::any_cast<std::reference_wrapper<google::protobuf::Message>>(original.getValue());
            auto converted_proto = toProtobufValue(proto_ref.get(), cel_value);
            return srclient::serdes::protobuf::makeProtobufValue(*converted_proto);
        }
        default:
            // For unknown formats, return a copy of the original
            return original.clone();
    }
}

google::api::expr::runtime::CelValue CelExecutor::fromJsonValue(const nlohmann::json& json) {
    if (json.is_null()) return google::api::expr::runtime::CelValue::CreateNull();
    if (json.is_boolean()) return google::api::expr::runtime::CelValue::CreateBool(json.get<bool>());
    if (json.is_number_integer()) return google::api::expr::runtime::CelValue::CreateInt64(json.get<int64_t>());
    if (json.is_number_unsigned()) return google::api::expr::runtime::CelValue::CreateUint64(json.get<uint64_t>());
    if (json.is_number_float()) return google::api::expr::runtime::CelValue::CreateDouble(json.get<double>());
    // Fix string creation to use pointer
    if (json.is_string()) {
        auto str_value = json.get<std::string>();
        // Store string in static storage or member variable to ensure lifetime
        static thread_local std::string temp_str;
        temp_str = str_value;
        return google::api::expr::runtime::CelValue::CreateString(&temp_str);
    }
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
    // Convert CEL values back to JSON
    if (cel_value.IsBool()) {
        return nlohmann::json(cel_value.BoolOrDie());
    } else if (cel_value.IsInt64()) {
        return nlohmann::json(cel_value.Int64OrDie());
    } else if (cel_value.IsUint64()) {
        return nlohmann::json(cel_value.Uint64OrDie());
    } else if (cel_value.IsDouble()) {
        return nlohmann::json(cel_value.DoubleOrDie());
    } else if (cel_value.IsString()) {
        return nlohmann::json(std::string(cel_value.StringOrDie().value()));
    } else if (cel_value.IsNull()) {
        return nlohmann::json(nullptr);
    }
    // For more complex types (List, Map) or unknown types, return the original
    // TODO: Implement recursive conversion for lists and maps
    return original;
}

google::api::expr::runtime::CelValue CelExecutor::fromAvroValue(const ::avro::GenericDatum& avro) {
    switch (avro.type()) {
        case ::avro::AVRO_BOOL: return google::api::expr::runtime::CelValue::CreateBool(avro.value<bool>());
        case ::avro::AVRO_INT: return google::api::expr::runtime::CelValue::CreateInt64(avro.value<int32_t>());
        case ::avro::AVRO_LONG: return google::api::expr::runtime::CelValue::CreateInt64(avro.value<int64_t>());
        case ::avro::AVRO_FLOAT: return google::api::expr::runtime::CelValue::CreateDouble(avro.value<float>());
        case ::avro::AVRO_DOUBLE: return google::api::expr::runtime::CelValue::CreateDouble(avro.value<double>());
        case ::avro::AVRO_STRING: {
            // Fix string creation to use pointer
            static thread_local std::string temp_str;
            temp_str = avro.value<std::string>();
            return google::api::expr::runtime::CelValue::CreateString(&temp_str);
        }
        case ::avro::AVRO_BYTES: {
            auto bytes_vec = avro.value<std::vector<uint8_t>>();
            // Convert vector to string for CreateBytes with pointer
            static thread_local std::string temp_bytes;
            temp_bytes.assign(bytes_vec.begin(), bytes_vec.end());
            return google::api::expr::runtime::CelValue::CreateBytes(&temp_bytes);
        }
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
    // Convert CEL values back to Avro
    if (cel_value.IsBool()) {
        return ::avro::GenericDatum(cel_value.BoolOrDie());
    } else if (cel_value.IsInt64()) {
        // Avro int is 32-bit, but we can handle both int32 and int64
        return ::avro::GenericDatum(static_cast<int64_t>(cel_value.Int64OrDie()));
    } else if (cel_value.IsUint64()) {
        return ::avro::GenericDatum(static_cast<int64_t>(cel_value.Uint64OrDie()));
    } else if (cel_value.IsDouble()) {
        return ::avro::GenericDatum(cel_value.DoubleOrDie());
    } else if (cel_value.IsString()) {
        return ::avro::GenericDatum(std::string(cel_value.StringOrDie().value()));
    } else if (cel_value.IsNull()) {
        return ::avro::GenericDatum();  // Creates null datum
    }
    // For more complex types or unknown types, return the original
    return original;
}

std::unique_ptr<google::protobuf::Message> CelExecutor::toProtobufValue(const google::protobuf::Message& original, const google::api::expr::runtime::CelValue& cel_value) {
    auto new_msg = std::unique_ptr<google::protobuf::Message>(original.New());
    new_msg->CopyFrom(original);

    // Comment out complex conversion logic for now
    /*
    if (!cel_value.Is<cel::MapValue>()) {
        return new_msg;
    }
    // TODO: Full conversion from cel::MapValue to a protobuf message is complex and not yet implemented.
    // For now, we return a copy of the original message.
    */
    return new_msg;
}

static google::api::expr::runtime::CelValue ConvertProtobufFieldToCel(const google::protobuf::Message& message,
                                              const google::protobuf::FieldDescriptor* field,
                                              int index = -1) {
    const auto* reflection = message.GetReflection();
    switch (field->cpp_type()) {
        case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
            return google::api::expr::runtime::CelValue::CreateInt64(index != -1 ? reflection->GetRepeatedInt32(message, field, index) : reflection->GetInt32(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
            return google::api::expr::runtime::CelValue::CreateInt64(index != -1 ? reflection->GetRepeatedInt64(message, field, index) : reflection->GetInt64(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
            return google::api::expr::runtime::CelValue::CreateUint64(index != -1 ? reflection->GetRepeatedUInt32(message, field, index) : reflection->GetUInt32(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
            return google::api::expr::runtime::CelValue::CreateUint64(index != -1 ? reflection->GetRepeatedUInt64(message, field, index) : reflection->GetUInt64(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
            return google::api::expr::runtime::CelValue::CreateDouble(index != -1 ? reflection->GetRepeatedFloat(message, field, index) : reflection->GetFloat(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
            return google::api::expr::runtime::CelValue::CreateDouble(index != -1 ? reflection->GetRepeatedDouble(message, field, index) : reflection->GetDouble(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
            return google::api::expr::runtime::CelValue::CreateBool(index != -1 ? reflection->GetRepeatedBool(message, field, index) : reflection->GetBool(message, field));
        case google::protobuf::FieldDescriptor::CPPTYPE_STRING: {
            // Fix string creation to use pointer
            static thread_local std::string temp_str;
            temp_str = index != -1 ? reflection->GetRepeatedString(message, field, index) : reflection->GetString(message, field);
            return google::api::expr::runtime::CelValue::CreateString(&temp_str);
        }
        case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
            return google::api::expr::runtime::CelValue::CreateInt64(index != -1 ? reflection->GetRepeatedEnum(message, field, index)->number() : reflection->GetEnum(message, field)->number());
        // TODO
        /*
        case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE:
            {
                const auto& sub_message = index != -1 ? reflection->GetRepeatedMessage(message, field, index) : reflection->GetMessage(message, field);
                return CelExecutor::fromProtobufValue(sub_message, value_manager);
            }
        */
        default:
            return google::api::expr::runtime::CelValue::CreateNull();
    }
}

google::api::expr::runtime::CelValue CelExecutor::fromProtobufValue(const google::protobuf::Message& protobuf) {
    const auto* descriptor = protobuf.GetDescriptor();
    if (!descriptor) return google::api::expr::runtime::CelValue::CreateNull();
    const auto* reflection = protobuf.GetReflection();
    if (!reflection) return google::api::expr::runtime::CelValue::CreateNull();

    // Comment out complex conversion logic for now
    /*
    // TODO
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
    // Register this executor with the global rule registry
    // This matches the Rust version: crate::serdes::rule_registry::register_rule_executor(CelExecutor::new());
    // TODO: Implement when rule registry is available
    // RuleRegistry::instance().registerRuleExecutor(std::make_shared<CelExecutor>());
}

} 