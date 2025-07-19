#include "srclient/rules/cel/CelExecutor.h"
#include "srclient/rules/cel/ExtraFunc.h"
#include "srclient/serdes/json/JsonTypes.h"
#include "srclient/serdes/avro/AvroTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"
#include "srclient/serdes/Serde.h"
#include "srclient/serdes/RuleRegistry.h"
#include "absl/strings/str_split.h"
#include "eval/public/activation.h"
#include "eval/public/builtin_func_registrar.h"
#include "eval/public/cel_expr_builder_factory.h"
#include "eval/public/containers/container_backed_list_impl.h"
#include "eval/public/containers/container_backed_map_impl.h"
#include "eval/public/string_extension_func_registrar.h"
#include "eval/public/structs/cel_proto_wrapper.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "parser/parser.h"
#include <regex>

namespace srclient::rules::cel {

using namespace srclient::serdes;

CelExecutor::CelExecutor() {
    // Try to initialize the CEL runtime
    auto runtime_result = newRuleBuilder(&arena_);
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


std::unique_ptr<SerdeValue> CelExecutor::transform(RuleContext& ctx, const SerdeValue& msg) {
    absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue> args;
    args.emplace("msg", fromSerdeValue(msg, &arena_));

    return execute(ctx, msg, args);
}

std::unique_ptr<SerdeValue> CelExecutor::execute(RuleContext& ctx, 
                               const SerdeValue& msg, 
                               const absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue>& args) {
    google::protobuf::Arena arena;
    
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
            auto guard_result = executeRule(ctx, msg, std::string(guard), args, &arena);
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
    auto result = executeRule(ctx, msg, expr, args, &arena);
    if (result) {
        return toSerdeValue(msg, *result);
    }
    
    return nullptr;
}

std::unique_ptr<google::api::expr::runtime::CelValue> CelExecutor::executeRule(RuleContext& ctx,
                                   const SerdeValue& msg,
                                   const std::string& expr,
                                   const absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue>& args,
                                   google::protobuf::Arena* arena) {
    // Get or compile the expression (with caching)
    auto parsed_expr_status = getOrCompileExpression(expr);
    if (!parsed_expr_status.ok()) {
        throw SerdeError("CEL expression compilation failed: " + std::string(parsed_expr_status.status().message()));
    }
    auto parsed_expr = parsed_expr_status.value();

    // Create activation context and add all arguments
    google::api::expr::runtime::Activation activation;
    for (const auto& pair : args) {
        activation.InsertValue(pair.first, pair.second);
    }
    
    // Evaluate the expression using the passed arena
    auto eval_status = parsed_expr->Evaluate(activation, arena);
    if (!eval_status.ok()) {
        throw SerdeError("CEL evaluation failed: " + std::string(eval_status.status().message()));
    }

    return std::make_unique<google::api::expr::runtime::CelValue>(eval_status.value());
}

absl::StatusOr<std::shared_ptr<google::api::expr::runtime::CelExpression>> CelExecutor::getOrCompileExpression(const std::string& expr) {
    // Thread-safe cache lookup
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        auto it = expression_cache_.find(expr);
        if (it != expression_cache_.end()) {
            // Return shared_ptr from cache
            return it->second;
        }
    }
    
    // Compile the expression using the runtime builder
    if (!runtime_) {
        return absl::FailedPreconditionError("CEL runtime not initialized");
    }
    
    auto pexpr_or = google::api::expr::parser::Parse(expr);
    if (!pexpr_or.ok()) {
        return pexpr_or.status();
    }
    auto pexpr = std::move(pexpr_or).value();
    auto expr_or = runtime_->CreateExpression(&pexpr.expr(), &pexpr.source_info());
    if (!expr_or.ok()) {
        return expr_or.status();
    }

    auto compiled_expr = std::move(expr_or.value());
    
    // Cache the compiled expression using shared_ptr
    std::shared_ptr<google::api::expr::runtime::CelExpression> shared_expr(compiled_expr.release());
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        expression_cache_[expr] = shared_expr;
    }
    
    return shared_expr;
}



google::api::expr::runtime::CelValue CelExecutor::fromSerdeValue(const SerdeValue& value, google::protobuf::Arena* arena) {
    switch (value.getFormat()) {
        case SerdeFormat::Json: {
            auto json_value = std::any_cast<nlohmann::json>(value.getValue());
            return fromJsonValue(json_value, arena);
        }
        case SerdeFormat::Avro: {
            auto avro_value = std::any_cast<::avro::GenericDatum>(value.getValue());
            return fromAvroValue(avro_value, arena);
        }
        case SerdeFormat::Protobuf: {
            auto proto_ref = std::any_cast<std::reference_wrapper<google::protobuf::Message>>(value.getValue());
            return fromProtobufValue(proto_ref.get(), arena);
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



google::api::expr::runtime::CelValue CelExecutor::fromJsonValue(const nlohmann::json& json, google::protobuf::Arena* arena) {
    if (json.is_null()) return google::api::expr::runtime::CelValue::CreateNull();
    if (json.is_boolean()) return google::api::expr::runtime::CelValue::CreateBool(json.get<bool>());
    if (json.is_number_integer()) return google::api::expr::runtime::CelValue::CreateInt64(json.get<int64_t>());
    if (json.is_number_unsigned()) return google::api::expr::runtime::CelValue::CreateUint64(json.get<uint64_t>());
    if (json.is_number_float()) return google::api::expr::runtime::CelValue::CreateDouble(json.get<double>());
    // Use arena allocation for string storage
    if (json.is_string()) {
        auto str_value = json.get<std::string>();
        // Allocate string in arena to ensure proper lifetime
        auto* arena_str = google::protobuf::Arena::Create<std::string>(arena, str_value);
        return google::api::expr::runtime::CelValue::CreateString(arena_str);
    }
    if (json.is_array()) {
        std::vector<google::api::expr::runtime::CelValue> vec;
        for (const auto& item : json) {
            vec.push_back(fromJsonValue(item, arena));
        }
        auto* list_impl = google::protobuf::Arena::Create<google::api::expr::runtime::ContainerBackedListImpl>(arena, vec);
        return google::api::expr::runtime::CelValue::CreateList(list_impl);
    }
    if (json.is_object()) {
        auto* map_impl = google::protobuf::Arena::Create<google::api::expr::runtime::CelMapBuilder>(arena);
        for (const auto& [key, value] : json.items()) {
            auto status = map_impl->Add(fromJsonValue(key, arena), fromJsonValue(value, arena));
            if (!status.ok()) {
                // Log error or handle as needed, but continue processing
            }
        }
        return google::api::expr::runtime::CelValue::CreateMap(map_impl);
    }
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
    } else if (cel_value.IsList()) {
        // Handle array/list conversion
        nlohmann::json json_array = nlohmann::json::array();
        const auto* cel_list = cel_value.ListOrDie();
        for (int i = 0; i < cel_list->size(); ++i) {
            auto item = cel_list->Get(nullptr, i);
            json_array.push_back(toJsonValue(nlohmann::json(), item));
        }
        return json_array;
    } else if (cel_value.IsMap()) {
        // Handle object/map conversion
        nlohmann::json json_object = nlohmann::json::object();
        const auto* cel_map = cel_value.MapOrDie();
        
        // Iterate through map entries
        auto map_keys = cel_map->ListKeys(nullptr);
        if (map_keys.ok()) {
            const auto* keys_list = map_keys.value();
            for (int i = 0; i < keys_list->size(); ++i) {
                auto key_val = keys_list->Get(nullptr, i);
                if (key_val.IsString()) {
                    std::string key = std::string(key_val.StringOrDie().value());
                    auto value_lookup = cel_map->Get(nullptr, key_val);
                    if (value_lookup.has_value()) {
                        json_object[key] = toJsonValue(nlohmann::json(), value_lookup.value());
                    }
                }
            }
        }
        return json_object;
    }
    
    // For unknown types, return the original value
    return original;
}

google::api::expr::runtime::CelValue CelExecutor::fromAvroValue(const ::avro::GenericDatum& avro, google::protobuf::Arena* arena) {
    switch (avro.type()) {
        case ::avro::AVRO_BOOL: return google::api::expr::runtime::CelValue::CreateBool(avro.value<bool>());
        case ::avro::AVRO_INT: return google::api::expr::runtime::CelValue::CreateInt64(avro.value<int32_t>());
        case ::avro::AVRO_LONG: return google::api::expr::runtime::CelValue::CreateInt64(avro.value<int64_t>());
        case ::avro::AVRO_FLOAT: return google::api::expr::runtime::CelValue::CreateDouble(avro.value<float>());
        case ::avro::AVRO_DOUBLE: return google::api::expr::runtime::CelValue::CreateDouble(avro.value<double>());
        case ::avro::AVRO_STRING: {
            // Use arena allocation for string storage
            auto* arena_str = google::protobuf::Arena::Create<std::string>(arena, avro.value<std::string>());
            return google::api::expr::runtime::CelValue::CreateString(arena_str);
        }
        case ::avro::AVRO_BYTES: {
            auto bytes_vec = avro.value<std::vector<uint8_t>>();
            // Use arena allocation for bytes storage
            auto* arena_bytes = google::protobuf::Arena::Create<std::string>(arena);
            arena_bytes->assign(bytes_vec.begin(), bytes_vec.end());
            return google::api::expr::runtime::CelValue::CreateBytes(arena_bytes);
        }
        case ::avro::AVRO_ARRAY: {
            const auto& arr = avro.value<::avro::GenericArray>().value();
            std::vector<google::api::expr::runtime::CelValue> vec;
            for (const auto& item : arr) {
                vec.push_back(fromAvroValue(item, arena));
            }
            auto* list_impl = google::protobuf::Arena::Create<google::api::expr::runtime::ContainerBackedListImpl>(arena, vec);
            return google::api::expr::runtime::CelValue::CreateList(list_impl);
        }
        case ::avro::AVRO_MAP: {
            auto* map_impl = google::protobuf::Arena::Create<google::api::expr::runtime::CelMapBuilder>(arena);
            const auto& map = avro.value<::avro::GenericMap>().value();
            for (const auto& pair : map) {
                // Use arena allocation for string key
                auto* arena_key = google::protobuf::Arena::Create<std::string>(arena, pair.first);
                auto status = map_impl->Add(google::api::expr::runtime::CelValue::CreateString(arena_key), fromAvroValue(pair.second, arena));
                if (!status.ok()) {
                    // Log error or handle as needed, but continue processing
                }
            }
            return google::api::expr::runtime::CelValue::CreateMap(map_impl);
        }
        case ::avro::AVRO_RECORD: {
            auto* map_impl = google::protobuf::Arena::Create<google::api::expr::runtime::CelMapBuilder>(arena);
            const auto& record = avro.value<::avro::GenericRecord>();
            for (size_t i = 0; i < record.schema()->names(); ++i) {
                // Use arena allocation for field name
                auto* arena_name = google::protobuf::Arena::Create<std::string>(arena, record.schema()->nameAt(i));
                auto status = map_impl->Add(google::api::expr::runtime::CelValue::CreateString(arena_name), fromAvroValue(record.fieldAt(i), arena));
                if (!status.ok()) {
                    // Log error or handle as needed, but continue processing
                }
            }
            return google::api::expr::runtime::CelValue::CreateMap(map_impl);
        }
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
    } else if (cel_value.IsList()) {
        // Convert CEL list to Avro array
        const auto* cel_list = cel_value.ListOrDie();
        
        if (original.type() == ::avro::AVRO_ARRAY) {
            // Use existing array schema
            auto orig_array_schema = original.value<::avro::GenericArray>().schema();
            ::avro::GenericDatum result_datum{::avro::ValidSchema(orig_array_schema)};
            auto& result_array = result_datum.value<::avro::GenericArray>();
            
            // Get element template from original if available
            ::avro::GenericDatum element_template;
            auto& orig_array = original.value<::avro::GenericArray>().value();
            if (!orig_array.empty()) {
                element_template = orig_array[0];
            }
            
            // Recursively convert each list element
            for (int i = 0; i < cel_list->size(); ++i) {
                auto item = cel_list->Get(nullptr, i);
                if (!item.IsError()) {
                    result_array.value().push_back(toAvroValue(element_template, item));
                }
            }
            
            return result_datum;
        } else {
            // For non-array originals, return the original (we can't create a new schema)
            return original;
        }
    } else if (cel_value.IsMap()) {
        // Convert CEL map to Avro map or record depending on original type
        const auto* cel_map = cel_value.MapOrDie();
        
        if (original.type() == ::avro::AVRO_RECORD) {
            // Convert to Avro record
            auto orig_record_schema = original.value<::avro::GenericRecord>().schema();
            ::avro::GenericDatum result_datum{::avro::ValidSchema(orig_record_schema)};
            auto& result_record = result_datum.value<::avro::GenericRecord>();
            
            // Copy original record first
            auto& orig_record = original.value<::avro::GenericRecord>();
            for (size_t i = 0; i < orig_record.fieldCount(); ++i) {
                result_record.setFieldAt(i, orig_record.fieldAt(i));
            }
            
            // Iterate through map entries and update matching fields
            auto map_keys = cel_map->ListKeys(nullptr);
            if (map_keys.ok()) {
                const auto* keys_list = map_keys.value();
                for (int i = 0; i < keys_list->size(); ++i) {
                    auto key_val = keys_list->Get(nullptr, i);
                    if (!key_val.IsError() && key_val.IsString()) {
                        std::string key = std::string(key_val.StringOrDie().value());
                        auto value_lookup = cel_map->Get(nullptr, key_val);
                        if (value_lookup.has_value()) {
                            // Find the field index by name
                            for (size_t field_idx = 0; field_idx < orig_record_schema->leaves(); ++field_idx) {
                                if (orig_record_schema->nameAt(field_idx) == key) {
                                    auto field_template = orig_record.fieldAt(field_idx);
                                    result_record.setFieldAt(field_idx, toAvroValue(field_template, value_lookup.value()));
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            
            return result_datum;
        } else if (original.type() == ::avro::AVRO_MAP) {
            // Convert to Avro map
            auto orig_map_schema = original.value<::avro::GenericMap>().schema();
            ::avro::GenericDatum result_datum{::avro::ValidSchema(orig_map_schema)};
            auto& result_map = result_datum.value<::avro::GenericMap>();
            
            // Get value template from original if available
            ::avro::GenericDatum value_template;
            auto& orig_map = original.value<::avro::GenericMap>().value();
            if (!orig_map.empty()) {
                value_template = orig_map.begin()->second;
            }
            
            // Iterate through map entries
            auto map_keys = cel_map->ListKeys(nullptr);
            if (map_keys.ok()) {
                const auto* keys_list = map_keys.value();
                for (int i = 0; i < keys_list->size(); ++i) {
                    auto key_val = keys_list->Get(nullptr, i);
                    if (!key_val.IsError() && key_val.IsString()) {
                        std::string key = std::string(key_val.StringOrDie().value());
                        auto value_lookup = cel_map->Get(nullptr, key_val);
                        if (value_lookup.has_value()) {
                            result_map.value().emplace_back(key, toAvroValue(value_template, value_lookup.value()));
                        }
                    }
                }
            }
            
            return result_datum;
        } else {
            // For non-map/record originals, return the original
            return original;
        }
    }
    
    // For more complex types or unknown types, return the original
    return original;
}

std::unique_ptr<google::protobuf::Message> CelExecutor::toProtobufValue(const google::protobuf::Message& original, const google::api::expr::runtime::CelValue& cel_value) {
    // Create a copy of the original message
    std::unique_ptr<google::protobuf::Message> result(original.New());
    result->CopyFrom(original);
    
    const auto* descriptor = result->GetDescriptor();
    const auto* reflection = result->GetReflection();
    
    if (!descriptor || !reflection) {
        return result;
    }
    
    // Handle different CEL value types
    if (cel_value.IsBool()) {
        // For boolean values, try to find a boolean field to update
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const auto* field = descriptor->field(i);
            if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_BOOL && !field->is_repeated()) {
                reflection->SetBool(result.get(), field, cel_value.BoolOrDie());
                break;
            }
        }
    } else if (cel_value.IsInt64()) {
        // Handle integer values, matching to appropriate integer field types
        int64_t int_value = cel_value.Int64OrDie();
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const auto* field = descriptor->field(i);
            if (field->is_repeated()) continue;
            
            switch (field->cpp_type()) {
                case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                    reflection->SetInt32(result.get(), field, static_cast<int32_t>(int_value));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                    reflection->SetInt64(result.get(), field, int_value);
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                    reflection->SetUInt32(result.get(), field, static_cast<uint32_t>(int_value));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                    reflection->SetUInt64(result.get(), field, static_cast<uint64_t>(int_value));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                    reflection->SetEnumValue(result.get(), field, static_cast<int>(int_value));
                    break;
                default:
                    continue;
            }
            break; // Only update the first matching field
        }
    } else if (cel_value.IsUint64()) {
        // Handle unsigned integer values
        uint64_t uint_value = cel_value.Uint64OrDie();
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const auto* field = descriptor->field(i);
            if (field->is_repeated()) continue;
            
            switch (field->cpp_type()) {
                case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                    reflection->SetInt32(result.get(), field, static_cast<int32_t>(uint_value));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                    reflection->SetInt64(result.get(), field, static_cast<int64_t>(uint_value));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                    reflection->SetUInt32(result.get(), field, static_cast<uint32_t>(uint_value));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                    reflection->SetUInt64(result.get(), field, uint_value);
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                    reflection->SetEnumValue(result.get(), field, static_cast<int>(uint_value));
                    break;
                default:
                    continue;
            }
            break; // Only update the first matching field
        }
    } else if (cel_value.IsDouble()) {
        // Handle floating point values
        double double_value = cel_value.DoubleOrDie();
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const auto* field = descriptor->field(i);
            if (field->is_repeated()) continue;
            
            switch (field->cpp_type()) {
                case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                    reflection->SetFloat(result.get(), field, static_cast<float>(double_value));
                    break;
                case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                    reflection->SetDouble(result.get(), field, double_value);
                    break;
                default:
                    continue;
            }
            break; // Only update the first matching field
        }
    } else if (cel_value.IsString()) {
        // Handle string values
        std::string string_value = std::string(cel_value.StringOrDie().value());
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const auto* field = descriptor->field(i);
            if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING && !field->is_repeated()) {
                reflection->SetString(result.get(), field, string_value);
                break;
            }
        }
    } else if (cel_value.IsBytes()) {
        // Handle bytes values
        std::string bytes_value = std::string(cel_value.BytesOrDie().value());
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const auto* field = descriptor->field(i);
            if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING && 
                field->type() == google::protobuf::FieldDescriptor::TYPE_BYTES && !field->is_repeated()) {
                reflection->SetString(result.get(), field, bytes_value);
                break;
            }
        }
    } else if (cel_value.IsList()) {
        // Handle list/repeated field values
        const auto* cel_list = cel_value.ListOrDie();
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const auto* field = descriptor->field(i);
            if (!field->is_repeated()) continue;
            
            // Clear the repeated field first
            reflection->ClearField(result.get(), field);
            
            // Add elements from the CEL list
            for (int j = 0; j < cel_list->size(); ++j) {
                auto item = cel_list->Get(nullptr, j);
                if (item.IsError()) continue;
                
                switch (field->cpp_type()) {
                    case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                        if (item.IsBool()) {
                            reflection->AddBool(result.get(), field, item.BoolOrDie());
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                        if (item.IsInt64()) {
                            reflection->AddInt32(result.get(), field, static_cast<int32_t>(item.Int64OrDie()));
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                        if (item.IsInt64()) {
                            reflection->AddInt64(result.get(), field, item.Int64OrDie());
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                        if (item.IsUint64()) {
                            reflection->AddUInt32(result.get(), field, static_cast<uint32_t>(item.Uint64OrDie()));
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                        if (item.IsUint64()) {
                            reflection->AddUInt64(result.get(), field, item.Uint64OrDie());
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                        if (item.IsDouble()) {
                            reflection->AddFloat(result.get(), field, static_cast<float>(item.DoubleOrDie()));
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                        if (item.IsDouble()) {
                            reflection->AddDouble(result.get(), field, item.DoubleOrDie());
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                        if (item.IsString()) {
                            reflection->AddString(result.get(), field, std::string(item.StringOrDie().value()));
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                        if (item.IsInt64()) {
                            reflection->AddEnumValue(result.get(), field, static_cast<int>(item.Int64OrDie()));
                        }
                        break;
                    default:
                        break;
                }
            }
            break; // Only update the first matching repeated field
        }
    } else if (cel_value.IsMap()) {
        // Handle map values by updating fields based on map keys
        const auto* cel_map = cel_value.MapOrDie();
        
        auto map_keys = cel_map->ListKeys(nullptr);
        if (map_keys.ok()) {
            const auto* keys_list = map_keys.value();
            for (int i = 0; i < keys_list->size(); ++i) {
                auto key_val = keys_list->Get(nullptr, i);
                if (key_val.IsError() || !key_val.IsString()) continue;
                
                std::string field_name = std::string(key_val.StringOrDie().value());
                auto value_lookup = cel_map->Get(nullptr, key_val);
                if (!value_lookup.has_value()) continue;
                
                auto field_value = value_lookup.value();
                
                // Find the field by name
                const auto* field = descriptor->FindFieldByName(field_name);
                if (!field || field->is_repeated()) continue;
                
                // Set the field value based on its type
                switch (field->cpp_type()) {
                    case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                        if (field_value.IsBool()) {
                            reflection->SetBool(result.get(), field, field_value.BoolOrDie());
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                        if (field_value.IsInt64()) {
                            reflection->SetInt32(result.get(), field, static_cast<int32_t>(field_value.Int64OrDie()));
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                        if (field_value.IsInt64()) {
                            reflection->SetInt64(result.get(), field, field_value.Int64OrDie());
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                        if (field_value.IsUint64()) {
                            reflection->SetUInt32(result.get(), field, static_cast<uint32_t>(field_value.Uint64OrDie()));
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                        if (field_value.IsUint64()) {
                            reflection->SetUInt64(result.get(), field, field_value.Uint64OrDie());
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                        if (field_value.IsDouble()) {
                            reflection->SetFloat(result.get(), field, static_cast<float>(field_value.DoubleOrDie()));
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                        if (field_value.IsDouble()) {
                            reflection->SetDouble(result.get(), field, field_value.DoubleOrDie());
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                        if (field_value.IsString()) {
                            reflection->SetString(result.get(), field, std::string(field_value.StringOrDie().value()));
                        } else if (field_value.IsBytes() && field->type() == google::protobuf::FieldDescriptor::TYPE_BYTES) {
                            reflection->SetString(result.get(), field, std::string(field_value.BytesOrDie().value()));
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                        if (field_value.IsInt64()) {
                            reflection->SetEnumValue(result.get(), field, static_cast<int>(field_value.Int64OrDie()));
                        }
                        break;
                    case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE:
                        // For nested messages, recursively convert
                        if (field_value.IsMap()) {
                            const auto& nested_original = reflection->GetMessage(original, field);
                            auto nested_result = toProtobufValue(nested_original, field_value);
                            if (nested_result) {
                                reflection->SetAllocatedMessage(result.get(), nested_result.release(), field);
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }
    
    return result;
}

google::api::expr::runtime::CelValue CelExecutor::fromProtobufValue(const google::protobuf::Message& protobuf, google::protobuf::Arena* arena) {
    const auto* descriptor = protobuf.GetDescriptor();
    if (!descriptor) return google::api::expr::runtime::CelValue::CreateNull();
    const auto* reflection = protobuf.GetReflection();
    if (!reflection) return google::api::expr::runtime::CelValue::CreateNull();

    // Create a map to represent the protobuf message
    auto* map_impl = google::protobuf::Arena::Create<google::api::expr::runtime::CelMapBuilder>(arena);

    // Get all fields that have values set
    std::vector<const google::protobuf::FieldDescriptor*> fields;
    reflection->ListFields(protobuf, &fields);

    for (const auto* field : fields) {
        // Create key for the field name
        auto* arena_field_name = google::protobuf::Arena::Create<std::string>(arena, field->name());
        auto cel_key = google::api::expr::runtime::CelValue::CreateString(arena_field_name);
        
        google::api::expr::runtime::CelValue cel_value = google::api::expr::runtime::CelValue::CreateNull();
        
        if (field->is_repeated()) {
            // Handle repeated fields as lists
            std::vector<google::api::expr::runtime::CelValue> vec;
            int field_size = reflection->FieldSize(protobuf, field);
            
            for (int i = 0; i < field_size; ++i) {
                cel_value = convertProtobufFieldToCel(protobuf, field, reflection, arena, i);
                if (!cel_value.IsError()) {
                    vec.push_back(cel_value);
                }
            }
            
            auto* list_impl = google::protobuf::Arena::Create<google::api::expr::runtime::ContainerBackedListImpl>(arena, vec);
            cel_value = google::api::expr::runtime::CelValue::CreateList(list_impl);
        } else {
            // Handle single value fields
            cel_value = convertProtobufFieldToCel(protobuf, field, reflection, arena, -1);
        }
        
        // Add to map if conversion was successful
        if (!cel_value.IsError()) {
            auto status = map_impl->Add(cel_key, cel_value);
            if (!status.ok()) {
                // Log error but continue processing other fields
            }
        }
    }

    return google::api::expr::runtime::CelValue::CreateMap(map_impl);
}

google::api::expr::runtime::CelValue CelExecutor::convertProtobufFieldToCel(
    const google::protobuf::Message& message,
    const google::protobuf::FieldDescriptor* field,
    const google::protobuf::Reflection* reflection,
    google::protobuf::Arena* arena,
    int index) {
    
    // For repeated fields, index >= 0; for singular fields, index = -1
    
    switch (field->cpp_type()) {
        case google::protobuf::FieldDescriptor::CPPTYPE_BOOL: {
            bool value = (index >= 0) ? reflection->GetRepeatedBool(message, field, index)
                                       : reflection->GetBool(message, field);
            return google::api::expr::runtime::CelValue::CreateBool(value);
        }
        
        case google::protobuf::FieldDescriptor::CPPTYPE_INT32: {
            int32_t value = (index >= 0) ? reflection->GetRepeatedInt32(message, field, index)
                                         : reflection->GetInt32(message, field);
            return google::api::expr::runtime::CelValue::CreateInt64(static_cast<int64_t>(value));
        }
        
        case google::protobuf::FieldDescriptor::CPPTYPE_INT64: {
            int64_t value = (index >= 0) ? reflection->GetRepeatedInt64(message, field, index)
                                         : reflection->GetInt64(message, field);
            return google::api::expr::runtime::CelValue::CreateInt64(value);
        }
        
        case google::protobuf::FieldDescriptor::CPPTYPE_UINT32: {
            uint32_t value = (index >= 0) ? reflection->GetRepeatedUInt32(message, field, index)
                                          : reflection->GetUInt32(message, field);
            return google::api::expr::runtime::CelValue::CreateInt64(static_cast<int64_t>(value));
        }
        
        case google::protobuf::FieldDescriptor::CPPTYPE_UINT64: {
            uint64_t value = (index >= 0) ? reflection->GetRepeatedUInt64(message, field, index)
                                          : reflection->GetUInt64(message, field);
            return google::api::expr::runtime::CelValue::CreateInt64(static_cast<int64_t>(value));
        }
        
        case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT: {
            float value = (index >= 0) ? reflection->GetRepeatedFloat(message, field, index)
                                       : reflection->GetFloat(message, field);
            return google::api::expr::runtime::CelValue::CreateDouble(static_cast<double>(value));
        }
        
        case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE: {
            double value = (index >= 0) ? reflection->GetRepeatedDouble(message, field, index)
                                        : reflection->GetDouble(message, field);
            return google::api::expr::runtime::CelValue::CreateDouble(value);
        }
        
        case google::protobuf::FieldDescriptor::CPPTYPE_STRING: {
            std::string value = (index >= 0) ? reflection->GetRepeatedString(message, field, index)
                                             : reflection->GetString(message, field);
            // Use arena allocation for string storage
            auto* arena_str = google::protobuf::Arena::Create<std::string>(arena, value);
            
            if (field->type() == google::protobuf::FieldDescriptor::TYPE_BYTES) {
                return google::api::expr::runtime::CelValue::CreateBytes(arena_str);
            } else {
                return google::api::expr::runtime::CelValue::CreateString(arena_str);
            }
        }
        
        case google::protobuf::FieldDescriptor::CPPTYPE_ENUM: {
            int value = (index >= 0) ? reflection->GetRepeatedEnumValue(message, field, index)
                                     : reflection->GetEnumValue(message, field);
            return google::api::expr::runtime::CelValue::CreateInt64(static_cast<int64_t>(value));
        }
        
        case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
            const google::protobuf::Message& nested_message = 
                (index >= 0) ? reflection->GetRepeatedMessage(message, field, index)
                             : reflection->GetMessage(message, field);
            
            // Handle map fields specially
            if (field->is_map()) {
                return convertProtobufMapToCel(nested_message, field, arena);
            } else {
                // Recursively convert nested message
                return fromProtobufValue(nested_message, arena);
            }
        }
        
        default:
            return google::api::expr::runtime::CelValue::CreateNull();
    }
}

google::api::expr::runtime::CelValue CelExecutor::convertProtobufMapToCel(
    const google::protobuf::Message& map_entry,
    const google::protobuf::FieldDescriptor* map_field,
    google::protobuf::Arena* arena) {
    
    auto* map_impl = google::protobuf::Arena::Create<google::api::expr::runtime::CelMapBuilder>(arena);
    
    const auto* descriptor = map_entry.GetDescriptor();
    const auto* reflection = map_entry.GetReflection();
    
    if (!descriptor || !reflection) {
        return google::api::expr::runtime::CelValue::CreateMap(map_impl);
    }
    
    // Map entries have exactly 2 fields: key and value
    const auto* key_field = descriptor->field(0);   // "key"
    const auto* value_field = descriptor->field(1); // "value"
    
    if (!key_field || !value_field) {
        return google::api::expr::runtime::CelValue::CreateMap(map_impl);
    }
    
    // Convert the key
    auto cel_key = convertProtobufFieldToCel(map_entry, key_field, reflection, arena, -1);
    
    // Convert the value  
    auto cel_value = convertProtobufFieldToCel(map_entry, value_field, reflection, arena, -1);
    
    // Add to map
    if (!cel_key.IsError() && !cel_value.IsError()) {
        auto status = map_impl->Add(cel_key, cel_value);
        if (!status.ok()) {
            // Log error but continue
        }
    }
    
    return google::api::expr::runtime::CelValue::CreateMap(map_impl);
}

void CelExecutor::registerExecutor() {
    // Register this executor with the global rule registry
    // This matches the Rust version: crate::serdes::rule_registry::register_rule_executor(CelExecutor::new());
    global_registry::registerRuleExecutor(
        std::make_shared<CelExecutor>()
    );
}

} 