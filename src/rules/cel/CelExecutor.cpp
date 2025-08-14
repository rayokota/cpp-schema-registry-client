#include "srclient/rules/cel/CelExecutor.h"

#include <regex>

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
#include "srclient/rules/cel/ExtraFunc.h"
#include "srclient/serdes/RuleRegistry.h"
#include "srclient/serdes/Serde.h"
#include "srclient/serdes/avro/AvroTypes.h"
#include "srclient/serdes/json/JsonTypes.h"
#include "srclient/serdes/protobuf/ProtobufTypes.h"

namespace srclient::rules::cel {

using namespace srclient::serdes;

CelExecutor::CelExecutor() {
    // Try to initialize the CEL runtime
    auto runtime_result = newRuleBuilder(&arena_);
    if (!runtime_result.ok()) {
        throw SerdeError("Failed to create CEL runtime: " +
                         std::string(runtime_result.status().message()));
    }
    runtime_ = std::move(runtime_result.value());
}

CelExecutor::CelExecutor(
    std::unique_ptr<const google::api::expr::runtime::CelExpressionBuilder>
        runtime)
    : runtime_(std::move(runtime)) {
    if (!runtime_) {
        throw SerdeError("CEL runtime cannot be null");
    }
}

// Implement the required getType method
std::string CelExecutor::getType() const { return "CEL"; }

absl::StatusOr<
    std::unique_ptr<google::api::expr::runtime::CelExpressionBuilder>>
CelExecutor::newRuleBuilder(google::protobuf::Arena *arena) {
    google::api::expr::runtime::InterpreterOptions options;
    options.enable_qualified_type_identifiers = true;
    options.enable_timestamp_duration_overflow_errors = true;
    options.enable_empty_wrapper_null_unboxing = true;
    options.enable_regex_precompilation = true;
    options.constant_folding = true;
    options.constant_arena = arena;

    std::unique_ptr<google::api::expr::runtime::CelExpressionBuilder> builder =
        google::api::expr::runtime::CreateCelExpressionBuilder(options);
    auto register_status = google::api::expr::runtime::RegisterBuiltinFunctions(
        builder->GetRegistry(), options);
    if (!register_status.ok()) {
        return register_status;
    }
    register_status =
        google::api::expr::runtime::RegisterStringExtensionFunctions(
            builder->GetRegistry());
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

std::unique_ptr<SerdeValue> CelExecutor::transform(
    srclient::serdes::RuleContext &ctx, const SerdeValue &msg) {
    google::protobuf::Arena arena;

    absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue> args;
    args.emplace("msg", fromSerdeValue(msg, &arena));

    return execute(ctx, msg, args, &arena);
}

std::unique_ptr<SerdeValue> CelExecutor::execute(
    srclient::serdes::RuleContext &ctx, const SerdeValue &msg,
    const absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue>
        &args,
    google::protobuf::Arena *arena) {
    // Get the expression from the rule context
    const Rule &rule = ctx.getRule();

    auto expr_opt = rule.getExpr();
    if (!expr_opt.has_value()) {
        throw SerdeError("rule does not contain an expression");
    }

    std::string expr = expr_opt.value();
    if (expr.empty()) {
        throw SerdeError("rule does not contain an expression");
    }

    // Split expression on semicolon to handle guard expressions like Rust
    // version
    std::vector<absl::string_view> parts = absl::StrSplit(expr, ";");

    if (parts.size() > 1) {
        // Handle guard expression
        absl::string_view guard = parts[0];
        if (!guard.empty()) {
            auto guard_result =
                executeRule(ctx, msg, std::string(guard), args, arena);
            if (guard_result) {
                // Check if guard evaluates to false - if so, return copy of
                // original message
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
    auto result = executeRule(ctx, msg, expr, args, arena);
    if (result) {
        return toSerdeValue(msg, *result);
    }

    return nullptr;
}

std::unique_ptr<google::api::expr::runtime::CelValue> CelExecutor::executeRule(
    RuleContext &ctx, const SerdeValue &msg, const std::string &expr,
    const absl::flat_hash_map<std::string, google::api::expr::runtime::CelValue>
        &args,
    google::protobuf::Arena *arena) {
    // Get or compile the expression (with caching)
    auto parsed_expr_status = getOrCompileExpression(expr);
    if (!parsed_expr_status.ok()) {
        throw SerdeError("CEL expression compilation failed: " +
                         std::string(parsed_expr_status.status().message()));
    }
    auto parsed_expr = parsed_expr_status.value();

    // Create activation context and add all arguments
    google::api::expr::runtime::Activation activation;
    for (const auto &pair : args) {
        activation.InsertValue(pair.first, pair.second);
    }

    // Evaluate the expression using the passed arena
    auto eval_status = parsed_expr->Evaluate(activation, arena);
    if (!eval_status.ok()) {
        throw SerdeError("CEL evaluation failed: " +
                         std::string(eval_status.status().message()));
    }

    return std::make_unique<google::api::expr::runtime::CelValue>(
        eval_status.value());
}

absl::StatusOr<std::shared_ptr<google::api::expr::runtime::CelExpression>>
CelExecutor::getOrCompileExpression(const std::string &expr) {
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
    auto expr_or =
        runtime_->CreateExpression(&pexpr.expr(), &pexpr.source_info());
    if (!expr_or.ok()) {
        return expr_or.status();
    }

    auto compiled_expr = std::move(expr_or.value());

    // Cache the compiled expression using shared_ptr
    std::shared_ptr<google::api::expr::runtime::CelExpression> shared_expr(
        compiled_expr.release());
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        expression_cache_[expr] = shared_expr;
    }

    return shared_expr;
}

google::api::expr::runtime::CelValue CelExecutor::fromSerdeValue(
    const SerdeValue &value, google::protobuf::Arena *arena) {
    switch (value.getFormat()) {
        case SerdeFormat::Json: {
            auto json_value = srclient::serdes::json::asJson(value);
            return fromJsonValue(srclient::serdes::json::toNlohmann(json_value), arena);
        }
        case SerdeFormat::Avro: {
            auto avro_value = srclient::serdes::avro::asAvro(value);
            return fromAvroValue(avro_value, arena);
        }
        case SerdeFormat::Protobuf: {
            auto &proto_variant = srclient::serdes::protobuf::asProtobuf(value);
            return fromProtobufValue(proto_variant, arena);
        }
        default:
            return google::api::expr::runtime::CelValue::CreateNull();
    }
}

std::unique_ptr<SerdeValue> CelExecutor::toSerdeValue(
    const SerdeValue &original,
    const google::api::expr::runtime::CelValue &cel_value) {
    switch (original.getFormat()) {
        case SerdeFormat::Json: {
            auto original_json = srclient::serdes::json::asJson(original);
            auto converted_json = toJsonValue(srclient::serdes::json::toNlohmann(original_json), cel_value);
            return srclient::serdes::json::makeJsonValue(srclient::serdes::json::fromNlohmann(converted_json));
        }
        case SerdeFormat::Avro: {
            auto original_avro = srclient::serdes::avro::asAvro(original);
            auto converted_avro = toAvroValue(original_avro, cel_value);
            return srclient::serdes::avro::makeAvroValue(converted_avro);
        }
        case SerdeFormat::Protobuf: {
            auto &proto_variant =
                srclient::serdes::protobuf::asProtobuf(original);
            auto converted_proto_variant =
                toProtobufValue(proto_variant, cel_value);
            return srclient::serdes::protobuf::makeProtobufValue(
                std::move(converted_proto_variant));
        }
        default:
            // For unknown formats, return a copy of the original
            return original.clone();
    }
}

google::api::expr::runtime::CelValue CelExecutor::fromJsonValue(
    const nlohmann::json &json, google::protobuf::Arena *arena) {
    if (json.is_null())
        return google::api::expr::runtime::CelValue::CreateNull();
    if (json.is_boolean())
        return google::api::expr::runtime::CelValue::CreateBool(
            json.get<bool>());
    if (json.is_number_integer())
        return google::api::expr::runtime::CelValue::CreateInt64(
            json.get<int64_t>());
    if (json.is_number_unsigned())
        return google::api::expr::runtime::CelValue::CreateUint64(
            json.get<uint64_t>());
    if (json.is_number_float())
        return google::api::expr::runtime::CelValue::CreateDouble(
            json.get<double>());
    // Use arena allocation for string storage
    if (json.is_string()) {
        auto str_value = json.get<std::string>();
        // Allocate string in arena to ensure proper lifetime
        auto *arena_str =
            google::protobuf::Arena::Create<std::string>(arena, str_value);
        return google::api::expr::runtime::CelValue::CreateString(arena_str);
    }
    if (json.is_array()) {
        std::vector<google::api::expr::runtime::CelValue> vec;
        for (const auto &item : json) {
            vec.push_back(fromJsonValue(item, arena));
        }
        auto *list_impl = google::protobuf::Arena::Create<
            google::api::expr::runtime::ContainerBackedListImpl>(arena, vec);
        return google::api::expr::runtime::CelValue::CreateList(list_impl);
    }
    if (json.is_object()) {
        auto *map_impl = google::protobuf::Arena::Create<
            google::api::expr::runtime::CelMapBuilder>(arena);
        for (const auto &[key, value] : json.items()) {
            auto status = map_impl->Add(fromJsonValue(key, arena),
                                        fromJsonValue(value, arena));
            if (!status.ok()) {
                // Log error or handle as needed, but continue processing
            }
        }
        return google::api::expr::runtime::CelValue::CreateMap(map_impl);
    }
    return google::api::expr::runtime::CelValue::CreateNull();
}

nlohmann::json CelExecutor::toJsonValue(
    const nlohmann::json &original,
    const google::api::expr::runtime::CelValue &cel_value) {
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
        const auto *cel_list = cel_value.ListOrDie();
        for (int i = 0; i < cel_list->size(); ++i) {
            auto item = cel_list->Get(nullptr, i);
            json_array.push_back(toJsonValue(nlohmann::json(), item));
        }
        return json_array;
    } else if (cel_value.IsMap()) {
        // Handle object/map conversion
        nlohmann::json json_object = nlohmann::json::object();
        const auto *cel_map = cel_value.MapOrDie();

        // Iterate through map entries
        auto map_keys = cel_map->ListKeys(nullptr);
        if (map_keys.ok()) {
            const auto *keys_list = map_keys.value();
            for (int i = 0; i < keys_list->size(); ++i) {
                auto key_val = keys_list->Get(nullptr, i);
                if (key_val.IsString()) {
                    std::string key =
                        std::string(key_val.StringOrDie().value());
                    auto value_lookup = cel_map->Get(nullptr, key_val);
                    if (value_lookup.has_value()) {
                        json_object[key] =
                            toJsonValue(nlohmann::json(), value_lookup.value());
                    }
                }
            }
        }
        return json_object;
    }

    // For unknown types, return the original value
    return original;
}

google::api::expr::runtime::CelValue CelExecutor::fromAvroValue(
    const ::avro::GenericDatum &avro, google::protobuf::Arena *arena) {
    switch (avro.type()) {
        case ::avro::AVRO_BOOL:
            return google::api::expr::runtime::CelValue::CreateBool(
                avro.value<bool>());
        case ::avro::AVRO_INT:
            return google::api::expr::runtime::CelValue::CreateInt64(
                avro.value<int32_t>());
        case ::avro::AVRO_LONG:
            return google::api::expr::runtime::CelValue::CreateInt64(
                avro.value<int64_t>());
        case ::avro::AVRO_FLOAT:
            return google::api::expr::runtime::CelValue::CreateDouble(
                avro.value<float>());
        case ::avro::AVRO_DOUBLE:
            return google::api::expr::runtime::CelValue::CreateDouble(
                avro.value<double>());
        case ::avro::AVRO_STRING: {
            // Use arena allocation for string storage
            auto *arena_str = google::protobuf::Arena::Create<std::string>(
                arena, avro.value<std::string>());
            return google::api::expr::runtime::CelValue::CreateString(
                arena_str);
        }
        case ::avro::AVRO_BYTES: {
            auto bytes_vec = avro.value<std::vector<uint8_t>>();
            // Use arena allocation for bytes storage
            auto *arena_bytes =
                google::protobuf::Arena::Create<std::string>(arena);
            arena_bytes->assign(bytes_vec.begin(), bytes_vec.end());
            return google::api::expr::runtime::CelValue::CreateBytes(
                arena_bytes);
        }
        case ::avro::AVRO_ARRAY: {
            const auto &arr = avro.value<::avro::GenericArray>().value();
            std::vector<google::api::expr::runtime::CelValue> vec;
            for (const auto &item : arr) {
                vec.push_back(fromAvroValue(item, arena));
            }
            auto *list_impl = google::protobuf::Arena::Create<
                google::api::expr::runtime::ContainerBackedListImpl>(arena,
                                                                     vec);
            return google::api::expr::runtime::CelValue::CreateList(list_impl);
        }
        case ::avro::AVRO_MAP: {
            auto *map_impl = google::protobuf::Arena::Create<
                google::api::expr::runtime::CelMapBuilder>(arena);
            const auto &map = avro.value<::avro::GenericMap>().value();
            for (const auto &pair : map) {
                // Use arena allocation for string key
                auto *arena_key = google::protobuf::Arena::Create<std::string>(
                    arena, pair.first);
                auto status = map_impl->Add(
                    google::api::expr::runtime::CelValue::CreateString(
                        arena_key),
                    fromAvroValue(pair.second, arena));
                if (!status.ok()) {
                    // Log error or handle as needed, but continue processing
                }
            }
            return google::api::expr::runtime::CelValue::CreateMap(map_impl);
        }
        case ::avro::AVRO_RECORD: {
            auto *map_impl = google::protobuf::Arena::Create<
                google::api::expr::runtime::CelMapBuilder>(arena);
            const auto &record = avro.value<::avro::GenericRecord>();
            for (size_t i = 0; i < record.schema()->names(); ++i) {
                // Use arena allocation for field name
                auto *arena_name = google::protobuf::Arena::Create<std::string>(
                    arena, record.schema()->nameAt(i));
                auto status = map_impl->Add(
                    google::api::expr::runtime::CelValue::CreateString(
                        arena_name),
                    fromAvroValue(record.fieldAt(i), arena));
                if (!status.ok()) {
                    // Log error or handle as needed, but continue processing
                }
            }
            return google::api::expr::runtime::CelValue::CreateMap(map_impl);
        }
        case ::avro::AVRO_NULL:
            return google::api::expr::runtime::CelValue::CreateNull();
        default:
            return google::api::expr::runtime::CelValue::CreateNull();
    }
}

::avro::GenericDatum CelExecutor::toAvroValue(
    const ::avro::GenericDatum &original,
    const google::api::expr::runtime::CelValue &cel_value) {
    // Convert CEL values back to Avro
    if (cel_value.IsBool()) {
        return ::avro::GenericDatum(cel_value.BoolOrDie());
    } else if (cel_value.IsInt64()) {
        // Avro int is 32-bit, but we can handle both int32 and int64
        return ::avro::GenericDatum(
            static_cast<int64_t>(cel_value.Int64OrDie()));
    } else if (cel_value.IsUint64()) {
        return ::avro::GenericDatum(
            static_cast<int64_t>(cel_value.Uint64OrDie()));
    } else if (cel_value.IsDouble()) {
        return ::avro::GenericDatum(cel_value.DoubleOrDie());
    } else if (cel_value.IsString()) {
        return ::avro::GenericDatum(
            std::string(cel_value.StringOrDie().value()));
    } else if (cel_value.IsNull()) {
        return ::avro::GenericDatum();  // Creates null datum
    } else if (cel_value.IsList()) {
        // Convert CEL list to Avro array
        const auto *cel_list = cel_value.ListOrDie();

        if (original.type() == ::avro::AVRO_ARRAY) {
            // Use existing array schema
            auto orig_array_schema =
                original.value<::avro::GenericArray>().schema();
            ::avro::GenericDatum result_datum{
                ::avro::ValidSchema(orig_array_schema)};
            auto &result_array = result_datum.value<::avro::GenericArray>();

            // Get element template from original if available
            ::avro::GenericDatum element_template;
            auto &orig_array = original.value<::avro::GenericArray>().value();
            if (!orig_array.empty()) {
                element_template = orig_array[0];
            }

            // Recursively convert each list element
            for (int i = 0; i < cel_list->size(); ++i) {
                auto item = cel_list->Get(nullptr, i);
                if (!item.IsError()) {
                    result_array.value().push_back(
                        toAvroValue(element_template, item));
                }
            }

            return result_datum;
        } else {
            // For non-array originals, return the original (we can't create a
            // new schema)
            return original;
        }
    } else if (cel_value.IsMap()) {
        // Convert CEL map to Avro map or record depending on original type
        const auto *cel_map = cel_value.MapOrDie();

        if (original.type() == ::avro::AVRO_RECORD) {
            // Convert to Avro record
            auto orig_record_schema =
                original.value<::avro::GenericRecord>().schema();
            ::avro::GenericDatum result_datum{
                ::avro::ValidSchema(orig_record_schema)};
            auto &result_record = result_datum.value<::avro::GenericRecord>();

            // Copy original record first
            auto &orig_record = original.value<::avro::GenericRecord>();
            for (size_t i = 0; i < orig_record.fieldCount(); ++i) {
                result_record.setFieldAt(i, orig_record.fieldAt(i));
            }

            // Iterate through map entries and update matching fields
            auto map_keys = cel_map->ListKeys(nullptr);
            if (map_keys.ok()) {
                const auto *keys_list = map_keys.value();
                for (int i = 0; i < keys_list->size(); ++i) {
                    auto key_val = keys_list->Get(nullptr, i);
                    if (!key_val.IsError() && key_val.IsString()) {
                        std::string key =
                            std::string(key_val.StringOrDie().value());
                        auto value_lookup = cel_map->Get(nullptr, key_val);
                        if (value_lookup.has_value()) {
                            // Find the field index by name
                            for (size_t field_idx = 0;
                                 field_idx < orig_record_schema->leaves();
                                 ++field_idx) {
                                if (orig_record_schema->nameAt(field_idx) ==
                                    key) {
                                    auto field_template =
                                        orig_record.fieldAt(field_idx);
                                    result_record.setFieldAt(
                                        field_idx,
                                        toAvroValue(field_template,
                                                    value_lookup.value()));
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
            auto orig_map_schema =
                original.value<::avro::GenericMap>().schema();
            ::avro::GenericDatum result_datum{
                ::avro::ValidSchema(orig_map_schema)};
            auto &result_map = result_datum.value<::avro::GenericMap>();

            // Get value template from original if available
            ::avro::GenericDatum value_template;
            auto &orig_map = original.value<::avro::GenericMap>().value();
            if (!orig_map.empty()) {
                value_template = orig_map.begin()->second;
            }

            // Iterate through map entries
            auto map_keys = cel_map->ListKeys(nullptr);
            if (map_keys.ok()) {
                const auto *keys_list = map_keys.value();
                for (int i = 0; i < keys_list->size(); ++i) {
                    auto key_val = keys_list->Get(nullptr, i);
                    if (!key_val.IsError() && key_val.IsString()) {
                        std::string key =
                            std::string(key_val.StringOrDie().value());
                        auto value_lookup = cel_map->Get(nullptr, key_val);
                        if (value_lookup.has_value()) {
                            result_map.value().emplace_back(
                                key, toAvroValue(value_template,
                                                 value_lookup.value()));
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

srclient::serdes::protobuf::ProtobufVariant CelExecutor::toProtobufValue(
    const srclient::serdes::protobuf::ProtobufVariant &original,
    const google::api::expr::runtime::CelValue &cel_value) {
    using namespace srclient::serdes::protobuf;

    if (cel_value.IsBool()) {
        return ProtobufVariant(cel_value.BoolOrDie());
    } else if (cel_value.IsInt64()) {
        int64_t value = cel_value.Int64OrDie();
        switch (original.type) {
            case ProtobufVariant::ValueType::I32:
                return ProtobufVariant(static_cast<int32_t>(value),
                                       ProtobufVariant::ValueType::I32);
            case ProtobufVariant::ValueType::I64:
                return ProtobufVariant(value);
            case ProtobufVariant::ValueType::U32:
                return ProtobufVariant(static_cast<uint32_t>(value));
            case ProtobufVariant::ValueType::U64:
                return ProtobufVariant(static_cast<uint64_t>(value));
            case ProtobufVariant::ValueType::EnumNumber:
                return ProtobufVariant::createEnum(static_cast<int32_t>(value));
            default:
                return ProtobufVariant(value);
        }
    } else if (cel_value.IsUint64()) {
        uint64_t value = cel_value.Uint64OrDie();
        switch (original.type) {
            case ProtobufVariant::ValueType::I32:
                return ProtobufVariant(static_cast<int32_t>(value),
                                       ProtobufVariant::ValueType::I32);
            case ProtobufVariant::ValueType::I64:
                return ProtobufVariant(static_cast<int64_t>(value));
            case ProtobufVariant::ValueType::U32:
                return ProtobufVariant(static_cast<uint32_t>(value));
            case ProtobufVariant::ValueType::U64:
                return ProtobufVariant(value);
            case ProtobufVariant::ValueType::EnumNumber:
                return ProtobufVariant::createEnum(static_cast<int32_t>(value));
            default:
                return ProtobufVariant(value);
        }
    } else if (cel_value.IsDouble()) {
        double value = cel_value.DoubleOrDie();
        if (original.type == ProtobufVariant::ValueType::F32) {
            return ProtobufVariant(static_cast<float>(value));
        } else {
            return ProtobufVariant(value);
        }
    } else if (cel_value.IsString()) {
        std::string value = std::string(cel_value.StringOrDie().value());
        return ProtobufVariant(value);
    } else if (cel_value.IsBytes()) {
        auto bytes_view = cel_value.BytesOrDie();
        std::vector<uint8_t> bytes(bytes_view.value().begin(),
                                   bytes_view.value().end());
        return ProtobufVariant(bytes);
    } else if (cel_value.IsList()) {
        const auto *cel_list = cel_value.ListOrDie();
        std::vector<ProtobufVariant> result_list;

        // Get element template from original if available
        ProtobufVariant element_template(false);  // Default template
        if (original.type == ProtobufVariant::ValueType::List) {
            const auto &orig_list =
                original.get<std::vector<ProtobufVariant>>();
            if (!orig_list.empty()) {
                element_template = orig_list[0];
            }
        }

        // Convert each list element recursively
        for (int i = 0; i < cel_list->size(); ++i) {
            auto item = cel_list->Get(nullptr, i);
            if (!item.IsError()) {
                result_list.push_back(toProtobufValue(element_template, item));
            }
        }

        return ProtobufVariant(result_list);
    } else if (cel_value.IsMap()) {
        const auto *cel_map = cel_value.MapOrDie();
        std::map<MapKey, ProtobufVariant> result_map;

        // Get value template from original if available
        ProtobufVariant value_template(false);  // Default template
        if (original.type == ProtobufVariant::ValueType::Map) {
            const auto &orig_map =
                original.get<std::map<MapKey, ProtobufVariant>>();
            if (!orig_map.empty()) {
                value_template = orig_map.begin()->second;
            }
        }

        // Iterate through map entries
        auto map_keys = cel_map->ListKeys(nullptr);
        if (map_keys.ok()) {
            const auto *keys_list = map_keys.value();
            for (int i = 0; i < keys_list->size(); ++i) {
                auto key_val = keys_list->Get(nullptr, i);
                if (!key_val.IsError()) {
                    auto value_lookup = cel_map->Get(nullptr, key_val);
                    if (value_lookup.has_value()) {
                        // Convert CEL key to MapKey
                        MapKey map_key;
                        if (key_val.IsBool()) {
                            map_key = key_val.BoolOrDie();
                        } else if (key_val.IsInt64()) {
                            map_key = key_val.Int64OrDie();
                        } else if (key_val.IsUint64()) {
                            map_key = key_val.Uint64OrDie();
                        } else if (key_val.IsString()) {
                            map_key =
                                std::string(key_val.StringOrDie().value());
                        } else {
                            // Default to string representation
                            map_key = std::string("unknown");
                        }

                        result_map.emplace(
                            map_key, toProtobufValue(value_template,
                                                     value_lookup.value()));
                    }
                }
            }
        }

        return ProtobufVariant(result_map);
    } else if (cel_value.IsNull()) {
        // Return empty bytes for null values
        return ProtobufVariant(std::vector<uint8_t>());
    } else {
        // For unknown types, return empty bytes
        return ProtobufVariant(std::vector<uint8_t>());
    }
}

google::api::expr::runtime::CelValue CelExecutor::fromProtobufValue(
    const srclient::serdes::protobuf::ProtobufVariant &variant,
    google::protobuf::Arena *arena) {
    using namespace srclient::serdes::protobuf;

    switch (variant.type) {
        case ProtobufVariant::ValueType::Bool:
            return google::api::expr::runtime::CelValue::CreateBool(
                variant.get<bool>());

        case ProtobufVariant::ValueType::I32:
            return google::api::expr::runtime::CelValue::CreateInt64(
                static_cast<int64_t>(variant.get<int32_t>()));

        case ProtobufVariant::ValueType::I64:
            return google::api::expr::runtime::CelValue::CreateInt64(
                variant.get<int64_t>());

        case ProtobufVariant::ValueType::U32:
            return google::api::expr::runtime::CelValue::CreateInt64(
                static_cast<int64_t>(variant.get<uint32_t>()));

        case ProtobufVariant::ValueType::U64:
            return google::api::expr::runtime::CelValue::CreateInt64(
                static_cast<int64_t>(variant.get<uint64_t>()));

        case ProtobufVariant::ValueType::F32:
            return google::api::expr::runtime::CelValue::CreateDouble(
                static_cast<double>(variant.get<float>()));

        case ProtobufVariant::ValueType::F64:
            return google::api::expr::runtime::CelValue::CreateDouble(
                variant.get<double>());

        case ProtobufVariant::ValueType::String: {
            const auto &str = variant.get<std::string>();
            auto *arena_str =
                google::protobuf::Arena::Create<std::string>(arena, str);
            return google::api::expr::runtime::CelValue::CreateString(
                arena_str);
        }

        case ProtobufVariant::ValueType::Bytes: {
            const auto &bytes = variant.get<std::vector<uint8_t>>();
            auto *arena_bytes =
                google::protobuf::Arena::Create<std::string>(arena);
            arena_bytes->assign(bytes.begin(), bytes.end());
            return google::api::expr::runtime::CelValue::CreateBytes(
                arena_bytes);
        }

        case ProtobufVariant::ValueType::EnumNumber:
            return google::api::expr::runtime::CelValue::CreateInt64(
                static_cast<int64_t>(variant.get<int32_t>()));

        case ProtobufVariant::ValueType::Message: {
            const auto &msg =
                variant.get<std::unique_ptr<google::protobuf::Message>>();
            if (!msg) {
                return google::api::expr::runtime::CelValue::CreateNull();
            }

            const auto *descriptor = msg->GetDescriptor();
            if (!descriptor)
                return google::api::expr::runtime::CelValue::CreateNull();
            const auto *reflection = msg->GetReflection();
            if (!reflection)
                return google::api::expr::runtime::CelValue::CreateNull();

            // Create a map to represent the protobuf message
            auto *map_impl = google::protobuf::Arena::Create<
                google::api::expr::runtime::CelMapBuilder>(arena);

            // Get all fields that have values set
            std::vector<const google::protobuf::FieldDescriptor *> fields;
            reflection->ListFields(*msg, &fields);

            for (const auto *field : fields) {
                // Create key for the field name
                auto *arena_field_name =
                    google::protobuf::Arena::Create<std::string>(arena,
                                                                 field->name());
                auto cel_key =
                    google::api::expr::runtime::CelValue::CreateString(
                        arena_field_name);

                google::api::expr::runtime::CelValue cel_value =
                    google::api::expr::runtime::CelValue::CreateNull();

                if (field->is_repeated()) {
                    // Handle repeated fields as lists
                    std::vector<google::api::expr::runtime::CelValue> vec;
                    int field_size = reflection->FieldSize(*msg, field);

                    for (int i = 0; i < field_size; ++i) {
                        cel_value = convertProtobufFieldToCel(
                            *msg, field, reflection, arena, i);
                        if (!cel_value.IsError()) {
                            vec.push_back(cel_value);
                        }
                    }

                    auto *list_impl = google::protobuf::Arena::Create<
                        google::api::expr::runtime::ContainerBackedListImpl>(
                        arena, vec);
                    cel_value =
                        google::api::expr::runtime::CelValue::CreateList(
                            list_impl);
                } else {
                    // Handle single value fields
                    cel_value = convertProtobufFieldToCel(
                        *msg, field, reflection, arena, -1);
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

        case ProtobufVariant::ValueType::List: {
            const auto &list = variant.get<std::vector<ProtobufVariant>>();
            std::vector<google::api::expr::runtime::CelValue> vec;

            for (const auto &item : list) {
                vec.push_back(fromProtobufValue(item, arena));
            }

            auto *list_impl = google::protobuf::Arena::Create<
                google::api::expr::runtime::ContainerBackedListImpl>(arena,
                                                                     vec);
            return google::api::expr::runtime::CelValue::CreateList(list_impl);
        }

        case ProtobufVariant::ValueType::Map: {
            const auto &map = variant.get<std::map<MapKey, ProtobufVariant>>();
            auto *map_impl = google::protobuf::Arena::Create<
                google::api::expr::runtime::CelMapBuilder>(arena);

            for (const auto &[key, value] : map) {
                // Convert MapKey to CEL value
                google::api::expr::runtime::CelValue cel_key;
                std::visit(
                    [&](const auto &k) {
                        using T = std::decay_t<decltype(k)>;
                        if constexpr (std::is_same_v<T, std::string>) {
                            auto *arena_str =
                                google::protobuf::Arena::Create<std::string>(
                                    arena, k);
                            cel_key = google::api::expr::runtime::CelValue::
                                CreateString(arena_str);
                        } else if constexpr (std::is_same_v<T, bool>) {
                            cel_key = google::api::expr::runtime::CelValue::
                                CreateBool(k);
                        } else {
                            // For all integer types, convert to int64
                            cel_key = google::api::expr::runtime::CelValue::
                                CreateInt64(static_cast<int64_t>(k));
                        }
                    },
                    key);

                auto cel_value = fromProtobufValue(value, arena);
                auto status = map_impl->Add(cel_key, cel_value);
                if (!status.ok()) {
                    // Log error but continue processing other entries
                }
            }

            return google::api::expr::runtime::CelValue::CreateMap(map_impl);
        }

        default:
            return google::api::expr::runtime::CelValue::CreateNull();
    }
}

google::api::expr::runtime::CelValue CelExecutor::convertProtobufFieldToCel(
    const google::protobuf::Message &message,
    const google::protobuf::FieldDescriptor *field,
    const google::protobuf::Reflection *reflection,
    google::protobuf::Arena *arena, int index) {
    // For repeated fields, index >= 0; for singular fields, index = -1

    switch (field->cpp_type()) {
        case google::protobuf::FieldDescriptor::CPPTYPE_BOOL: {
            bool value = (index >= 0) ? reflection->GetRepeatedBool(
                                            message, field, index)
                                      : reflection->GetBool(message, field);
            return google::api::expr::runtime::CelValue::CreateBool(value);
        }

        case google::protobuf::FieldDescriptor::CPPTYPE_INT32: {
            int32_t value = (index >= 0) ? reflection->GetRepeatedInt32(
                                               message, field, index)
                                         : reflection->GetInt32(message, field);
            return google::api::expr::runtime::CelValue::CreateInt64(
                static_cast<int64_t>(value));
        }

        case google::protobuf::FieldDescriptor::CPPTYPE_INT64: {
            int64_t value = (index >= 0) ? reflection->GetRepeatedInt64(
                                               message, field, index)
                                         : reflection->GetInt64(message, field);
            return google::api::expr::runtime::CelValue::CreateInt64(value);
        }

        case google::protobuf::FieldDescriptor::CPPTYPE_UINT32: {
            uint32_t value =
                (index >= 0)
                    ? reflection->GetRepeatedUInt32(message, field, index)
                    : reflection->GetUInt32(message, field);
            return google::api::expr::runtime::CelValue::CreateInt64(
                static_cast<int64_t>(value));
        }

        case google::protobuf::FieldDescriptor::CPPTYPE_UINT64: {
            uint64_t value =
                (index >= 0)
                    ? reflection->GetRepeatedUInt64(message, field, index)
                    : reflection->GetUInt64(message, field);
            return google::api::expr::runtime::CelValue::CreateInt64(
                static_cast<int64_t>(value));
        }

        case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT: {
            float value = (index >= 0) ? reflection->GetRepeatedFloat(
                                             message, field, index)
                                       : reflection->GetFloat(message, field);
            return google::api::expr::runtime::CelValue::CreateDouble(
                static_cast<double>(value));
        }

        case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE: {
            double value = (index >= 0) ? reflection->GetRepeatedDouble(
                                              message, field, index)
                                        : reflection->GetDouble(message, field);
            return google::api::expr::runtime::CelValue::CreateDouble(value);
        }

        case google::protobuf::FieldDescriptor::CPPTYPE_STRING: {
            std::string value =
                (index >= 0)
                    ? reflection->GetRepeatedString(message, field, index)
                    : reflection->GetString(message, field);
            // Use arena allocation for string storage
            auto *arena_str =
                google::protobuf::Arena::Create<std::string>(arena, value);

            if (field->type() ==
                google::protobuf::FieldDescriptor::TYPE_BYTES) {
                return google::api::expr::runtime::CelValue::CreateBytes(
                    arena_str);
            } else {
                return google::api::expr::runtime::CelValue::CreateString(
                    arena_str);
            }
        }

        case google::protobuf::FieldDescriptor::CPPTYPE_ENUM: {
            int value = (index >= 0) ? reflection->GetRepeatedEnumValue(
                                           message, field, index)
                                     : reflection->GetEnumValue(message, field);
            return google::api::expr::runtime::CelValue::CreateInt64(
                static_cast<int64_t>(value));
        }

        case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
            const google::protobuf::Message &nested_message =
                (index >= 0)
                    ? reflection->GetRepeatedMessage(message, field, index)
                    : reflection->GetMessage(message, field);

            // Handle map fields specially
            if (field->is_map()) {
                return convertProtobufMapToCel(nested_message, field, arena);
            } else {
                // Recursively convert nested message
                // Create a copy of the message and wrap it in a ProtobufVariant
                auto message_copy = std::unique_ptr<google::protobuf::Message>(
                    nested_message.New());
                message_copy->CopyFrom(nested_message);
                srclient::serdes::protobuf::ProtobufVariant variant(
                    std::move(message_copy));
                return fromProtobufValue(variant, arena);
            }
        }

        default:
            return google::api::expr::runtime::CelValue::CreateNull();
    }
}

google::api::expr::runtime::CelValue CelExecutor::convertProtobufMapToCel(
    const google::protobuf::Message &map_entry,
    const google::protobuf::FieldDescriptor *map_field,
    google::protobuf::Arena *arena) {
    auto *map_impl = google::protobuf::Arena::Create<
        google::api::expr::runtime::CelMapBuilder>(arena);

    const auto *descriptor = map_entry.GetDescriptor();
    const auto *reflection = map_entry.GetReflection();

    if (!descriptor || !reflection) {
        return google::api::expr::runtime::CelValue::CreateMap(map_impl);
    }

    // Map entries have exactly 2 fields: key and value
    const auto *key_field = descriptor->field(0);    // "key"
    const auto *value_field = descriptor->field(1);  // "value"

    if (!key_field || !value_field) {
        return google::api::expr::runtime::CelValue::CreateMap(map_impl);
    }

    // Convert the key
    auto cel_key =
        convertProtobufFieldToCel(map_entry, key_field, reflection, arena, -1);

    // Convert the value
    auto cel_value = convertProtobufFieldToCel(map_entry, value_field,
                                               reflection, arena, -1);

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
    // This matches the Rust version:
    // crate::serdes::rule_registry::register_rule_executor(CelExecutor::new());
    global_registry::registerRuleExecutor(std::make_shared<CelExecutor>());
}

}  // namespace srclient::rules::cel