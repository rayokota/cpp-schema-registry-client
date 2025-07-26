#include "srclient/serdes/avro/AvroUtils.h"

#include <avro/Exception.hh>
#include <avro/Stream.hh>
#include <iostream>
#include <sstream>

#include "srclient/serdes/RuleRegistry.h"
#include "srclient/serdes/Serde.h"
#include "srclient/serdes/avro/AvroTypes.h"

namespace srclient::serdes::avro {

namespace utils {

::avro::GenericDatum transformFields(RuleContext &ctx,
                                     const ::avro::ValidSchema &schema,
                                     const ::avro::GenericDatum &datum) {
    switch (schema.root()->type()) {
        case ::avro::AVRO_RECORD: {
            auto record = datum.value<::avro::GenericRecord>();
            ::avro::GenericRecord result(schema.root());

            for (size_t i = 0; i < record.fieldCount(); ++i) {
                auto field_datum = record.fieldAt(i);
                auto field_schema_node = schema.root()->leafAt(i);
                ::avro::ValidSchema field_schema(field_schema_node);
                const std::string &field_name = schema.root()->nameAt(i);

                auto transformed_field = transformFieldWithContext(
                    ctx, schema, field_name, field_datum, field_schema);
                result.setFieldAt(i, transformed_field);
            }

            // Create a new GenericDatum with the schema and set the record
            // value
            ::avro::GenericDatum result_datum(schema);
            result_datum.value<::avro::GenericRecord>() = result;
            return result_datum;
        }

        case ::avro::AVRO_ARRAY: {
            auto array = datum.value<::avro::GenericArray>();
            ::avro::GenericDatum result_datum(schema);
            auto &result = result_datum.value<::avro::GenericArray>();
            auto item_schema_node = schema.root()->leafAt(0);
            ::avro::ValidSchema item_schema(item_schema_node);

            for (size_t i = 0; i < array.value().size(); ++i) {
                auto transformed =
                    transformFields(ctx, item_schema, array.value()[i]);
                result.value().push_back(transformed);
            }

            return result_datum;
        }

        case ::avro::AVRO_MAP: {
            auto map = datum.value<::avro::GenericMap>();
            ::avro::GenericDatum result_datum(schema);
            auto &result = result_datum.value<::avro::GenericMap>();
            for (const auto &[key, value] : map.value()) {
                auto value_schema_node = schema.root()->leafAt(0);
                ::avro::ValidSchema value_schema(value_schema_node);
                auto transformed = transformFields(ctx, value_schema, value);
                result.value().emplace_back(key, transformed);
            }
            return result_datum;
        }

        case ::avro::AVRO_UNION: {
            auto union_val = datum.value<::avro::GenericUnion>();
            auto [branch_idx, branch_schema] = resolveUnion(schema, datum);
            auto transformed =
                transformFields(ctx, branch_schema, union_val.datum());

            ::avro::GenericDatum result_datum(schema);
            auto &result = result_datum.value<::avro::GenericUnion>();
            result.selectBranch(branch_idx);
            result.datum() = transformed;

            return result_datum;
        }

        default: {
            // Field-level transformation logic
            auto field_ctx = ctx.currentField();
            if (field_ctx.has_value()) {
                field_ctx->setFieldType(avroSchemaToFieldType(schema));

                auto rule_tags = ctx.getRule().getTags();
                std::unordered_set<std::string> rule_tags_set;
                if (rule_tags.has_value()) {
                    rule_tags_set = std::unordered_set<std::string>(
                        rule_tags->begin(), rule_tags->end());
                }

                // Check if rule tags overlap with field context tags (empty
                // rule_tags means apply to all)
                bool should_apply =
                    !rule_tags.has_value() || rule_tags_set.empty();
                if (!should_apply) {
                    const auto &field_tags = field_ctx->getTags();
                    for (const auto &field_tag : field_tags) {
                        if (rule_tags_set.find(field_tag) !=
                            rule_tags_set.end()) {
                            should_apply = true;
                            break;
                        }
                    }
                }

                if (should_apply) {
                    auto message_value = makeAvroValue(datum);

                    // Get field executor type from the rule
                    auto field_executor_type =
                        ctx.getRule().getType().value_or("");

                    // Try to get executor from context's rule registry first,
                    // then global
                    std::shared_ptr<RuleExecutor> executor;
                    if (ctx.getRuleRegistry()) {
                        executor = ctx.getRuleRegistry()->getExecutor(
                            field_executor_type);
                    }
                    if (!executor) {
                        executor = global_registry::getRuleExecutor(
                            field_executor_type);
                    }

                    if (executor) {
                        auto field_executor =
                            std::dynamic_pointer_cast<FieldRuleExecutor>(
                                executor);
                        if (!field_executor) {
                            throw AvroError("executor " + field_executor_type +
                                            " is not a field rule executor");
                        }

                        auto new_value =
                            field_executor->transformField(ctx, *message_value);
                        if (new_value &&
                            new_value->getFormat() == SerdeFormat::Avro) {
                            return asAvro(*new_value);
                        }
                    }
                }
            }

            return datum;
        }
    }
}

// Transform individual field with context handling
::avro::GenericDatum transformFieldWithContext(
    RuleContext &ctx, const ::avro::ValidSchema &record_schema,
    const std::string &field_name, const ::avro::GenericDatum &field_datum,
    const ::avro::ValidSchema &field_schema) {
    // Get field type from schema
    FieldType field_type = avroSchemaToFieldType(field_schema);

    // Create full field name
    std::string schema_name = getSchemaName(record_schema).value_or("unknown");
    std::string full_name = schema_name + "." + field_name;

    // Create message value from current field datum
    auto message_value = makeAvroValue(field_datum);

    // Get inline tags for the field (placeholder implementation)
    std::unordered_set<std::string> inline_tags = {};

    // Enter field context
    ctx.enterField(*message_value, full_name, field_name, field_type,
                   inline_tags);

    try {
        // Transform the field value (synchronous call)
        ::avro::GenericDatum new_value =
            transformFields(ctx, field_schema, field_datum);

        // Check for condition rules
        auto rule_kind = ctx.getRule().getKind();
        if (rule_kind.has_value() && rule_kind.value() == Kind::Condition) {
            if (new_value.type() == ::avro::AVRO_BOOL) {
                bool condition_result = new_value.value<bool>();
                if (!condition_result) {
                    throw AvroError("Rule condition failed for field: " +
                                    field_name);
                }
            }
        }

        ctx.exitField();
        return new_value;

    } catch (const std::exception &e) {
        ctx.exitField();
        throw;
    }
}

FieldType avroSchemaToFieldType(const ::avro::ValidSchema &schema) {
    switch (schema.root()->type()) {
        case ::avro::AVRO_NULL:
            return FieldType::Null;
        case ::avro::AVRO_BOOL:
            return FieldType::Boolean;
        case ::avro::AVRO_INT:
            return FieldType::Int;
        case ::avro::AVRO_LONG:
            return FieldType::Long;
        case ::avro::AVRO_FLOAT:
            return FieldType::Float;
        case ::avro::AVRO_DOUBLE:
            return FieldType::Double;
        case ::avro::AVRO_BYTES:
            return FieldType::Bytes;
        case ::avro::AVRO_STRING:
            return FieldType::String;
        case ::avro::AVRO_RECORD:
            return FieldType::Record;
        case ::avro::AVRO_ENUM:
            return FieldType::Enum;
        case ::avro::AVRO_ARRAY:
            return FieldType::Array;
        case ::avro::AVRO_MAP:
            return FieldType::Map;
        case ::avro::AVRO_UNION:
            return FieldType::Combined;
        case ::avro::AVRO_FIXED:
            return FieldType::Fixed;
        case ::avro::AVRO_SYMBOLIC:
            return FieldType::Record;  // Assume symbolic references are records
        default:
            return FieldType::String;  // Default fallback
    }
}

jsoncons::ojson avroToJson(const ::avro::GenericDatum &datum) {
    switch (datum.type()) {
        case ::avro::AVRO_NULL:
            return jsoncons::ojson::null();

        case ::avro::AVRO_BOOL:
            return jsoncons::ojson(datum.value<bool>());

        case ::avro::AVRO_INT:
            return jsoncons::ojson(datum.value<int32_t>());

        case ::avro::AVRO_LONG:
            return jsoncons::ojson(datum.value<int64_t>());

        case ::avro::AVRO_FLOAT:
            return jsoncons::ojson(datum.value<float>());

        case ::avro::AVRO_DOUBLE:
            return jsoncons::ojson(datum.value<double>());

        case ::avro::AVRO_STRING:
            return jsoncons::ojson(datum.value<std::string>());

        case ::avro::AVRO_BYTES: {
            const auto &bytes = datum.value<std::vector<uint8_t>>();
            jsoncons::ojson array = jsoncons::ojson::array();
            for (uint8_t byte : bytes) {
                array.push_back(static_cast<unsigned int>(byte));
            }
            return array;
        }

        case ::avro::AVRO_FIXED: {
            const auto &fixed = datum.value<::avro::GenericFixed>();
            jsoncons::ojson array = jsoncons::ojson::array();
            for (size_t i = 0; i < fixed.value().size(); ++i) {
                array.push_back(static_cast<unsigned int>(fixed.value()[i]));
            }
            return array;
        }

        case ::avro::AVRO_ENUM: {
            const auto &enum_val = datum.value<::avro::GenericEnum>();
            return jsoncons::ojson(enum_val.symbol());
        }

        case ::avro::AVRO_ARRAY: {
            const auto &array = datum.value<::avro::GenericArray>();
            jsoncons::ojson json_array = jsoncons::ojson::array();
            for (size_t i = 0; i < array.value().size(); ++i) {
                json_array.push_back(avroToJson(array.value()[i]));
            }
            return json_array;
        }

        case ::avro::AVRO_MAP: {
            const auto &map = datum.value<::avro::GenericMap>();
            jsoncons::ojson json_obj = jsoncons::ojson::object();
            for (const auto &pair : map.value()) {
                json_obj[pair.first] = avroToJson(pair.second);
            }
            return json_obj;
        }

        case ::avro::AVRO_RECORD: {
            const auto &record = datum.value<::avro::GenericRecord>();
            jsoncons::ojson json_obj = jsoncons::ojson::object();
            for (size_t i = 0; i < record.fieldCount(); ++i) {
                json_obj[record.schema()->nameAt(i)] =
                    avroToJson(record.fieldAt(i));
            }
            return json_obj;
        }

        case ::avro::AVRO_UNION: {
            const auto &union_val = datum.value<::avro::GenericUnion>();
            return avroToJson(union_val.datum());
        }

        default:
            throw AvroError("Unsupported Avro type for JSON conversion");
    }
}

::avro::GenericDatum jsonToAvro(const jsoncons::ojson &json_value,
                                const ::avro::ValidSchema &schema) {
    switch (schema.root()->type()) {
        case ::avro::AVRO_NULL:
            return ::avro::GenericDatum();

        case ::avro::AVRO_BOOL:
            if (!json_value.is_bool()) {
                throw AvroError("Expected boolean value");
            }
            return ::avro::GenericDatum(json_value.as<bool>());

        case ::avro::AVRO_INT:
            if (!json_value.is_int64()) {
                throw AvroError("Expected integer value");
            }
            return ::avro::GenericDatum(json_value.as<int32_t>());

        case ::avro::AVRO_LONG:
            if (!json_value.is_number()) {
                throw AvroError("Expected number value for long");
            }
            return ::avro::GenericDatum(json_value.as<int64_t>());

        case ::avro::AVRO_FLOAT:
            if (!json_value.is_number()) {
                throw AvroError("Expected number value for float");
            }
            return ::avro::GenericDatum(json_value.as<float>());

        case ::avro::AVRO_DOUBLE:
            if (!json_value.is_number()) {
                throw AvroError("Expected number value for double");
            }
            return ::avro::GenericDatum(json_value.as<double>());

        case ::avro::AVRO_STRING:
            if (!json_value.is_string()) {
                throw AvroError("Expected string value");
            }
            return ::avro::GenericDatum(json_value.as<std::string>());

        case ::avro::AVRO_BYTES: {
            if (!json_value.is_array()) {
                throw AvroError("Expected array for bytes");
            }
            std::vector<uint8_t> bytes;
            for (const auto &elem : json_value.array_range()) {
                if (!elem.is_uint64()) {
                    throw AvroError(
                        "Expected unsigned integers in bytes array");
                }
                bytes.push_back(elem.as<uint8_t>());
            }
            return ::avro::GenericDatum(bytes);
        }

        case ::avro::AVRO_ENUM: {
            if (!json_value.is_string()) {
                throw AvroError("Expected string value for enum");
            }
            ::avro::GenericDatum datum(schema);
            auto &enum_val = datum.value<::avro::GenericEnum>();
            enum_val.set(json_value.as<std::string>());
            return datum;
        }

        case ::avro::AVRO_ARRAY: {
            if (!json_value.is_array()) {
                throw AvroError("Expected array value");
            }
            ::avro::GenericDatum datum(schema);
            auto &array = datum.value<::avro::GenericArray>();
            auto item_schema = schema.root()->leafAt(0);
            ::avro::ValidSchema item_valid_schema(item_schema);

            for (const auto &item : json_value.array_range()) {
                array.value().push_back(jsonToAvro(item, item_valid_schema));
            }
            return datum;
        }

        case ::avro::AVRO_MAP: {
            if (!json_value.is_object()) {
                throw AvroError("Expected object value for map");
            }
            ::avro::GenericDatum datum(schema);
            auto &map = datum.value<::avro::GenericMap>();
            auto value_schema = schema.root()->leafAt(0);
            ::avro::ValidSchema value_valid_schema(value_schema);

            for (const auto &member : json_value.object_range()) {
                map.value().push_back(
                    {member.key(), jsonToAvro(member.value(), value_valid_schema)});
            }
            return datum;
        }

        case ::avro::AVRO_RECORD: {
            if (!json_value.is_object()) {
                throw AvroError("Expected object value for record");
            }
            ::avro::GenericDatum datum(schema);
            auto &record = datum.value<::avro::GenericRecord>();

            for (size_t i = 0; i < schema.root()->leaves(); ++i) {
                const std::string &field_name = schema.root()->nameAt(i);
                if (json_value.contains(field_name)) {
                    auto field_schema = schema.root()->leafAt(i);
                    ::avro::ValidSchema field_valid_schema(field_schema);
                    record.setFieldAt(
                        i, jsonToAvro(json_value[field_name], field_valid_schema));
                }
            }
            return datum;
        }

        default:
            throw AvroError(
                "Unsupported schema type for JSON to Avro conversion");
    }
}

std::pair<size_t, ::avro::ValidSchema> resolveUnion(
    const ::avro::ValidSchema &union_schema,
    const ::avro::GenericDatum &datum) {
    if (union_schema.root()->type() != ::avro::AVRO_UNION) {
        throw AvroError("Schema is not a union type");
    }

    // Try to find matching branch based on datum type
    for (size_t i = 0; i < union_schema.root()->leaves(); ++i) {
        auto branch_schema = union_schema.root()->leafAt(i);
        if (branch_schema->type() == datum.type()) {
            return {i, ::avro::ValidSchema(branch_schema)};
        }
    }

    throw AvroError("No matching union branch found for datum type");
}

std::optional<std::string> getSchemaName(const ::avro::ValidSchema &schema) {
    if (schema.root()->type() == ::avro::AVRO_RECORD) {
        return schema.root()->name().fullname();
    }
    return std::nullopt;
}

std::vector<uint8_t> serializeAvroData(
    const ::avro::GenericDatum &datum, const ::avro::ValidSchema &writer_schema,
    const std::vector<::avro::ValidSchema> &named_schemas) {
    try {
        auto output_stream = ::avro::memoryOutputStream();
        auto encoder = ::avro::binaryEncoder();
        encoder->init(*output_stream);

        ::avro::encode(*encoder, datum);
        encoder->flush();

        // Get the data from the output stream
        auto input_stream = ::avro::memoryInputStream(*output_stream);
        const uint8_t *data = nullptr;
        size_t size = 0;
        if (input_stream->next(&data, &size)) {
            return std::vector<uint8_t>(data, data + size);
        }
        return {};
    } catch (const ::avro::Exception &e) {
        throw AvroError(e);
    }
}

::avro::GenericDatum deserializeAvroData(
    const std::vector<uint8_t> &data, const ::avro::ValidSchema &writer_schema,
    const ::avro::ValidSchema *reader_schema,
    const std::vector<::avro::ValidSchema> &named_schemas) {
    try {
        auto input_stream = ::avro::memoryInputStream(data.data(), data.size());
        auto decoder = ::avro::binaryDecoder();
        decoder->init(*input_stream);

        ::avro::GenericDatum datum(reader_schema ? *reader_schema
                                                 : writer_schema);
        ::avro::decode(*decoder, datum);

        return datum;
    } catch (const ::avro::Exception &e) {
        throw AvroError(e);
    }
}

std::pair<::avro::ValidSchema, std::vector<::avro::ValidSchema>>
parseSchemaWithNamed(const std::string &schema_str,
                     const std::vector<std::string> &named_schemas) {
    // Currently Avro does not support references to named schema
    // The code below is a placeholder for future implementation
    try {
        // Parse named schemas first
        std::vector<::avro::ValidSchema> parsed_named;
        for (const auto &named_schema_str : named_schemas) {
            ::avro::ValidSchema named_schema =
                compileJsonSchema(named_schema_str);
            parsed_named.push_back(named_schema);
        }

        // Parse main schema
        ::avro::ValidSchema main_schema = compileJsonSchema(schema_str);

        return {main_schema, parsed_named};
    } catch (const ::avro::Exception &e) {
        throw AvroError(e);
    }
}

std::string impliedNamespace(const std::string &name) {
    size_t last_dot = name.find_last_of('.');
    if (last_dot != std::string::npos && last_dot > 0) {
        return name.substr(0, last_dot);
    }
    return "";
}

std::unordered_map<std::string, std::unordered_set<std::string>> getInlineTags(
    const jsoncons::ojson &schema) {
    std::unordered_map<std::string, std::unordered_set<std::string>>
        inline_tags;
    getInlineTagsRecursively("", "", schema, inline_tags);
    return inline_tags;
}

void getInlineTagsRecursively(
    const std::string &ns, const std::string &name,
    const jsoncons::ojson &schema,
    std::unordered_map<std::string, std::unordered_set<std::string>> &tags) {
    if (schema.is_null()) {
        return;
    }

    if (schema.is_array()) {
        for (const auto &subschema : schema.array_range()) {
            getInlineTagsRecursively(ns, name, subschema, tags);
        }
    } else if (!schema.is_object()) {
        // string schemas; this could be either a named schema or a primitive
        // type
        return;
    } else {
        if (!schema.contains("type")) {
            return;
        }

        std::string schema_type = schema["type"].is_string()
                                      ? schema["type"].as<std::string>()
                                      : "";

        if (schema_type == "array") {
            if (schema.contains("items")) {
                getInlineTagsRecursively(ns, name, schema["items"], tags);
            }
        } else if (schema_type == "map") {
            if (schema.contains("values")) {
                getInlineTagsRecursively(ns, name, schema["values"], tags);
            }
        } else if (schema_type == "record") {
            std::string record_ns;
            std::string record_name;

            if (schema.contains("namespace") && schema["namespace"].is_string()) {
                record_ns = schema["namespace"].as<std::string>();
            } else {
                record_ns = impliedNamespace(name);
                if (record_ns.empty()) {
                    record_ns = ns;
                }
            }

            if (schema.contains("name") && schema["name"].is_string()) {
                record_name = schema["name"].as<std::string>();

                // Add namespace prefix if needed
                if (!record_ns.empty() && record_name.find(record_ns) != 0) {
                    record_name = record_ns + "." + record_name;
                }
            }

            if (schema.contains("fields") && schema["fields"].is_array()) {
                for (const auto &field : schema["fields"].array_range()) {
                    if (!field.is_object()) continue;

                    if (field.contains("confluent:tags") &&
                        field.contains("name") &&
                        field["confluent:tags"].is_array() &&
                        field["name"].is_string()) {
                        std::string field_name =
                            field["name"].as<std::string>();
                        std::string full_field_name =
                            record_name + "." + field_name;

                        for (const auto &tag : field["confluent:tags"].array_range()) {
                            if (tag.is_string()) {
                                tags[full_field_name].insert(
                                    tag.as<std::string>());
                            }
                        }
                    }

                    if (field.contains("type")) {
                        getInlineTagsRecursively(record_ns, record_name,
                                                 field["type"], tags);
                    }
                }
            }
        }
    }
}

/**
 * Recursively remove "confluent:tags" properties from a JSON schema
 * @param schema JSON schema object to clean
 */
void removeConfluentTags(jsoncons::ojson &schema) {
    if (schema.is_object()) {
        // Remove confluent:tags if it exists
        if (schema.contains("confluent:tags")) {
            schema.erase("confluent:tags");
        }

        // Recursively process all values in the object
        for (auto &member : schema.object_range()) {
            removeConfluentTags(const_cast<jsoncons::ojson&>(member.value()));
        }
    } else if (schema.is_array()) {
        // Recursively process all elements in the array
        for (auto &element : schema.array_range()) {
            removeConfluentTags(const_cast<jsoncons::ojson&>(element));
        }
    }
}

::avro::ValidSchema compileJsonSchema(const std::string &schema_str) {
    try {
        // Parse the schema string as JSON
        jsoncons::ojson schema_json = jsoncons::ojson::parse(schema_str);

        // Remove confluent:tags properties recursively
        removeConfluentTags(schema_json);

        // Convert back to string
        std::string cleaned_schema_str = schema_json.to_string();

        // Create istringstream and compile the schema
        std::istringstream schema_stream(cleaned_schema_str);
        ::avro::ValidSchema avro_schema;
        ::avro::compileJsonSchema(schema_stream, avro_schema);

        return avro_schema;
    } catch (const std::exception &e) {
        throw AvroError("Failed to parse schema JSON: " +
                        std::string(e.what()));
    } catch (const ::avro::Exception &e) {
        throw AvroError("Failed to compile Avro schema: " +
                        std::string(e.what()));
    }
}

}  // namespace utils

}  // namespace srclient::serdes::avro