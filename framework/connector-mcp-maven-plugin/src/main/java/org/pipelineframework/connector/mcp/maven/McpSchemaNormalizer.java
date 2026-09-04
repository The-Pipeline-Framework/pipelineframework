package org.pipelineframework.connector.mcp.maven;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.pipelineframework.config.template.PipelineFieldNullability;
import org.pipelineframework.config.template.PipelineFieldPresence;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.config.template.PipelineTemplateWrapperConstraints;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

/** Lossless importer-v1 projection from the supported JSON Schema subset to canonical v3 types. */
public final class McpSchemaNormalizer {
    private static final ConnectorProviderId NAMESPACE = ConnectorProviderId.of("mcp.client");
    private static final Set<String> UNSUPPORTED = Set.of(
        "$ref", "$defs", "definitions", "oneOf", "anyOf", "allOf", "not", "enum", "const",
        "multipleOf", "minItems", "maxItems", "uniqueItems", "contains", "minContains", "maxContains",
        "prefixItems", "additionalItems", "unevaluatedItems", "minProperties", "maxProperties",
        "patternProperties", "propertyNames", "dependentRequired", "dependentSchemas", "unevaluatedProperties",
        "if", "then", "else", "contentEncoding", "contentMediaType", "contentSchema");

    public List<ProtocolTypeDescriptor> normalize(String rootName, Map<String, Object> schema, String source) {
        String root = requireName(rootName, source);
        State state = new State(source);
        state.object(root, schema, "$", true);
        return state.types.values().stream().toList();
    }

    private static String requireName(String value, String source) {
        try {
            return new ProtocolTypeIdentity(NAMESPACE, value).typeName();
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException(source + " has invalid canonical type name '" + value + "'", failure);
        }
    }

    private static final class State {
        private final String source;
        private final Map<String, ProtocolTypeDescriptor> types = new LinkedHashMap<>();

        private State(String source) {
            this.source = source;
        }

        private PipelineTemplateTypeReference object(
            String name,
            Map<String, Object> schema,
            String path,
            boolean root
        ) {
            rejectUnsupported(schema, path);
            if (!"object".equals(type(schema, path).base())) {
                throw failure(path, root ? "root schema must have type object" : "nested schema must have type object");
            }
            if (!Boolean.FALSE.equals(schema.get("additionalProperties"))) {
                throw failure(path + ".additionalProperties", "must be false for importer v1");
            }
            Map<String, Object> properties = schema.containsKey("properties")
                ? map(schema.get("properties"), path + ".properties") : Map.of();
            Set<String> required = strings(schema.get("required"), path + ".required");
            if (!properties.keySet().containsAll(required)) {
                throw failure(path + ".required", "names an undeclared property");
            }
            List<PipelineTemplateTypeDefinition.Field> fields = new ArrayList<>();
            properties.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                String fieldPath = path + ".properties." + entry.getKey();
                Map<String, Object> fieldSchema = map(entry.getValue(), fieldPath);
                JsonType fieldType = type(fieldSchema, fieldPath);
                boolean isRequired = required.contains(entry.getKey());
                if ("array".equals(fieldType.base())) {
                    rejectUnsupported(fieldSchema, fieldPath);
                    if (!isRequired || fieldType.nullable()) {
                        throw failure(fieldPath, "arrays must be required and non-null in importer v1");
                    }
                    Map<String, Object> items = map(fieldSchema.get("items"), fieldPath + ".items");
                    JsonType itemType = type(items, fieldPath + ".items");
                    if (itemType.nullable() || "array".equals(itemType.base())) {
                        throw failure(fieldPath + ".items", "must be a non-null scalar or object");
                    }
                    fields.add(new PipelineTemplateTypeDefinition.Field(
                        entry.getKey(), reference(name, entry.getKey(), items, itemType, fieldPath + ".items"), true));
                } else {
                    fields.add(new PipelineTemplateTypeDefinition.Field(
                        entry.getKey(), reference(name, entry.getKey(), fieldSchema, fieldType, fieldPath), false,
                        isRequired ? PipelineFieldPresence.REQUIRED : PipelineFieldPresence.OPTIONAL,
                        fieldType.nullable() ? PipelineFieldNullability.NULLABLE : PipelineFieldNullability.NON_NULL));
                }
            });
            ProtocolTypeIdentity identity = new ProtocolTypeIdentity(NAMESPACE, name);
            ProtocolTypeDescriptor descriptor = new ProtocolTypeDescriptor(
                identity, new PipelineTemplateTypeDefinition.RecordType(name, fields));
            if (types.putIfAbsent(name, descriptor) != null) {
                throw failure(path, "derives duplicate canonical type name '" + name + "'");
            }
            return new PipelineTemplateTypeReference.Contributed(identity.qualifiedName());
        }

        private PipelineTemplateTypeReference reference(
            String owner,
            String property,
            Map<String, Object> schema,
            JsonType type,
            String path
        ) {
            rejectUnsupported(schema, path);
            if ("object".equals(type.base())) {
                return object(owner + pascal(property), schema, path, false);
            }
            String scalar = scalar(schema, type.base(), path);
            PipelineTemplateWrapperConstraints constraints = constraints(schema, scalar, path);
            if (constraints.isEmpty()) {
                return new PipelineTemplateTypeReference.Scalar(scalar);
            }
            String wrapperName = owner + pascal(property) + "Value";
            ProtocolTypeIdentity identity = new ProtocolTypeIdentity(NAMESPACE, wrapperName);
            ProtocolTypeDescriptor wrapper = new ProtocolTypeDescriptor(identity,
                new PipelineTemplateTypeDefinition.WrapperType(
                    wrapperName, new PipelineTemplateTypeReference.Scalar(scalar), constraints));
            if (types.putIfAbsent(wrapperName, wrapper) != null) {
                throw failure(path, "derives duplicate canonical type name '" + wrapperName + "'");
            }
            return new PipelineTemplateTypeReference.Contributed(identity.qualifiedName());
        }

        private String scalar(Map<String, Object> schema, String type, String path) {
            String format = optionalString(schema.get("format"), path + ".format").orElse("");
            return switch (type) {
                case "string" -> switch (format) {
                    case "", "email" -> "string";
                    case "uuid" -> "uuid";
                    case "date-time" -> "timestamp";
                    case "date" -> "date";
                    case "duration" -> "duration";
                    case "uri" -> "uri";
                    default -> throw failure(path + ".format", "unsupported string format '" + format + "'");
                };
                case "boolean" -> "bool";
                case "integer" -> "int32".equals(format) ? "int32" : "int64";
                case "number" -> "float".equals(format) ? "float32" : "double".equals(format) ? "float64" : "decimal";
                default -> throw failure(path + ".type", "unsupported type '" + type + "'");
            };
        }

        private PipelineTemplateWrapperConstraints constraints(Map<String, Object> schema, String scalar, String path) {
            Optional<String> pattern = optionalString(schema.get("pattern"), path + ".pattern");
            Optional<PipelineTemplateWrapperConstraints.Format> format = "email".equals(schema.get("format"))
                ? Optional.of(PipelineTemplateWrapperConstraints.Format.EMAIL) : Optional.empty();
            return new PipelineTemplateWrapperConstraints(
                optionalInt(schema.get("minLength"), path + ".minLength"),
                optionalInt(schema.get("maxLength"), path + ".maxLength"), pattern, format,
                optionalDecimal(schema.get("minimum"), path + ".minimum"),
                optionalDecimal(schema.get("exclusiveMinimum"), path + ".exclusiveMinimum"),
                optionalDecimal(schema.get("maximum"), path + ".maximum"),
                optionalDecimal(schema.get("exclusiveMaximum"), path + ".exclusiveMaximum"));
        }

        private void rejectUnsupported(Map<String, Object> schema, String path) {
            UNSUPPORTED.stream().filter(schema::containsKey).sorted().findFirst().ifPresent(key -> {
                throw failure(path + "." + key, "is not supported by importer v1");
            });
        }

        private IllegalArgumentException failure(String path, String reason) {
            return new IllegalArgumentException(source + " schema " + path + " " + reason);
        }

        private JsonType type(Map<String, Object> schema, String path) {
            Object raw = schema.get("type");
            if (raw instanceof String value) {
                return new JsonType(value, false);
            }
            if (raw instanceof List<?> values && values.size() == 2 && values.contains("null")) {
                Object base = values.get(0).equals("null") ? values.get(1) : values.get(0);
                if (base instanceof String value) {
                    return new JsonType(value, true);
                }
            }
            throw failure(path + ".type", "must be a supported JSON Schema type");
        }

        @SuppressWarnings("unchecked")
        private Map<String, Object> map(Object value, String path) {
            if (!(value instanceof Map<?, ?> raw)) {
                throw failure(path, "must be an object");
            }
            Map<String, Object> result = new LinkedHashMap<>();
            raw.forEach((key, item) -> {
                if (!(key instanceof String text)) {
                    throw failure(path, "must use string keys");
                }
                result.put(text, item);
            });
            return result;
        }

        private Set<String> strings(Object value, String path) {
            if (value == null) {
                return Set.of();
            }
            if (!(value instanceof List<?> list)) {
                throw failure(path, "must be an array of strings");
            }
            Set<String> result = new LinkedHashSet<>();
            for (Object item : list) {
                if (!(item instanceof String text) || !result.add(text)) {
                    throw failure(path, "must contain unique strings");
                }
            }
            return result;
        }

        private Optional<String> optionalString(Object value, String path) {
            if (value == null) {
                return Optional.empty();
            }
            if (!(value instanceof String text)) {
                throw failure(path, "must be a string");
            }
            return Optional.of(text);
        }

        private Optional<Integer> optionalInt(Object value, String path) {
            if (value == null) {
                return Optional.empty();
            }
            if (!(value instanceof Number number) || number.intValue() < 0) {
                throw failure(path, "must be a non-negative integer");
            }
            return Optional.of(number.intValue());
        }

        private Optional<BigDecimal> optionalDecimal(Object value, String path) {
            if (value == null) {
                return Optional.empty();
            }
            if (!(value instanceof Number number)) {
                throw failure(path, "must be a number");
            }
            return Optional.of(new BigDecimal(number.toString()));
        }

        private static String pascal(String value) {
            String[] parts = value.split("[^A-Za-z0-9]+");
            StringBuilder result = new StringBuilder();
            for (String part : parts) {
                if (!part.isEmpty()) {
                    result.append(part.substring(0, 1).toUpperCase(Locale.ROOT)).append(part.substring(1));
                }
            }
            if (result.isEmpty()) {
                throw new IllegalArgumentException("JSON Schema property cannot derive a canonical type name: " + value);
            }
            return result.toString();
        }
    }

    private record JsonType(String base, boolean nullable) {
    }
}
