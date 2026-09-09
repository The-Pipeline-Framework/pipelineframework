package org.pipelineframework.processor.representation;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.config.template.PipelineFieldNullability;
import org.pipelineframework.config.template.PipelineFieldPresence;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeModel;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;

/** Deterministic JSON Schema view of the compiler-owned canonical v3 type universe. */
public final class CanonicalSchemaJson {
    private CanonicalSchemaJson() {
    }

    public static String render(PipelineTemplateTypeModel model, String typeName) {
        JsonNode schema = schema(model, new PipelineTemplateTypeReference.Named(typeName), new HashSet<>());
        try {
            return PipelineJson.mapper().writeValueAsString(schema);
        } catch (com.fasterxml.jackson.core.JsonProcessingException impossible) {
            throw new IllegalStateException("unable to render canonical schema for " + typeName, impossible);
        }
    }

    private static JsonNode schema(
        PipelineTemplateTypeModel model,
        PipelineTemplateTypeReference reference,
        Set<String> visiting
    ) {
        if (reference instanceof PipelineTemplateTypeReference.Scalar scalar) return scalar(scalar.name());
        if (reference instanceof PipelineTemplateTypeReference.MapType) {
            return JsonNodeFactory.instance.objectNode().put("type", "object");
        }
        String name = reference.name();
        if (!visiting.add(name)) {
            throw new IllegalArgumentException("recursive canonical type requires an explicit curated HTTP mapping: " + name);
        }
        PipelineTemplateTypeDefinition definition = model.definition(name).orElseThrow(() ->
            new IllegalArgumentException("canonical type has no normalized definition: " + name));
        JsonNode result = definition(model, definition, visiting);
        visiting.remove(name);
        return result;
    }

    private static JsonNode definition(
        PipelineTemplateTypeModel model,
        PipelineTemplateTypeDefinition definition,
        Set<String> visiting
    ) {
        if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
            ObjectNode result = JsonNodeFactory.instance.objectNode().put("type", "object");
            result.put("additionalProperties", false);
            ObjectNode properties = result.putObject("properties");
            ArrayNode required = result.putArray("required");
            record.fields().stream().sorted(java.util.Comparator.comparing(PipelineTemplateTypeDefinition.Field::name))
                .forEach(field -> {
                    JsonNode value = schema(model, field.type(), visiting);
                    if (field.repeated()) {
                        ObjectNode array = JsonNodeFactory.instance.objectNode().put("type", "array");
                        array.set("items", value);
                        field.constraints().minItems().ifPresent(limit -> array.put("minItems", limit));
                        field.constraints().maxItems().ifPresent(limit -> array.put("maxItems", limit));
                        value = array;
                    }
                    if (field.nullability() == PipelineFieldNullability.NULLABLE && value instanceof ObjectNode object) {
                        JsonNode type = object.path("type");
                        if (type.isTextual()) {
                            ArrayNode types = JsonNodeFactory.instance.arrayNode().add(type.textValue()).add("null");
                            object.set("type", types);
                        }
                    }
                    properties.set(field.name(), value);
                    if (field.presence() == PipelineFieldPresence.REQUIRED) required.add(field.name());
                });
            return result;
        }
        if (definition instanceof PipelineTemplateTypeDefinition.WrapperType wrapper) {
            ObjectNode result = scalar(wrapper.wraps().name());
            wrapper.constraints().minLength().ifPresent(value -> result.put("minLength", value));
            wrapper.constraints().maxLength().ifPresent(value -> result.put("maxLength", value));
            wrapper.constraints().pattern().ifPresent(value -> result.put("pattern", value));
            wrapper.constraints().format()
                .ifPresent(value -> result.put("format", value.name().toLowerCase(java.util.Locale.ROOT)));
            wrapper.constraints().minimum().ifPresent(value -> result.put("minimum", value));
            wrapper.constraints().minimumExclusive().ifPresent(value -> result.put("exclusiveMinimum", value));
            wrapper.constraints().maximum().ifPresent(value -> result.put("maximum", value));
            wrapper.constraints().maximumExclusive().ifPresent(value -> result.put("exclusiveMaximum", value));
            if (!wrapper.constraints().allowedValues().isEmpty()) {
                ArrayNode allowed = result.putArray("enum");
                wrapper.constraints().allowedValues().forEach(value -> allowed.addPOJO(value));
            }
            return result;
        }
        if (definition instanceof PipelineTemplateTypeDefinition.AliasType alias) {
            return schema(model, alias.target(), visiting);
        }
        PipelineTemplateTypeDefinition.UnionType union = (PipelineTemplateTypeDefinition.UnionType) definition;
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        ArrayNode oneOf = result.putArray("oneOf");
        union.variants().entrySet().stream().sorted(java.util.Map.Entry.comparingByKey())
            .forEach(entry -> oneOf.add(schema(model, entry.getValue().payload(), visiting)));
        return result;
    }

    private static ObjectNode scalar(String name) {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        switch (name) {
            case "bool" -> result.put("type", "boolean");
            case "int32" -> result.put("type", "integer").put("format", "int32");
            case "int64" -> result.put("type", "integer").put("format", "int64");
            case "float32" -> result.put("type", "number").put("format", "float");
            case "float64" -> result.put("type", "number").put("format", "double");
            case "decimal" -> result.put("type", "number");
            case "bytes" -> result.put("type", "string").put("contentEncoding", "base64");
            case "uuid" -> result.put("type", "string").put("format", "uuid");
            case "timestamp", "datetime" -> result.put("type", "string").put("format", "date-time");
            case "date" -> result.put("type", "string").put("format", "date");
            case "duration" -> result.put("type", "string").put("format", "duration");
            case "uri" -> result.put("type", "string").put("format", "uri");
            case "path", "currency", "payload_ref", "string" -> result.put("type", "string");
            default -> throw new IllegalArgumentException("unsupported canonical scalar for HTTP mapping: " + name);
        }
        return result;
    }
}
