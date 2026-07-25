package org.pipelineframework.util;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Utility for deserializing JSON input into Uni or Multi streams.
 */
@ApplicationScoped
public class PipelineInputDeserializer {

    /**
     * Creates a new PipelineInputDeserializer.
     */
    public PipelineInputDeserializer() {
    }

    @Inject
    ObjectMapper objectMapper;

    /**
     * Deserialize a JSON payload into a Uni of the requested type.
     *
     * @param json the JSON payload
     * @param type the target type
     * @param <T> the target type
     * @return a Uni that emits the deserialized value
     * @throws IOException if deserialization fails
     */
    public <T> Uni<T> uniFromJson(String json, Class<T> type) throws IOException {
        if (json == null || json.trim().isEmpty()) {
            throw new IllegalArgumentException("JSON input is required.");
        }
        T value = objectMapper.readValue(json, type);
        return Uni.createFrom().item(value);
    }

    /**
     * Deserialize a JSON array into a Multi of the requested type.
     *
     * @param json the JSON array payload
     * @param type the element type
     * @param <T> the element type
     * @return a Multi that emits each element
     * @throws IOException if deserialization fails
     */
    public <T> Multi<T> multiFromJsonList(String json, Class<T> type) throws IOException {
        if (json == null || json.trim().isEmpty()) {
            throw new IllegalArgumentException("JSON input list is required.");
        }
        List<T> values = objectMapper.readValue(
            json,
            objectMapper.getTypeFactory().constructCollectionType(List.class, type));
        return Multi.createFrom().iterable(values);
    }

    public <T extends Message> Uni<T> uniFromProtoJson(
            String json, Class<T> type) throws IOException {
        return Uni.createFrom().item(fromProtoJson(json, type));
    }

    public <T extends Message> Multi<T> multiFromProtoJsonList(
            String json, Class<T> type) throws IOException {
        if (json == null || json.trim().isEmpty()) {
            throw new IllegalArgumentException("JSON input list is required.");
        }
        JsonNode valuesNode = objectMapper.readTree(json);
        if (!valuesNode.isArray()) {
            throw new IllegalArgumentException("JSON input list must be an array.");
        }
        List<T> values = new ArrayList<>();
        for (JsonNode valueNode : valuesNode) {
            values.add(fromProtoJson(objectMapper.writeValueAsString(valueNode), type));
        }
        return Multi.createFrom().iterable(values);
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> T fromProtoJson(
            String json, Class<T> type) throws IOException {
        if (json == null || json.trim().isEmpty()) {
            throw new IllegalArgumentException("JSON input is required.");
        }
        Message.Builder builder = newBuilder(Objects.requireNonNull(type, "type"));
        JsonFormat.parser().merge(json, builder);
        return type.cast(builder.build());
    }

    private static Message.Builder newBuilder(Class<? extends Message> type) {
        try {
            return (Message.Builder) type.getMethod("newBuilder").invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Protobuf message type must expose newBuilder(): " + type.getName(), e);
        }
    }
}
