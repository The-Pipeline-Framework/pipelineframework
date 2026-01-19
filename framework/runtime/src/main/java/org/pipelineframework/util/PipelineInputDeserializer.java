package org.pipelineframework.util;

import java.io.IOException;
import java.util.List;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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
}
