package org.pipelineframework.connector.graphql;

import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

final class GraphQlJsonObjects {
    static final int MAX_JSON_LENGTH = 1_048_576;
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    private static final TypeReference<Map<String, Object>> OBJECT = new TypeReference<>() { };

    private GraphQlJsonObjects() {
    }

    static String normalize(String value, String label) {
        String json = Objects.requireNonNull(value, label + " must not be null").trim();
        if (json.length() > MAX_JSON_LENGTH) {
            throw new IllegalArgumentException(label + " must not exceed " + MAX_JSON_LENGTH + " characters");
        }
        try {
            return MAPPER.writeValueAsString(MAPPER.readValue(json, OBJECT));
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException(label + " must be a JSON object", exception);
        }
    }

}
