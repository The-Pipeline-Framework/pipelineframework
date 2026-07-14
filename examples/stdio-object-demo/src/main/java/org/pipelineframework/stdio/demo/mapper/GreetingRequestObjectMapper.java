package org.pipelineframework.stdio.demo.mapper;

import java.util.List;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.pipelineframework.objectingest.ObjectSnapshot;
import org.pipelineframework.objectingest.ObjectSnapshotMapper;
import org.pipelineframework.stdio.demo.common.domain.GreetingRequests;

/** Maps one EOF-delimited JSON value into the typed pipeline input. */
public final class GreetingRequestObjectMapper implements ObjectSnapshotMapper<GreetingRequests> {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Override
    public GreetingRequests map(ObjectSnapshot snapshot) {
        String content = snapshot.textContent();
        if (content == null || content.isBlank()) {
            throw new IllegalArgumentException("stdin JSON must not be blank");
        }
        JsonNode root;
        try {
            root = JSON.readTree(content);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("stdin must contain valid JSON", e);
        }
        List<JsonNode> values = root.isObject() ? List.of(root)
            : root.isArray() ? java.util.stream.StreamSupport.stream(root.spliterator(), false).toList()
            : List.of();
        if (values.isEmpty() || values.stream().anyMatch(value -> !value.hasNonNull("name") || !value.path("name").isTextual())) {
            throw new IllegalArgumentException("stdin must contain one JSON object or JSON collection with name fields");
        }
        List<String> names = values.stream().map(value -> value.path("name").textValue()).toList();
        if (names.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException("stdin JSON names must not be blank");
        }
        if (names.isEmpty()) {
            throw new IllegalArgumentException("stdin JSON must contain at least one name field");
        }
        return new GreetingRequests(names);
    }
}
