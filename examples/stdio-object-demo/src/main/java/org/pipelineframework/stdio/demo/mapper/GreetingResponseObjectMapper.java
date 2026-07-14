package org.pipelineframework.stdio.demo.mapper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.pipelineframework.objectpublish.ObjectPayload;
import org.pipelineframework.objectpublish.ObjectPublishMapper;
import org.pipelineframework.stdio.demo.common.domain.GreetingResponse;

/** Renders terminal typed output as a single JSON object for stdout. */
public final class GreetingResponseObjectMapper implements ObjectPublishMapper<GreetingResponse> {
    @Override
    public String groupKey(GreetingResponse item) {
        return "stdout";
    }

    @Override
    public ObjectPayload render(String groupKey, List<GreetingResponse> items) {
        GreetingResponse response = items.getFirst();
        String greetings = response.greetings().stream()
            .map(value -> "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"")
            .collect(java.util.stream.Collectors.joining(","));
        String json = "{\"greetings\":[" + greetings + "]}";
        return new ObjectPayload(json.getBytes(StandardCharsets.UTF_8), "application/json", Map.of("records", "1"));
    }
}
