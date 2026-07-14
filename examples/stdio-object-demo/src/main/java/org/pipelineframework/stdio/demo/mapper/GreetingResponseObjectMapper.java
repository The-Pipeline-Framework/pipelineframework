package org.pipelineframework.stdio.demo.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        try {
            return new ObjectPayload(new ObjectMapper().writeValueAsBytes(Map.of("greetings", response.greetings())), "application/json", Map.of("records", "1"));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize greeting response", e);
        }
    }
}
