package org.pipelineframework.stdio.demo.common.domain;

import java.util.List;

public record GreetingResponse(List<String> greetings) {
    public GreetingResponse {
        greetings = greetings == null ? List.of() : List.copyOf(greetings);
    }
}
