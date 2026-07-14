package org.pipelineframework.stdio.demo.common.domain;

import java.util.List;

public record GreetingRequests(List<String> names) {
    public GreetingRequests {
        names = names == null ? List.of() : List.copyOf(names);
        if (names.isEmpty()) {
            throw new IllegalArgumentException("names must not be empty");
        }
    }
}
