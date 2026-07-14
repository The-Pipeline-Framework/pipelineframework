package org.pipelineframework.stdio.demo.common.domain;

import java.util.List;

public record NormalizedGreetings(List<String> names) {
    public NormalizedGreetings {
        names = names == null ? List.of() : List.copyOf(names);
    }
}
