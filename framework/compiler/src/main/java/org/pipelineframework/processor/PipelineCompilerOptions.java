package org.pipelineframework.processor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Immutable host-neutral snapshot of compiler option values. */
public final class PipelineCompilerOptions {

    private final Map<String, String> values;

    public PipelineCompilerOptions(Map<String, String> values) {
        Objects.requireNonNull(values, "values must not be null");
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    public Optional<String> value(String name) {
        return Optional.ofNullable(values.get(Objects.requireNonNull(name, "name must not be null")));
    }

    public Map<String, String> asMap() {
        return values;
    }
}
