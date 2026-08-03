package org.pipelineframework.representation.spi;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Shared normalization for provider-owned opaque configuration. */
public final class ImmutableMapSupport {
    private ImmutableMapSupport() {
    }

    public static <K, V> Map<K, V> copy(Map<K, V> values) {
        return values == null || values.isEmpty()
            ? Map.of()
            : Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }
}
