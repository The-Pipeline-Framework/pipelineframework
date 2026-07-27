package org.pipelineframework.representation.spi;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Internal normalization for provider-owned opaque configuration. */
final class ImmutableMapSupport {
    private ImmutableMapSupport() {
    }

    static <K, V> Map<K, V> copy(Map<K, V> values) {
        return values == null || values.isEmpty()
            ? Map.of()
            : Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }
}
