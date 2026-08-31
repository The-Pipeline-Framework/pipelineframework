package org.pipelineframework.connector;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Host-supplied untyped configuration input. Provider code never receives this document.
 */
public record ConnectorConfigurationDocument(Map<String, Object> values) {
    public ConnectorConfigurationDocument {
        values = copy(values);
    }

    public static ConnectorConfigurationDocument empty() {
        return new ConnectorConfigurationDocument(Map.of());
    }

    private static Map<String, Object> copy(Map<String, Object> source) {
        Objects.requireNonNull(source, "configuration values must not be null");
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = ConnectorConfigFieldDescriptor.requireName(entry.getKey());
            copy.put(name, Objects.requireNonNull(entry.getValue(), "configuration value must not be null for " + name));
        }
        return Collections.unmodifiableMap(copy);
    }
}
