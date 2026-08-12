package org.pipelineframework.connector;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Stable provider identity. Provider IDs are lowercase dotted names.
 */
public record ConnectorProviderId(String value) implements Comparable<ConnectorProviderId> {
    private static final Pattern PATTERN = Pattern.compile("[a-z][a-z0-9]*(?:\\.[a-z][a-z0-9]*)*");

    public ConnectorProviderId {
        value = require(value, "provider ID");
    }

    public static ConnectorProviderId of(String value) {
        return new ConnectorProviderId(value);
    }

    /**
     * Whether this ID belongs to the namespace reserved for framework-provided adapters.
     */
    public boolean isFrameworkReserved() {
        return value.equals("tpf") || value.startsWith("tpf.");
    }

    static String require(String value, String label) {
        Objects.requireNonNull(value, label + " must not be null");
        if (!PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(label + " must be a lowercase dotted name: " + value);
        }
        return value;
    }

    @Override
    public int compareTo(ConnectorProviderId other) {
        return value.compareTo(Objects.requireNonNull(other, "other must not be null").value);
    }
}
