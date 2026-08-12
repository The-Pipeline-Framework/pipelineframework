package org.pipelineframework.connector;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Open, namespaced operation kind used for catalog metadata, not generic execution.
 */
public record ConnectorOperationKind(String value) implements Comparable<ConnectorOperationKind> {
    private static final Pattern PATTERN = Pattern.compile("[a-z][a-z0-9.-]*:[a-z][a-z0-9.-]*");

    public static final ConnectorOperationKind COMMAND = new ConnectorOperationKind("tpf:command");
    public static final ConnectorOperationKind QUERY = new ConnectorOperationKind("tpf:query");
    public static final ConnectorOperationKind AGENT = new ConnectorOperationKind("tpf:agent");

    public ConnectorOperationKind {
        Objects.requireNonNull(value, "operation kind must not be null");
        if (!PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException("operation kind must be a lowercase namespaced value: " + value);
        }
    }

    public static ConnectorOperationKind of(String value) {
        return new ConnectorOperationKind(value);
    }

    @Override
    public int compareTo(ConnectorOperationKind other) {
        return value.compareTo(Objects.requireNonNull(other, "other must not be null").value);
    }
}
