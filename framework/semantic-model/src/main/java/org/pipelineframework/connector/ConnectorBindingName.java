package org.pipelineframework.connector;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Pipeline-local address of one configured connector provider instance.
 */
public record ConnectorBindingName(String value) implements Comparable<ConnectorBindingName> {
    private static final Pattern VALID = Pattern.compile("[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*");

    public ConnectorBindingName {
        Objects.requireNonNull(value, "connector binding name must not be null");
        value = value.trim();
        if (!VALID.matcher(value).matches()) {
            throw new IllegalArgumentException(
                "connector binding name must be lowercase dotted or hyphenated text: " + value);
        }
    }

    public static ConnectorBindingName of(String value) {
        return new ConnectorBindingName(value);
    }

    @Override
    public int compareTo(ConnectorBindingName other) {
        return value.compareTo(Objects.requireNonNull(other, "other binding name must not be null").value);
    }

    @Override
    public String toString() {
        return value;
    }
}
