package org.pipelineframework.connector;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Static schema metadata for one record component.
 */
public record ConnectorConfigFieldDescriptor(
    String name,
    ConnectorConfigValueType type,
    boolean required,
    List<String> enumValues
) {
    private static final Pattern NAME = Pattern.compile("[A-Za-z][A-Za-z0-9]*");

    public ConnectorConfigFieldDescriptor {
        name = requireName(name);
        type = Objects.requireNonNull(type, "configuration field type must not be null");
        enumValues = List.copyOf(Objects.requireNonNull(enumValues, "enum values must not be null"));
        if (type != ConnectorConfigValueType.ENUM && !enumValues.isEmpty()) {
            throw new IllegalArgumentException("only enum configuration fields may declare enum values");
        }
    }

    public ConnectorConfigFieldDescriptor(String name, ConnectorConfigValueType type, boolean required) {
        this(name, type, required, List.of());
    }

    static String requireName(String value) {
        Objects.requireNonNull(value, "configuration field name must not be null");
        if (!NAME.matcher(value).matches()) {
            throw new IllegalArgumentException("configuration field name must be a Java-style identifier: " + value);
        }
        return value;
    }
}
