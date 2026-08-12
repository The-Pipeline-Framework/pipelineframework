package org.pipelineframework.connector;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Derived schema and target type for one immutable Java record configuration.
 */
public final class ConnectorConfigSchema<T> {
    private final Class<T> configType;
    private final ConnectorConfigSchemaDescriptor descriptor;

    private ConnectorConfigSchema(Class<T> configType, ConnectorConfigSchemaDescriptor descriptor) {
        this.configType = configType;
        this.descriptor = descriptor;
    }

    public static <T> ConnectorConfigSchema<T> record(Class<T> configType, String id, int version) {
        Objects.requireNonNull(configType, "configuration type must not be null");
        if (!configType.isRecord()) {
            throw new IllegalArgumentException("connector configuration type must be an immutable Java record: " + configType.getName());
        }
        List<ConnectorConfigFieldDescriptor> fields = Arrays.stream(configType.getRecordComponents())
            .map(ConnectorConfigSchema::field)
            .toList();
        return new ConnectorConfigSchema<>(configType, new ConnectorConfigSchemaDescriptor(id, version, fields));
    }

    public Class<T> configType() {
        return configType;
    }

    public ConnectorConfigSchemaDescriptor descriptor() {
        return descriptor;
    }

    private static ConnectorConfigFieldDescriptor field(RecordComponent component) {
        Class<?> type = component.getType();
        boolean required = type != Optional.class;
        if (type.isEnum()) {
            return new ConnectorConfigFieldDescriptor(
                component.getName(),
                ConnectorConfigValueType.ENUM,
                required,
                Arrays.stream(type.getEnumConstants()).map(constant -> ((Enum<?>) constant).name()).toList());
        }
        return new ConnectorConfigFieldDescriptor(component.getName(), ConnectorConfigurationBinder.valueType(component), required);
    }
}
