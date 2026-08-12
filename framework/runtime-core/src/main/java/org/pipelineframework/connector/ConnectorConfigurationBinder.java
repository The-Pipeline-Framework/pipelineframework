package org.pipelineframework.connector;

import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Strict host-neutral binder from a configuration document into a declared immutable record.
 */
public final class ConnectorConfigurationBinder {
    private ConnectorConfigurationBinder() {
    }

    public static <T> T bind(
        ConnectorConfigSchema<T> schema,
        ConnectorConfigurationDocument document,
        String subject
    ) {
        Objects.requireNonNull(schema, "configuration schema must not be null");
        Objects.requireNonNull(document, "configuration document must not be null");
        Objects.requireNonNull(subject, "configuration subject must not be null");
        RecordComponent[] components = schema.configType().getRecordComponents();
        Set<String> declared = new HashSet<>();
        Object[] arguments = new Object[components.length];
        for (int index = 0; index < components.length; index++) {
            RecordComponent component = components[index];
            declared.add(component.getName());
            Object value = document.values().get(component.getName());
            if (value == null) {
                if (component.getType() == Optional.class) {
                    arguments[index] = Optional.empty();
                    continue;
                }
                throw failure(subject, schema, component.getName(), "missing required value");
            }
            arguments[index] = convert(value, component.getGenericType(), subject, schema, component.getName());
        }
        document.values().keySet().stream()
            .filter(name -> !declared.contains(name))
            .sorted()
            .findFirst()
            .ifPresent(name -> {
                throw failure(subject, schema, name, "unknown configuration field");
            });
        try {
            Class<?>[] parameterTypes = Arrays.stream(components).map(RecordComponent::getType).toArray(Class<?>[]::new);
            Constructor<T> constructor = schema.configType().getDeclaredConstructor(parameterTypes);
            return constructor.newInstance(arguments);
        } catch (ReflectiveOperationException exception) {
            throw new ConnectorConfigurationException(
                subject + " configuration schema " + schema.descriptor().id() + " v" + schema.descriptor().version()
                    + " could not construct " + schema.configType().getName() + ": " + exception.getMessage());
        }
    }

    static ConnectorConfigValueType valueType(RecordComponent component) {
        Class<?> type = component.getType();
        if (type == Optional.class) {
            type = optionalType(component.getGenericType(), component.getName());
        }
        if (type == String.class) {
            return ConnectorConfigValueType.STRING;
        }
        if (type == boolean.class || type == Boolean.class) {
            return ConnectorConfigValueType.BOOLEAN;
        }
        if (type == byte.class || type == Byte.class || type == short.class || type == Short.class
            || type == int.class || type == Integer.class || type == long.class || type == Long.class) {
            return ConnectorConfigValueType.INTEGER;
        }
        if (type == float.class || type == Float.class || type == double.class || type == Double.class || type == BigDecimal.class) {
            return ConnectorConfigValueType.DECIMAL;
        }
        if (type == Duration.class) {
            return ConnectorConfigValueType.DURATION;
        }
        if (type == ConnectionRef.class) {
            return ConnectorConfigValueType.CONNECTION_REF;
        }
        if (type == SecretRef.class) {
            return ConnectorConfigValueType.SECRET_REF;
        }
        if (type.isEnum()) {
            return ConnectorConfigValueType.ENUM;
        }
        throw new IllegalArgumentException("unsupported connector configuration component '" + component.getName() + "': " + type.getName());
    }

    private static Object convert(
        Object value,
        Type target,
        String subject,
        ConnectorConfigSchema<?> schema,
        String field
    ) {
        if (target instanceof ParameterizedType parameterized && parameterized.getRawType() == Optional.class) {
            return Optional.of(convert(value, parameterized.getActualTypeArguments()[0], subject, schema, field));
        }
        if (!(target instanceof Class<?> type)) {
            throw failure(subject, schema, field, "unsupported target type " + target.getTypeName());
        }
        if (type.isInstance(value)) {
            return value;
        }
        if (type == String.class && value instanceof String string) {
            return string;
        }
        if ((type == boolean.class || type == Boolean.class) && value instanceof Boolean bool) {
            return bool;
        }
        if ((type == int.class || type == Integer.class) && value instanceof Number number) {
            return number.intValue();
        }
        if ((type == byte.class || type == Byte.class) && value instanceof Number number) {
            return number.byteValue();
        }
        if ((type == short.class || type == Short.class) && value instanceof Number number) {
            return number.shortValue();
        }
        if ((type == long.class || type == Long.class) && value instanceof Number number) {
            return number.longValue();
        }
        if ((type == double.class || type == Double.class) && value instanceof Number number) {
            return number.doubleValue();
        }
        if ((type == float.class || type == Float.class) && value instanceof Number number) {
            return number.floatValue();
        }
        if (type == BigDecimal.class && value instanceof Number number) {
            return new BigDecimal(number.toString());
        }
        if (type == Duration.class && value instanceof String duration) {
            try {
                return Duration.parse(duration);
            } catch (RuntimeException exception) {
                throw failure(subject, schema, field, "expected ISO-8601 duration but received '" + duration + "'");
            }
        }
        if (type == ConnectionRef.class && value instanceof String reference) {
            return new ConnectionRef(reference);
        }
        if (type == SecretRef.class && value instanceof String reference) {
            return new SecretRef(reference);
        }
        if (type.isEnum() && value instanceof String name) {
            try {
                @SuppressWarnings({ "rawtypes", "unchecked" })
                Class<? extends Enum> enumType = (Class<? extends Enum>) type;
                return Enum.valueOf(enumType, name);
            } catch (IllegalArgumentException exception) {
                throw failure(subject, schema, field, "expected one of " + Arrays.toString(type.getEnumConstants()));
            }
        }
        throw failure(subject, schema, field, "expected " + type.getSimpleName() + " but received " + value.getClass().getSimpleName());
    }

    private static Class<?> optionalType(Type genericType, String field) {
        if (genericType instanceof ParameterizedType parameterized && parameterized.getActualTypeArguments()[0] instanceof Class<?> type) {
            return type;
        }
        throw new IllegalArgumentException("optional connector configuration field must have a supported concrete type: " + field);
    }

    private static ConnectorConfigurationException failure(
        String subject,
        ConnectorConfigSchema<?> schema,
        String field,
        String reason
    ) {
        return new ConnectorConfigurationException(
            subject + " configuration schema " + schema.descriptor().id() + " v" + schema.descriptor().version()
                + " field '" + field + "': " + reason);
    }
}
