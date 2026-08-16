package org.pipelineframework.connector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Pure static validation of an untyped configuration document against manifest schema metadata.
 */
public final class ConnectorConfigurationValidator {
    private ConnectorConfigurationValidator() {
    }

    public static void validate(
        ConnectorConfigSchemaDescriptor schema,
        ConnectorConfigurationDocument document,
        String subject
    ) {
        Objects.requireNonNull(schema, "configuration schema must not be null");
        Objects.requireNonNull(document, "configuration document must not be null");
        Objects.requireNonNull(subject, "configuration subject must not be null");
        Set<String> declared = new HashSet<>();
        for (ConnectorConfigFieldDescriptor field : schema.fields()) {
            if (!declared.add(field.name())) {
                throw failure(subject, schema, field.name(), "duplicate schema field");
            }
            Object value = document.values().get(field.name());
            if (value == null) {
                if (field.required()) {
                    throw failure(subject, schema, field.name(), "missing required value");
                }
                continue;
            }
            validateValue(value, field, subject, schema);
        }
        document.values().keySet().stream()
            .filter(name -> !declared.contains(name))
            .sorted()
            .findFirst()
            .ifPresent(name -> {
                throw failure(subject, schema, name, "unknown configuration field");
            });
    }

    private static void validateValue(
        Object value,
        ConnectorConfigFieldDescriptor field,
        String subject,
        ConnectorConfigSchemaDescriptor schema
    ) {
        boolean valid = switch (field.type()) {
            case STRING, CONNECTION_REF, SECRET_REF -> value instanceof String;
            case BOOLEAN -> value instanceof Boolean;
            case INTEGER -> value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;
            case DECIMAL -> value instanceof Number;
            case ENUM -> value instanceof String string && field.enumValues().contains(string);
            case DURATION -> validDuration(value);
        };
        if (!valid) {
            throw failure(subject, schema, field.name(), "expected " + field.type() + " value");
        }
    }

    private static boolean validDuration(Object value) {
        if (!(value instanceof String text)) {
            return false;
        }
        try {
            Duration parsed = Duration.parse(text);
            return parsed != null;
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    private static ConnectorConfigurationException failure(
        String subject,
        ConnectorConfigSchemaDescriptor schema,
        String field,
        String reason
    ) {
        return new ConnectorConfigurationException(
            subject + " configuration schema " + schema.id() + " v" + schema.version()
                + " field '" + field + "': " + reason);
    }
}
