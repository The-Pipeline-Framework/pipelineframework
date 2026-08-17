package org.pipelineframework.connector;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Sanitized configuration metadata suitable for future durable metadata; resolved values are excluded.
 */
public record ConnectorConfigurationSnapshot(
    String schemaId,
    int schemaVersion,
    String digest,
    List<ConnectionRef> connectionReferences
) {
    public ConnectorConfigurationSnapshot {
        schemaId = ConnectorProviderId.require(schemaId, "configuration schema ID");
        if (schemaVersion < 1) {
            throw new IllegalArgumentException("configuration schema version must be positive");
        }
        digest = Objects.requireNonNull(digest, "configuration digest must not be null");
        connectionReferences = List.copyOf(Objects.requireNonNull(connectionReferences, "connection references must not be null"));
    }

    public static ConnectorConfigurationSnapshot from(
        ConnectorConfigSchema<?> schema,
        ConnectorConfigurationDocument document,
        boolean includeConnectionReferences
    ) {
        Objects.requireNonNull(schema, "configuration schema must not be null");
        return from(schema.descriptor(), document, includeConnectionReferences);
    }

    public static ConnectorConfigurationSnapshot from(
        ConnectorConfigSchemaDescriptor schema,
        ConnectorConfigurationDocument document,
        boolean includeConnectionReferences
    ) {
        Objects.requireNonNull(schema, "configuration schema descriptor must not be null");
        Objects.requireNonNull(document, "configuration document must not be null");
        List<String> entries = new ArrayList<>();
        List<ConnectionRef> connections = new ArrayList<>();
        for (ConnectorConfigFieldDescriptor field : schema.fields()) {
            Object value = document.values().get(field.name());
            if (value == null) {
                entries.add(field.name() + "=<absent>");
            } else if (field.type() == ConnectorConfigValueType.SECRET_REF) {
                entries.add(field.name() + "=<secret-ref>");
            } else {
                entries.add(field.name() + "=" + value);
                if (includeConnectionReferences && field.type() == ConnectorConfigValueType.CONNECTION_REF) {
                    connections.add(value instanceof ConnectionRef reference ? reference : new ConnectionRef((String) value));
                }
            }
        }
        entries.sort(Comparator.naturalOrder());
        return new ConnectorConfigurationSnapshot(
            schema.id(), schema.version(), sha256(String.join("\n", entries)), connections);
    }

    private static String sha256(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(bytes.length * 2);
            for (byte valueByte : bytes) {
                hex.append(String.format("%02x", valueByte));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }
}
