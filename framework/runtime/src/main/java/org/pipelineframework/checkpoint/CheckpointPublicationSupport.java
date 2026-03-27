package org.pipelineframework.checkpoint;

import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.jboss.logging.Logger;

/**
 * Helpers for checkpoint publication metadata and idempotency-key derivation.
 */
public final class CheckpointPublicationSupport {

    private static final Logger LOG = Logger.getLogger(CheckpointPublicationSupport.class);

    private CheckpointPublicationSupport() {
    }

    public static String deriveIdempotencyKey(String fallbackKey, List<String> keyFields, Object payload) {
        if (fallbackKey != null && !fallbackKey.isBlank()) {
            return fallbackKey.trim();
        }
        if (payload == null || keyFields == null || keyFields.isEmpty()) {
            return null;
        }
        List<String> parts = new ArrayList<>(keyFields.size() + 1);
        for (String field : keyFields) {
            String value = readProperty(payload, field);
            if (value == null || value.isBlank()) {
                return null;
            }
            parts.add(value.trim());
        }
        return String.join(":", parts);
    }

    private static String readProperty(Object payload, String field) {
        if (field == null || field.isBlank()) {
            return null;
        }
        Class<?> type = payload.getClass();
        if (type.isRecord()) {
            for (RecordComponent component : type.getRecordComponents()) {
                if (component.getName().equals(field)) {
                    try {
                        Object value = component.getAccessor().invoke(payload);
                        return value == null ? null : value.toString();
                    } catch (ReflectiveOperationException e) {
                        LOG.tracef(e, "Failed reading checkpoint idempotency field '%s' from record payload %s",
                            field, type.getName());
                        return null;
                    }
                }
            }
        }
        String capitalized = field.substring(0, 1).toUpperCase(Locale.ROOT) + field.substring(1);
        for (String candidate : List.of(field, "get" + capitalized, "is" + capitalized)) {
            try {
                Method method = type.getMethod(candidate);
                Object value = method.invoke(payload);
                return value == null ? null : value.toString();
            } catch (ReflectiveOperationException e) {
                LOG.tracef(e, "Failed reading checkpoint idempotency field '%s' via accessor '%s' on %s",
                    field, candidate, type.getName());
                // Try next accessor name.
            }
        }
        return null;
    }
}
