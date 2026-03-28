package org.pipelineframework.tpfgo.common.util;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;

public final class GrpcMappingSupport {

    private GrpcMappingSupport() {
    }

    public static UUID uuid(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid UUID for " + fieldName + ": " + value, e);
        }
    }

    public static String str(UUID value) {
        return value == null ? "" : value.toString();
    }

    public static Instant instant(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid Instant for " + fieldName + ": " + value, e);
        }
    }

    public static String str(Instant value) {
        return value == null ? "" : value.toString();
    }

    public static BigDecimal decimal(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid decimal for " + fieldName + ": " + value, e);
        }
    }

    public static String str(BigDecimal value) {
        return value == null ? "" : value.toPlainString();
    }
}
