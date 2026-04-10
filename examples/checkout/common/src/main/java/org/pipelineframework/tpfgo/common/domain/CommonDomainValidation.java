package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.Locale;

public final class CommonDomainValidation {

    private CommonDomainValidation() {
    }

    static String requireNonBlank(String value, String fieldName) {
        if (value == null) {
            throw new NullPointerException(fieldName + " must not be null");
        }
        String normalized = value.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }

    static BigDecimal requireNonNegative(BigDecimal value, String fieldName) {
        if (value == null) {
            throw new NullPointerException(fieldName + " must not be null");
        }
        if (value.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException(fieldName + " must be non-negative");
        }
        return value;
    }

    static Integer requireNonNegative(Integer value, String fieldName) {
        if (value == null) {
            throw new NullPointerException(fieldName + " must not be null");
        }
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must be non-negative");
        }
        return value;
    }

    static String requireCurrencyCode(String value, String fieldName) {
        String normalized = requireNonBlank(value, fieldName).toUpperCase(Locale.ROOT);
        try {
            Currency.getInstance(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(fieldName + " must be a valid ISO-4217 currency code", e);
        }
        return normalized;
    }

    public static String blankToNull(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }
}
