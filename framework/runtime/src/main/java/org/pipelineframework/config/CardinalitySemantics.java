package org.pipelineframework.config;

import java.util.Locale;

/**
 * Canonical cardinality semantics shared by runtime and deployment code paths.
 */
public final class CardinalitySemantics {
    public static final String ONE_TO_ONE = "ONE_TO_ONE";
    public static final String ONE_TO_MANY = "ONE_TO_MANY";
    public static final String MANY_TO_ONE = "MANY_TO_ONE";
    public static final String MANY_TO_MANY = "MANY_TO_MANY";

    private static final String EXPANSION = "EXPANSION";
    private static final String REDUCTION = "REDUCTION";

    private CardinalitySemantics() {
    }

    /**
     * Converts aliases (e.g. EXPANSION/REDUCTION) into canonical cardinality names.
     *
     * @param cardinality input cardinality value
     * @return canonical cardinality value, or {@code null} if input is {@code null}
     * @throws IllegalArgumentException if cardinality is blank or not recognized
     */
    public static String canonical(String cardinality) {
        if (cardinality == null) {
            return null;
        }
        String normalized = cardinality.trim().toUpperCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("Cardinality must not be blank");
        }
        return switch (normalized) {
            case EXPANSION -> ONE_TO_MANY;
            case REDUCTION -> MANY_TO_ONE;
            case ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE, MANY_TO_MANY -> normalized;
            default -> throw new IllegalArgumentException("Unsupported cardinality: " + cardinality);
        };
    }

    public static boolean isStreamingInput(String cardinality) {
        String canonical = canonical(cardinality);
        return MANY_TO_ONE.equals(canonical) || MANY_TO_MANY.equals(canonical);
    }

    public static boolean applyToOutputStreaming(String cardinality, boolean currentStreaming) {
        String canonical = canonical(cardinality);
        if (ONE_TO_MANY.equals(canonical) || MANY_TO_MANY.equals(canonical)) {
            return true;
        }
        if (MANY_TO_ONE.equals(canonical)) {
            return false;
        }
        return currentStreaming;
    }
}
