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

    /**
     * Determines whether a cardinality implies streaming input.
     *
     * @param cardinality cardinality string (for example {@code MANY_TO_ONE}, {@code MANY_TO_MANY})
     * @return {@code true} when the input side is streaming, otherwise {@code false}
     * @throws IllegalArgumentException if cardinality is null, blank, or unsupported
     */
    public static boolean isStreamingInput(String cardinality) {
        if (cardinality == null) {
            throw new IllegalArgumentException("Cardinality must not be null");
        }
        String canonical = canonical(cardinality);
        return MANY_TO_ONE.equals(canonical) || MANY_TO_MANY.equals(canonical);
    }

    /**
     * Applies cardinality semantics to the output streaming state.
     *
     * @param cardinality cardinality string (for example {@code ONE_TO_MANY}, {@code MANY_TO_ONE})
     * @param currentStreaming current output streaming flag before applying this cardinality
     * @return updated output streaming flag after canonicalizing cardinality via {@link #canonical(String)}
     * @throws IllegalArgumentException if cardinality is null, blank, or unsupported
     */
    public static boolean applyToOutputStreaming(String cardinality, boolean currentStreaming) {
        if (cardinality == null) {
            throw new IllegalArgumentException("Cardinality must not be null");
        }
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
