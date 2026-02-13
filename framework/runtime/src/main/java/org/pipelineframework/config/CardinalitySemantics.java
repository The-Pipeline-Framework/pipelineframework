/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.config;

import java.util.Locale;

/**
 * Canonical cardinality semantics shared by runtime and deployment code paths.
 */
public enum CardinalitySemantics {
    ONE_TO_ONE,
    ONE_TO_MANY,
    MANY_TO_ONE,
    MANY_TO_MANY;

    private static final String EXPANSION = "EXPANSION";
    private static final String REDUCTION = "REDUCTION";

    /**
     * Parses aliases (e.g. EXPANSION/REDUCTION) into canonical enum values.
     *
     * @param cardinality input cardinality value
     * @return canonical enum value, or {@code null} when input is {@code null}
     * @throws IllegalArgumentException if cardinality is blank or not recognized
     */
    public static CardinalitySemantics fromString(String cardinality) {
        if (cardinality == null) {
            return null;
        }
        String normalized = cardinality.strip().toUpperCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("Cardinality must not be blank");
        }
        return switch (normalized) {
            case EXPANSION -> ONE_TO_MANY;
            case REDUCTION -> MANY_TO_ONE;
            case "ONE_TO_ONE" -> ONE_TO_ONE;
            case "ONE_TO_MANY" -> ONE_TO_MANY;
            case "MANY_TO_ONE" -> MANY_TO_ONE;
            case "MANY_TO_MANY" -> MANY_TO_MANY;
            default -> throw new IllegalArgumentException("Unsupported cardinality: " + cardinality);
        };
    }

    /**
     * Compatibility helper returning canonical enum name.
     *
     * @param cardinality input cardinality value
     * @return canonical enum name, or {@code null} when input is {@code null}
     * @throws IllegalArgumentException if cardinality is blank or not recognized
     */
    public static String canonical(String cardinality) {
        CardinalitySemantics canonicalValue = fromString(cardinality);
        return canonicalValue == null ? null : canonicalValue.name();
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
        CardinalitySemantics canonicalValue = fromString(cardinality);
        return canonicalValue == MANY_TO_ONE || canonicalValue == MANY_TO_MANY;
    }

    /**
     * Applies cardinality semantics to the output streaming state.
     *
     * @param cardinality cardinality string (for example {@code ONE_TO_MANY}, {@code MANY_TO_ONE})
     * @param currentStreaming current output streaming flag before applying this cardinality
     * @return updated output streaming flag after canonicalizing cardinality via {@link #fromString(String)}
     * @throws IllegalArgumentException if cardinality is null, blank, or unsupported
     */
    public static boolean applyToOutputStreaming(String cardinality, boolean currentStreaming) {
        if (cardinality == null) {
            throw new IllegalArgumentException("Cardinality must not be null");
        }
        CardinalitySemantics canonicalValue = fromString(cardinality);
        if (canonicalValue == ONE_TO_MANY || canonicalValue == MANY_TO_MANY) {
            return true;
        }
        if (canonicalValue == MANY_TO_ONE) {
            return false;
        }
        return currentStreaming;
    }
}
