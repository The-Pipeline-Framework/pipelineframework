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

import java.util.List;
import java.util.Locale;
import java.util.Objects;

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

    /**
     * Returns the reactive invocation shape represented by this cardinality.
     *
     * <p>The two output values intentionally model unary and streaming input separately. A
     * terminal streaming flag is insufficient because {@link #ONE_TO_MANY} and
     * {@link #MANY_TO_MANY} both produce streaming output while only the latter must receive the
     * complete upstream stream in one invocation.
     *
     * @return the invocation shape used when composing ordered pipeline steps
     */
    public InvocationShape invocationShape() {
        return switch (this) {
            case ONE_TO_ONE -> new InvocationShape(ReactiveShape.ONE, ReactiveShape.MANY, false);
            case ONE_TO_MANY -> new InvocationShape(ReactiveShape.MANY, ReactiveShape.MANY, false);
            case MANY_TO_ONE -> new InvocationShape(ReactiveShape.ONE, ReactiveShape.ONE, true);
            case MANY_TO_MANY -> new InvocationShape(ReactiveShape.MANY, ReactiveShape.MANY, true);
        };
    }

    /**
     * Folds a non-empty ordered sequence of step cardinalities into the cardinality of their
     * pipeline invocation.
     *
     * @param cardinalities ordered child-step cardinalities
     * @return the unique existing cardinality represented by the composed invocation shape
     */
    public static CardinalitySemantics compose(List<CardinalitySemantics> cardinalities) {
        Objects.requireNonNull(cardinalities, "cardinalities must not be null");
        if (cardinalities.isEmpty()) {
            throw new IllegalArgumentException("Pipeline cardinality requires at least one step");
        }
        InvocationShape composed = null;
        for (CardinalitySemantics cardinality : cardinalities) {
            CardinalitySemantics current = Objects.requireNonNull(
                cardinality,
                "cardinalities must not contain null");
            composed = composed == null
                ? current.invocationShape()
                : composed.then(current.invocationShape());
        }
        return composed.toCardinality();
    }

    /**
     * Reactive multiplicity of a pipeline invocation boundary.
     */
    public enum ReactiveShape {
        ONE,
        MANY
    }

    /**
     * Composable reactive shape for a cardinality.
     *
     * @param unaryInputOutput output shape for a unary input
     * @param streamingInputOutput output shape for a streaming input
     * @param streamScoped whether the step must be invoked over the complete upstream stream
     */
    public record InvocationShape(
        ReactiveShape unaryInputOutput,
        ReactiveShape streamingInputOutput,
        boolean streamScoped
    ) {
        public InvocationShape {
            Objects.requireNonNull(unaryInputOutput, "unaryInputOutput must not be null");
            Objects.requireNonNull(streamingInputOutput, "streamingInputOutput must not be null");
        }

        /**
         * Composes this shape with the following ordered step shape.
         *
         * @param following shape of the next step
         * @return composed invocation shape
         */
        public InvocationShape then(InvocationShape following) {
            Objects.requireNonNull(following, "following must not be null");
            return new InvocationShape(
                following.outputFor(unaryInputOutput),
                following.outputFor(streamingInputOutput),
                streamScoped || following.streamScoped);
        }

        private ReactiveShape outputFor(ReactiveShape input) {
            return input == ReactiveShape.ONE ? unaryInputOutput : streamingInputOutput;
        }

        private CardinalitySemantics toCardinality() {
            if (!streamScoped && unaryInputOutput == ReactiveShape.ONE
                && streamingInputOutput == ReactiveShape.MANY) {
                return ONE_TO_ONE;
            }
            if (!streamScoped && unaryInputOutput == ReactiveShape.MANY
                && streamingInputOutput == ReactiveShape.MANY) {
                return ONE_TO_MANY;
            }
            if (streamScoped && unaryInputOutput == ReactiveShape.ONE
                && streamingInputOutput == ReactiveShape.ONE) {
                return MANY_TO_ONE;
            }
            if (streamScoped && unaryInputOutput == ReactiveShape.MANY
                && streamingInputOutput == ReactiveShape.MANY) {
                return MANY_TO_MANY;
            }
            throw new IllegalStateException("Unsupported composed pipeline invocation shape: " + this);
        }
    }
}
