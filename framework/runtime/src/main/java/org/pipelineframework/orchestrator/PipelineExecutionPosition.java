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

package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Durable continuation location within one pinned pipeline release.
 *
 * <p>{@code rootStepIndex} remains the physical cursor used to enter the root runtime step list.
 * {@code staticLocation} is compiler-derived and identifies an inner static location when the
 * cursor is an invocation step. It is intentionally not a runtime invocation-instance stack;
 * recursive invocation identity remains a later concern.
 *
 * @param rootStepIndex root pipeline cursor
 * @param staticLocation current compiler-derived nested location, or empty for a root step
 * @param nextStaticLocation compiler-derived continuation location after the current nested step
 */
public record PipelineExecutionPosition(
    int rootStepIndex,
    String staticLocation,
    String nextStaticLocation
) {

    public PipelineExecutionPosition {
        if (rootStepIndex < 0) {
            throw new IllegalArgumentException("rootStepIndex must not be negative");
        }
        staticLocation = normalize(staticLocation);
        nextStaticLocation = normalize(nextStaticLocation);
        if (staticLocation.isEmpty() && !nextStaticLocation.isEmpty()) {
            throw new IllegalArgumentException("Root execution positions cannot declare nextStaticLocation");
        }
    }

    public static PipelineExecutionPosition root(int rootStepIndex) {
        return new PipelineExecutionPosition(rootStepIndex, "", "");
    }

    public static PipelineExecutionPosition nested(
        int rootStepIndex,
        String staticLocation,
        String nextStaticLocation
    ) {
        return new PipelineExecutionPosition(rootStepIndex, staticLocation, nextStaticLocation);
    }

    public boolean nested() {
        return !staticLocation.isEmpty();
    }

    /**
     * Returns the position to use after completing the represented await boundary.
     *
     * @return next nested position, or the next root cursor for a root boundary
     */
    public PipelineExecutionPosition next() {
        return nested()
            ? new PipelineExecutionPosition(rootStepIndex, nextStaticLocation, "")
            : root(rootStepIndex + 1);
    }

    /**
     * Stable component for transition idempotency within a pinned release.
     *
     * @return canonical transition-location component
     */
    public String transitionLocation() {
        return nested() ? staticLocation : Integer.toString(rootStepIndex);
    }

    /** Stable storage form, intentionally independent from a particular state-store schema. */
    public String encode() {
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        return rootStepIndex + "|" + encoder.encodeToString(staticLocation.getBytes(StandardCharsets.UTF_8))
            + "|" + encoder.encodeToString(nextStaticLocation.getBytes(StandardCharsets.UTF_8));
    }

    /** Reads a stored position, retaining the flat cursor for pre-migration records. */
    public static PipelineExecutionPosition decode(String encoded, int fallbackRootStepIndex) {
        if (encoded == null || encoded.isBlank()) {
            return root(fallbackRootStepIndex);
        }
        String[] parts = encoded.split("\\|", -1);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Malformed pipeline execution position");
        }
        try {
            Base64.Decoder decoder = Base64.getUrlDecoder();
            return new PipelineExecutionPosition(
                Integer.parseInt(parts[0]),
                new String(decoder.decode(parts[1]), StandardCharsets.UTF_8),
                new String(decoder.decode(parts[2]), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("Malformed pipeline execution position", failure);
        }
    }

    private static String normalize(String value) {
        return Objects.requireNonNull(value, "execution position location must not be null").strip();
    }
}
