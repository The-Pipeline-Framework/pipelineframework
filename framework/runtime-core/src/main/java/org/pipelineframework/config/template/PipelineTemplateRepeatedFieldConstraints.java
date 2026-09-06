/*
 * Copyright (c) 2026 Mariano Barcia
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

package org.pipelineframework.config.template;

import java.beans.Transient;
import java.util.Optional;

/** Target-neutral cardinality constraints for a canonical repeated field. */
public record PipelineTemplateRepeatedFieldConstraints(
    Optional<Integer> minItems,
    Optional<Integer> maxItems
) {
    public PipelineTemplateRepeatedFieldConstraints {
        minItems = minItems == null ? Optional.empty() : minItems;
        maxItems = maxItems == null ? Optional.empty() : maxItems;
        minItems.ifPresent(value -> requireNonNegative("minItems", value));
        maxItems.ifPresent(value -> requireNonNegative("maxItems", value));
        if (minItems.isPresent() && maxItems.isPresent() && minItems.orElseThrow() > maxItems.orElseThrow()) {
            throw new IllegalArgumentException("minItems must not exceed maxItems");
        }
    }

    public PipelineTemplateRepeatedFieldConstraints() {
        this(Optional.empty(), Optional.empty());
    }

    public static PipelineTemplateRepeatedFieldConstraints empty() {
        return new PipelineTemplateRepeatedFieldConstraints();
    }

    @Transient
    public boolean isEmpty() {
        return minItems.isEmpty() && maxItems.isEmpty();
    }

    public PipelineTemplateWrapperConstraints.Compatibility classifyChangeFrom(
        PipelineTemplateRepeatedFieldConstraints baseline
    ) {
        PipelineTemplateRepeatedFieldConstraints before = baseline == null ? empty() : baseline;
        boolean lowerNarrows = compareLower(before.minItems, minItems) > 0;
        boolean lowerWidens = compareLower(before.minItems, minItems) < 0;
        boolean upperNarrows = compareUpper(before.maxItems, maxItems) > 0;
        boolean upperWidens = compareUpper(before.maxItems, maxItems) < 0;
        if ((lowerNarrows || upperNarrows) && (lowerWidens || upperWidens)) {
            return PipelineTemplateWrapperConstraints.Compatibility.INCOMPARABLE;
        }
        if (lowerNarrows || upperNarrows) {
            return PipelineTemplateWrapperConstraints.Compatibility.NARROWING;
        }
        if (lowerWidens || upperWidens) {
            return PipelineTemplateWrapperConstraints.Compatibility.WIDENING;
        }
        return PipelineTemplateWrapperConstraints.Compatibility.UNCHANGED;
    }

    private static void requireNonNegative(String name, int value) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be a non-negative integer");
        }
    }

    private static int compareLower(Optional<Integer> before, Optional<Integer> after) {
        if (before.equals(after)) {
            return 0;
        }
        if (before.isEmpty()) {
            return 1;
        }
        if (after.isEmpty()) {
            return -1;
        }
        return Integer.compare(after.orElseThrow(), before.orElseThrow());
    }

    private static int compareUpper(Optional<Integer> before, Optional<Integer> after) {
        if (before.equals(after)) {
            return 0;
        }
        if (before.isEmpty()) {
            return 1;
        }
        if (after.isEmpty()) {
            return -1;
        }
        return Integer.compare(before.orElseThrow(), after.orElseThrow());
    }
}
