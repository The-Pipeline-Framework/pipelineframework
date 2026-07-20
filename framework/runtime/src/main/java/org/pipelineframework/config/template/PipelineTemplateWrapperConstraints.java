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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.math.BigDecimal;
import java.util.Optional;

/** Target-neutral constraints declared beside a v3 nominal wrapper's scalar representation. */
public record PipelineTemplateWrapperConstraints(
    Optional<Integer> minLength,
    Optional<Integer> maxLength,
    Optional<String> pattern,
    Optional<Format> format,
    Optional<BigDecimal> minimum,
    Optional<BigDecimal> minimumExclusive,
    Optional<BigDecimal> maximum,
    Optional<BigDecimal> maximumExclusive
) {
    public enum Format { EMAIL }

    public enum Compatibility { UNCHANGED, NARROWING, WIDENING, INCOMPARABLE }

    public PipelineTemplateWrapperConstraints {
        minLength = optional(minLength);
        maxLength = optional(maxLength);
        pattern = optional(pattern);
        format = optional(format);
        minimum = canonical(optional(minimum));
        minimumExclusive = canonical(optional(minimumExclusive));
        maximum = canonical(optional(maximum));
        maximumExclusive = canonical(optional(maximumExclusive));
    }

    public PipelineTemplateWrapperConstraints() {
        this(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty());
    }

    public static PipelineTemplateWrapperConstraints empty() {
        return new PipelineTemplateWrapperConstraints();
    }

    @JsonIgnore
    public boolean isEmpty() {
        return minLength.isEmpty() && maxLength.isEmpty() && pattern.isEmpty() && format.isEmpty()
            && minimum.isEmpty() && minimumExclusive.isEmpty() && maximum.isEmpty() && maximumExclusive.isEmpty();
    }

    /**
     * Classifies this constraint set relative to the supplied baseline. The classification is semantic
     * rather than a protobuf-wire compatibility decision.
     */
    public Compatibility classifyChangeFrom(PipelineTemplateWrapperConstraints baseline) {
        PipelineTemplateWrapperConstraints before = baseline == null ? empty() : baseline;
        Change change = Change.UNCHANGED;
        change = change.combine(compareLower(before.minLength.map(BigDecimal::valueOf), this.minLength.map(BigDecimal::valueOf),
            false, false));
        change = change.combine(compareUpper(before.maxLength.map(BigDecimal::valueOf), this.maxLength.map(BigDecimal::valueOf),
            false, false));
        change = change.combine(compareLower(bound(before.minimum, before.minimumExclusive),
            bound(this.minimum, this.minimumExclusive)));
        change = change.combine(compareUpper(bound(before.maximum, before.maximumExclusive),
            bound(this.maximum, this.maximumExclusive)));
        change = change.combine(compareOpaque(before.pattern, this.pattern));
        change = change.combine(compareOpaque(before.format, this.format));
        return change.compatibility();
    }

    private static <T> Optional<T> optional(Optional<T> value) {
        return value == null ? Optional.empty() : value;
    }

    private static Optional<BigDecimal> canonical(Optional<BigDecimal> value) {
        return value.map(BigDecimal::stripTrailingZeros);
    }

    private static Change compareOpaque(Optional<?> before, Optional<?> after) {
        if (before.equals(after)) {
            return Change.UNCHANGED;
        }
        if (before.isEmpty()) {
            return Change.NARROWING;
        }
        if (after.isEmpty()) {
            return Change.WIDENING;
        }
        return Change.INCOMPARABLE;
    }

    private static Change compareLower(Optional<BigDecimal> before, Optional<BigDecimal> after,
                                       boolean beforeExclusive, boolean afterExclusive) {
        if (before.equals(after) && beforeExclusive == afterExclusive) {
            return Change.UNCHANGED;
        }
        if (before.isEmpty()) {
            return Change.NARROWING;
        }
        if (after.isEmpty()) {
            return Change.WIDENING;
        }
        int comparison = after.get().compareTo(before.get());
        if (comparison > 0 || comparison == 0 && afterExclusive && !beforeExclusive) {
            return Change.NARROWING;
        }
        if (comparison < 0 || comparison == 0 && !afterExclusive && beforeExclusive) {
            return Change.WIDENING;
        }
        return Change.UNCHANGED;
    }

    private static Change compareLower(Optional<Bound> before, Optional<Bound> after) {
        if (before.equals(after)) {
            return Change.UNCHANGED;
        }
        if (before.isEmpty()) {
            return Change.NARROWING;
        }
        if (after.isEmpty()) {
            return Change.WIDENING;
        }
        int comparison = after.get().value().compareTo(before.get().value());
        if (comparison > 0 || comparison == 0 && after.get().exclusive() && !before.get().exclusive()) {
            return Change.NARROWING;
        }
        return Change.WIDENING;
    }

    private static Change compareUpper(Optional<BigDecimal> before, Optional<BigDecimal> after,
                                       boolean beforeExclusive, boolean afterExclusive) {
        if (before.equals(after) && beforeExclusive == afterExclusive) {
            return Change.UNCHANGED;
        }
        if (before.isEmpty()) {
            return Change.NARROWING;
        }
        if (after.isEmpty()) {
            return Change.WIDENING;
        }
        int comparison = after.get().compareTo(before.get());
        if (comparison < 0 || comparison == 0 && afterExclusive && !beforeExclusive) {
            return Change.NARROWING;
        }
        if (comparison > 0 || comparison == 0 && !afterExclusive && beforeExclusive) {
            return Change.WIDENING;
        }
        return Change.UNCHANGED;
    }

    private static Change compareUpper(Optional<Bound> before, Optional<Bound> after) {
        if (before.equals(after)) {
            return Change.UNCHANGED;
        }
        if (before.isEmpty()) {
            return Change.NARROWING;
        }
        if (after.isEmpty()) {
            return Change.WIDENING;
        }
        int comparison = after.get().value().compareTo(before.get().value());
        if (comparison < 0 || comparison == 0 && after.get().exclusive() && !before.get().exclusive()) {
            return Change.NARROWING;
        }
        return Change.WIDENING;
    }

    private static Optional<Bound> bound(Optional<BigDecimal> inclusive, Optional<BigDecimal> exclusive) {
        return inclusive.map(value -> new Bound(value, false)).or(() -> exclusive.map(value -> new Bound(value, true)));
    }

    private record Bound(BigDecimal value, boolean exclusive) {
    }

    private enum Change {
        UNCHANGED, NARROWING, WIDENING, INCOMPARABLE;

        private Change combine(Change other) {
            if (this == INCOMPARABLE || other == INCOMPARABLE || this != UNCHANGED && other != UNCHANGED && this != other) {
                return INCOMPARABLE;
            }
            return this == UNCHANGED ? other : this;
        }

        private Compatibility compatibility() {
            return Compatibility.valueOf(name());
        }
    }
}
