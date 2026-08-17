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

import java.math.BigDecimal;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/** Shared semantic validation for canonical v3 scalar-wrapper constraints. */
public final class PipelineTemplateWrapperConstraintValidator {
    public static final int MAX_PATTERN_LENGTH = 512;
    public static final int MAX_PATTERN_INPUT_LENGTH = 4_096;

    private static final Set<String> NUMERIC_SCALARS =
        Set.of("int32", "int64", "float32", "float64", "decimal");
    private static final Pattern BACK_REFERENCE = Pattern.compile("(?<!\\\\)\\\\[1-9]");
    private static final Pattern QUANTIFIED_GROUP = Pattern.compile("\\)(?:[?*+]|\\{)");

    private PipelineTemplateWrapperConstraintValidator() {
    }

    public static Optional<Violation> findViolation(
        String scalar,
        PipelineTemplateWrapperConstraints constraints
    ) {
        boolean hasString = constraints.minLength().isPresent() || constraints.maxLength().isPresent()
            || constraints.pattern().isPresent() || constraints.format().isPresent();
        boolean hasNumber = constraints.minimum().isPresent() || constraints.minimumExclusive().isPresent()
            || constraints.maximum().isPresent() || constraints.maximumExclusive().isPresent();
        if (hasString && !"string".equals(scalar)) {
            return violation(Kind.STRING_ON_NON_STRING);
        }
        if (hasNumber && !NUMERIC_SCALARS.contains(scalar)) {
            return violation(Kind.NUMERIC_ON_NON_NUMERIC);
        }
        if (constraints.pattern().isPresent() && constraints.maxLength().isEmpty()) {
            return violation(Kind.PATTERN_REQUIRES_MAX_LENGTH);
        }
        if (constraints.pattern().filter(pattern -> pattern.length() > MAX_PATTERN_LENGTH).isPresent()) {
            return violation(Kind.PATTERN_TOO_LONG);
        }
        if (constraints.pattern().isPresent()
            && constraints.maxLength().filter(length -> length > MAX_PATTERN_INPUT_LENGTH).isPresent()) {
            return violation(Kind.PATTERN_INPUT_TOO_LONG);
        }
        if (constraints.pattern().filter(PipelineTemplateWrapperConstraintValidator::usesUnsafeRegexFeature).isPresent()) {
            return violation(Kind.UNSAFE_PATTERN);
        }
        if (constraints.minLength().isPresent() && constraints.maxLength().isPresent()
            && constraints.minLength().orElseThrow() > constraints.maxLength().orElseThrow()) {
            return violation(Kind.MIN_LENGTH_EXCEEDS_MAX_LENGTH);
        }
        if (constraints.minimum().isPresent() && constraints.minimumExclusive().isPresent()) {
            return violation(Kind.LOWER_BOUNDS_COMBINED);
        }
        if (constraints.maximum().isPresent() && constraints.maximumExclusive().isPresent()) {
            return violation(Kind.UPPER_BOUNDS_COMBINED);
        }
        Optional<BigDecimal> lower = constraints.minimum().isPresent()
            ? constraints.minimum() : constraints.minimumExclusive();
        Optional<BigDecimal> upper = constraints.maximum().isPresent()
            ? constraints.maximum() : constraints.maximumExclusive();
        if (lower.isPresent() && upper.isPresent()) {
            int comparison = lower.orElseThrow().compareTo(upper.orElseThrow());
            if (comparison > 0 || comparison == 0
                && (constraints.minimumExclusive().isPresent() || constraints.maximumExclusive().isPresent())) {
                return violation(Kind.EMPTY_INTERVAL);
            }
        }
        return Optional.empty();
    }

    private static boolean usesUnsafeRegexFeature(String expression) {
        return BACK_REFERENCE.matcher(expression).find()
            || QUANTIFIED_GROUP.matcher(expression).find()
            || expression.contains("(?=")
            || expression.contains("(?!")
            || expression.contains("(?<=")
            || expression.contains("(?<!");
    }

    private static Optional<Violation> violation(Kind kind) {
        return Optional.of(new Violation(kind));
    }

    public enum Kind {
        STRING_ON_NON_STRING,
        NUMERIC_ON_NON_NUMERIC,
        PATTERN_REQUIRES_MAX_LENGTH,
        PATTERN_TOO_LONG,
        PATTERN_INPUT_TOO_LONG,
        UNSAFE_PATTERN,
        MIN_LENGTH_EXCEEDS_MAX_LENGTH,
        LOWER_BOUNDS_COMBINED,
        UPPER_BOUNDS_COMBINED,
        EMPTY_INTERVAL
    }

    public record Violation(Kind kind) {
    }
}
