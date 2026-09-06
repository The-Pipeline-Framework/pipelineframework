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
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.Currency;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Shared semantic validation for canonical v3 scalar-wrapper constraints. */
public final class PipelineTemplateWrapperConstraintValidator {
    public static final int MAX_PATTERN_LENGTH = 512;
    public static final int MAX_PATTERN_INPUT_LENGTH = 4_096;

    private static final Set<String> NUMERIC_SCALARS =
        Set.of("int32", "int64", "float32", "float64", "decimal");
    private static final Pattern BACK_REFERENCE = Pattern.compile("(?<!\\\\)\\\\[1-9]");
    private static final Pattern QUANTIFIED_GROUP = Pattern.compile("\\)(?:[?*+]|\\{)");
    private static final String REGEX_ESCAPE = "\\\\(?:x\\{[0-9A-Fa-f]+\\}|x[0-9A-Fa-f]{2}|u[0-9A-Fa-f]{4}|[pP]\\{[^}]+\\}|.)";
    private static final String REGEX_ATOM = "(\\[(?:" + REGEX_ESCAPE
        + "|[^\\]\\\\])*\\]|" + REGEX_ESCAPE + "|\\.|[^\\\\\\[\\]().|^$*+?{}])";
    private static final String REGEX_QUANTIFIER = "(?:[*+?]|\\{\\d+(?:,\\d*)?\\})[?+]?";
    private static final Pattern ADJACENT_QUANTIFIED_ATOMS =
        Pattern.compile(REGEX_ATOM + REGEX_QUANTIFIER + REGEX_ATOM + REGEX_QUANTIFIER);

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
        if (!constraints.allowedValues().isEmpty()) {
            if ("payload_ref".equals(scalar)) {
                return violation(Kind.ALLOWED_VALUES_ON_NON_SCALAR_JSON);
            }
            for (Object allowedValue : constraints.allowedValues()) {
                if (!matchesScalar(scalar, allowedValue)) {
                    return violation(Kind.ALLOWED_VALUE_TYPE_MISMATCH);
                }
                if (!satisfiesOtherConstraints(scalar, allowedValue, constraints)) {
                    return violation(Kind.ALLOWED_VALUE_OUTSIDE_CONSTRAINTS);
                }
            }
        }
        return Optional.empty();
    }

    private static boolean matchesScalar(String scalar, Object value) {
        if ("bool".equals(scalar)) {
            return value instanceof Boolean;
        }
        if ("int32".equals(scalar)) {
            return value instanceof BigInteger integer && integer.bitLength() < 32;
        }
        if ("int64".equals(scalar)) {
            return value instanceof BigInteger integer && integer.bitLength() < 64;
        }
        if (Set.of("float32", "float64", "decimal").contains(scalar)) {
            if (!(value instanceof BigInteger || value instanceof BigDecimal)) {
                return false;
            }
            String number = value.toString();
            return switch (scalar) {
                case "float32" -> Float.isFinite(Float.parseFloat(number));
                case "float64" -> Double.isFinite(Double.parseDouble(number));
                default -> true;
            };
        }
        if (!(value instanceof String text)) {
            return false;
        }
        try {
            switch (scalar) {
                case "uuid" -> UUID.fromString(text);
                case "timestamp" -> Instant.parse(text);
                case "datetime" -> LocalDateTime.parse(text);
                case "date" -> LocalDate.parse(text);
                case "duration" -> Duration.parse(text);
                case "currency" -> Currency.getInstance(text);
                case "uri" -> URI.create(text);
                case "path" -> Path.of(text);
                case "bytes" -> Base64.getDecoder().decode(text);
                default -> { }
            }
            return true;
        } catch (RuntimeException failure) {
            return false;
        }
    }

    private static boolean satisfiesOtherConstraints(
        String scalar,
        Object value,
        PipelineTemplateWrapperConstraints constraints
    ) {
        if ("string".equals(scalar)) {
            String text = (String) value;
            int length = text.codePointCount(0, text.length());
            if (constraints.minLength().filter(minimum -> length < minimum).isPresent()
                || constraints.maxLength().filter(maximum -> length > maximum).isPresent()
                || constraints.pattern().filter(pattern -> !Pattern.compile(pattern).matcher(text).matches()).isPresent()
                || constraints.format().isPresent() && !isPracticalEmail(text)) {
                return false;
            }
        }
        if (NUMERIC_SCALARS.contains(scalar)) {
            BigDecimal number = value instanceof BigInteger integer ? new BigDecimal(integer) : (BigDecimal) value;
            if (constraints.minimum().filter(bound -> number.compareTo(bound) < 0).isPresent()
                || constraints.minimumExclusive().filter(bound -> number.compareTo(bound) <= 0).isPresent()
                || constraints.maximum().filter(bound -> number.compareTo(bound) > 0).isPresent()
                || constraints.maximumExclusive().filter(bound -> number.compareTo(bound) >= 0).isPresent()) {
                return false;
            }
        }
        return true;
    }

    private static boolean isPracticalEmail(String value) {
        if (value.chars().anyMatch(Character::isWhitespace)) {
            return false;
        }
        int at = value.indexOf('@');
        if (at <= 0 || at != value.lastIndexOf('@') || at == value.length() - 1) {
            return false;
        }
        for (String label : value.substring(at + 1).split("\\.", -1)) {
            if (label.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private static boolean usesUnsafeRegexFeature(String expression) {
        return BACK_REFERENCE.matcher(expression).find()
            || QUANTIFIED_GROUP.matcher(expression).find()
            || hasAdjacentOverlappingQuantifiedAtoms(expression)
            || expression.contains("(?=")
            || expression.contains("(?!")
            || expression.contains("(?<=")
            || expression.contains("(?<!");
    }

    private static boolean hasAdjacentOverlappingQuantifiedAtoms(String expression) {
        Matcher matcher = ADJACENT_QUANTIFIED_ATOMS.matcher(expression);
        int from = 0;
        while (matcher.find(from)) {
            if (atomsMayOverlap(matcher.group(1), matcher.group(2))) {
                return true;
            }
            from = matcher.start(2);
        }
        return false;
    }

    private static boolean atomsMayOverlap(String left, String right) {
        if (left.equals(".") || right.equals(".")) {
            return true;
        }
        Optional<Set<Integer>> leftCharacters = atomCharacters(left);
        Optional<Set<Integer>> rightCharacters = atomCharacters(right);
        if (leftCharacters.isEmpty() || rightCharacters.isEmpty()) {
            return true;
        }
        return leftCharacters.orElseThrow().stream().anyMatch(rightCharacters.orElseThrow()::contains);
    }

    private static Optional<Set<Integer>> atomCharacters(String atom) {
        if (!atom.startsWith("[")) {
            return atom.startsWith("\\") ? escapedAtomCharacters(atom) : Optional.of(Set.of(atom.codePointAt(0)));
        }
        if (atom.length() < 3 || atom.charAt(1) == '^') {
            return Optional.empty();
        }
        Set<Integer> characters = new HashSet<>();
        for (int index = 1; index < atom.length() - 1; index++) {
            int current = atom.codePointAt(index);
            if (current == '\\') {
                if (++index >= atom.length() - 1) {
                    return Optional.empty();
                }
                current = atom.codePointAt(index);
            }
            if (index + 2 < atom.length() - 1 && atom.charAt(index + 1) == '-') {
                int end = atom.codePointAt(index + 2);
                if (end == '\\' || current > end || end - current > MAX_PATTERN_INPUT_LENGTH) {
                    return Optional.empty();
                }
                for (int value = current; value <= end; value++) {
                    characters.add(value);
                }
                index += 2;
            } else {
                characters.add(current);
            }
        }
        return Optional.of(Set.copyOf(characters));
    }

    private static Optional<Set<Integer>> escapedAtomCharacters(String atom) {
        return switch (atom) {
            case "\\d" -> Optional.of(characterRange('0', '9'));
            case "\\w" -> {
                Set<Integer> characters = new HashSet<>(characterRange('0', '9'));
                characters.addAll(characterRange('A', 'Z'));
                characters.addAll(characterRange('a', 'z'));
                characters.add((int) '_');
                yield Optional.of(Set.copyOf(characters));
            }
            case "\\s" -> Optional.of(Set.of((int) ' ', (int) '\t', (int) '\n', 0x0B, (int) '\f', (int) '\r'));
            case "\\t" -> Optional.of(Set.of((int) '\t'));
            case "\\n" -> Optional.of(Set.of((int) '\n'));
            case "\\r" -> Optional.of(Set.of((int) '\r'));
            case "\\f" -> Optional.of(Set.of((int) '\f'));
            case "\\a" -> Optional.of(Set.of(0x07));
            case "\\e" -> Optional.of(Set.of(0x1B));
            default -> escapedCodePoint(atom).map(Set::of);
        };
    }

    private static Optional<Integer> escapedCodePoint(String atom) {
        try {
            if (atom.matches("\\\\x[0-9A-Fa-f]{2}")) {
                return Optional.of(Integer.parseInt(atom.substring(2), 16));
            }
            if (atom.matches("\\\\x\\{[0-9A-Fa-f]+}")) {
                int codePoint = Integer.parseInt(atom.substring(3, atom.length() - 1), 16);
                return Character.isValidCodePoint(codePoint) ? Optional.of(codePoint) : Optional.empty();
            }
            if (atom.matches("\\\\u[0-9A-Fa-f]{4}")) {
                return Optional.of(Integer.parseInt(atom.substring(2), 16));
            }
            if (atom.length() == 2 && ".^$|?*+()[]{}\\\\-".indexOf(atom.charAt(1)) >= 0) {
                return Optional.of((int) atom.charAt(1));
            }
        } catch (NumberFormatException ignored) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    private static Set<Integer> characterRange(char start, char end) {
        Set<Integer> characters = new HashSet<>();
        for (int value = start; value <= end; value++) {
            characters.add(value);
        }
        return Set.copyOf(characters);
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
        EMPTY_INTERVAL,
        ALLOWED_VALUES_ON_NON_SCALAR_JSON,
        ALLOWED_VALUE_TYPE_MISMATCH,
        ALLOWED_VALUE_OUTSIDE_CONSTRAINTS
    }

    public record Violation(Kind kind) {
    }
}
