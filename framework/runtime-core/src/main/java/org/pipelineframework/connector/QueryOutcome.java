package org.pipelineframework.connector;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/** Typed semantic result of a unary Query operation. */
public sealed interface QueryOutcome<O>
    permits QueryOutcome.Found, QueryOutcome.NotFound, QueryOutcome.TemporarilyUnavailable,
        QueryOutcome.AuthenticationRequired, QueryOutcome.TerminalFailure {

    String code();

    Optional<QueryObservation> observation();

    record Found<O>(O output, Optional<QueryObservation> observation) implements QueryOutcome<O> {
        public Found {
            output = Objects.requireNonNull(output, "query outcome output must not be null");
            observation = Objects.requireNonNull(observation, "query observation must not be null");
        }

        public Found(O output) {
            this(output, Optional.empty());
        }

        @Override
        public String code() {
            return "found";
        }
    }

    record NotFound<O>(String code, Optional<QueryObservation> observation) implements QueryOutcome<O> {
        public NotFound {
            code = outcomeCode(code);
            observation = Objects.requireNonNull(observation, "query observation must not be null");
        }

        public NotFound(String code) {
            this(code, Optional.empty());
        }
    }

    record TemporarilyUnavailable<O>(String code, Optional<QueryObservation> observation) implements QueryOutcome<O> {
        public TemporarilyUnavailable {
            code = outcomeCode(code);
            observation = Objects.requireNonNull(observation, "query observation must not be null");
        }

        public TemporarilyUnavailable(String code) {
            this(code, Optional.empty());
        }
    }

    record AuthenticationRequired<O>(String code, Optional<QueryObservation> observation) implements QueryOutcome<O> {
        public AuthenticationRequired {
            code = outcomeCode(code);
            observation = Objects.requireNonNull(observation, "query observation must not be null");
        }

        public AuthenticationRequired(String code) {
            this(code, Optional.empty());
        }
    }

    record TerminalFailure<O>(String code, Optional<QueryObservation> observation) implements QueryOutcome<O> {
        public TerminalFailure {
            code = outcomeCode(code);
            observation = Objects.requireNonNull(observation, "query observation must not be null");
        }

        public TerminalFailure(String code) {
            this(code, Optional.empty());
        }
    }

    private static String outcomeCode(String value) {
        if (value == null || !QueryOutcomeCode.PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(
                "query outcome code must start with a lowercase letter, contain only lowercase letters, digits, "
                    + "or hyphens, and be at most 128 characters: " + value);
        }
        return value;
    }
}

final class QueryOutcomeCode {
    static final Pattern PATTERN = Pattern.compile("[a-z][a-z0-9-]{0,127}");

    private QueryOutcomeCode() {
    }
}
