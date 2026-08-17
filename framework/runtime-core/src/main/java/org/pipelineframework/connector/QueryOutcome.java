package org.pipelineframework.connector;

import java.util.Objects;
import java.util.regex.Pattern;

/** Typed semantic result of a unary Query operation. */
public sealed interface QueryOutcome<O>
    permits QueryOutcome.Found, QueryOutcome.NotFound, QueryOutcome.TemporarilyUnavailable,
        QueryOutcome.AuthenticationRequired, QueryOutcome.TerminalFailure {

    String code();

    record Found<O>(O output) implements QueryOutcome<O> {
        public Found {
            output = Objects.requireNonNull(output, "query outcome output must not be null");
        }

        @Override
        public String code() {
            return "found";
        }
    }

    record NotFound<O>(String code) implements QueryOutcome<O> {
        public NotFound {
            code = outcomeCode(code);
        }
    }

    record TemporarilyUnavailable<O>(String code) implements QueryOutcome<O> {
        public TemporarilyUnavailable {
            code = outcomeCode(code);
        }
    }

    record AuthenticationRequired<O>(String code) implements QueryOutcome<O> {
        public AuthenticationRequired {
            code = outcomeCode(code);
        }
    }

    record TerminalFailure<O>(String code) implements QueryOutcome<O> {
        public TerminalFailure {
            code = outcomeCode(code);
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
