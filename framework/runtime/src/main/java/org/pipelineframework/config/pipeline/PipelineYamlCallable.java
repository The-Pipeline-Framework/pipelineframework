package org.pipelineframework.config.pipeline;

import java.util.Locale;
import java.util.Objects;

import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationKind;

/** One explicitly exposed, release-pinned capability visible to a one-turn LLM Query. */
public record PipelineYamlCallable(
    String alias,
    String using,
    String operation,
    ConnectorOperationKind kind,
    int operationVersion,
    String input
) {
    public PipelineYamlCallable {
        alias = requireToken(alias, "callable alias");
        using = ConnectorBindingName.of(using).value();
        operation = requireDottedName(operation, "callable operation");
        kind = Objects.requireNonNull(kind, "callable operation kind must not be null");
        if (operationVersion < 1) {
            throw new IllegalArgumentException("callable operation version must be positive");
        }
        input = Objects.requireNonNull(input, "callable input contract must not be null").trim();
        if (input.isEmpty()) {
            throw new IllegalArgumentException("callable input contract must not be blank");
        }
    }

    public static ConnectorOperationKind parseKind(String value) {
        String normalized = Objects.requireNonNull(value, "callable operation kind must not be null")
            .trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "command" -> ConnectorOperationKind.COMMAND;
            case "query" -> ConnectorOperationKind.QUERY;
            default -> throw new IllegalArgumentException(
                "callable operation kind must be command or query: " + value);
        };
    }

    public String kindToken() {
        return kind.equals(ConnectorOperationKind.COMMAND) ? "command" : "query";
    }

    private static String requireToken(String value, String label) {
        String normalized = Objects.requireNonNull(value, label + " must not be null").trim();
        if (!normalized.matches("[a-z][a-z0-9]*(?:[_-][a-z0-9]+)*")) {
            throw new IllegalArgumentException(label + " must be a lowercase model-safe token: " + normalized);
        }
        return normalized;
    }

    private static String requireDottedName(String value, String label) {
        String normalized = Objects.requireNonNull(value, label + " must not be null").trim();
        if (!normalized.matches("[a-z][a-z0-9]*(?:\\.[a-z][a-z0-9]*)*")) {
            throw new IllegalArgumentException(label + " must be a lowercase dotted name: " + normalized);
        }
        return normalized;
    }
}
