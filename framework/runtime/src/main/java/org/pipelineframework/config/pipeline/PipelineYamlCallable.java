package org.pipelineframework.config.pipeline;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationKind;

/** One explicitly exposed, release-pinned capability visible to a one-turn LLM Query. */
public record PipelineYamlCallable(
    String alias,
    String using,
    String operation,
    ConnectorOperationKind kind,
    int operationVersion,
    String input,
    Optional<String> commandIdGenerator,
    String duplicatePolicy,
    Map<String, Object> config,
    Map<String, Object> policy
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
        commandIdGenerator = Objects.requireNonNull(
            commandIdGenerator, "callable command ID generator must not be null").map(String::trim)
            .filter(value -> !value.isEmpty());
        duplicatePolicy = duplicatePolicy == null || duplicatePolicy.isBlank()
            ? "RETURN_RECORDED"
            : duplicatePolicy.trim();
        config = Map.copyOf(Objects.requireNonNull(config, "callable operation config must not be null"));
        policy = Map.copyOf(Objects.requireNonNull(policy, "callable command policy must not be null"));
        if (ConnectorOperationKind.QUERY.equals(kind)
            && (commandIdGenerator.isPresent() || !policy.isEmpty() || !"RETURN_RECORDED".equals(duplicatePolicy))) {
            throw new IllegalArgumentException("Query callable cannot declare Command execution settings");
        }
    }

    public PipelineYamlCallable(
        String alias,
        String using,
        String operation,
        ConnectorOperationKind kind,
        int operationVersion,
        String input
    ) {
        this(alias, using, operation, kind, operationVersion, input, Optional.empty(), "RETURN_RECORDED", Map.of(), Map.of());
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
