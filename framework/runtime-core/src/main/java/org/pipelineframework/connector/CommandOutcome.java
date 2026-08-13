package org.pipelineframework.connector;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Typed semantic result of a command operation.
 */
public sealed interface CommandOutcome<O>
    permits CommandOutcome.Succeeded, CommandOutcome.RetryableFailure, CommandOutcome.TerminalFailure,
        CommandOutcome.Ambiguous, CommandOutcome.UserActionRequired {

    default String code() {
        return "legacy";
    }

    default List<CommandReference> references() {
        return List.of();
    }

    default Set<String> flags() {
        return Set.of();
    }

    default CommandConfirmation confirmation() {
        return CommandConfirmation.none();
    }

    record Succeeded<O>(O output, CommandConfirmation confirmation, Set<String> flags, List<CommandReference> references)
        implements CommandOutcome<O> {
        public Succeeded {
            output = Objects.requireNonNull(output, "command outcome output must not be null");
            confirmation = Objects.requireNonNull(confirmation, "command confirmation must not be null");
            flags = copyFlags(flags);
            references = List.copyOf(Objects.requireNonNull(references, "command references must not be null"));
        }

        public Succeeded(O output, CommandConfirmation confirmation, List<CommandReference> references) {
            this(output, confirmation, Set.of(), references);
        }

        @Override
        public String code() {
            return "succeeded";
        }
    }

    record RetryableFailure<O>(
        String code,
        CommandConfirmation confirmation,
        Set<String> flags,
        List<CommandReference> references
    ) implements CommandOutcome<O> {
        public RetryableFailure {
            code = outcomeCode(code);
            confirmation = Objects.requireNonNull(confirmation, "command confirmation must not be null");
            flags = copyFlags(flags);
            references = copyReferences(references);
        }

        public RetryableFailure(String code, List<CommandReference> references) {
            this(code, CommandConfirmation.none(), Set.of(), references);
        }
    }

    record TerminalFailure<O>(
        String code,
        CommandConfirmation confirmation,
        Set<String> flags,
        List<CommandReference> references
    ) implements CommandOutcome<O> {
        public TerminalFailure {
            code = outcomeCode(code);
            confirmation = Objects.requireNonNull(confirmation, "command confirmation must not be null");
            flags = copyFlags(flags);
            references = copyReferences(references);
        }

        public TerminalFailure(String code, List<CommandReference> references) {
            this(code, CommandConfirmation.none(), Set.of(), references);
        }
    }

    record Ambiguous<O>(
        String code,
        CommandConfirmation confirmation,
        Set<String> flags,
        List<CommandReference> references
    ) implements CommandOutcome<O> {
        public Ambiguous {
            code = outcomeCode(code);
            confirmation = Objects.requireNonNull(confirmation, "command confirmation must not be null");
            flags = copyFlags(flags);
            references = copyReferences(references);
        }

        public Ambiguous(String code, List<CommandReference> references) {
            this(code, CommandConfirmation.none(), Set.of(), references);
        }
    }

    /**
     * Requests an attended action. The description is intentionally transient and is never
     * included in durable command-effect metadata.
     */
    record UserActionRequired<O>(
        String code,
        String actionDescription,
        CommandConfirmation confirmation,
        Set<String> flags,
        List<CommandReference> references
    )
        implements CommandOutcome<O> {
        public UserActionRequired {
            code = outcomeCode(code);
            actionDescription = Objects.requireNonNull(actionDescription, "user action description must not be null");
            confirmation = Objects.requireNonNull(confirmation, "command confirmation must not be null");
            flags = copyFlags(flags);
            references = copyReferences(references);
        }

        public UserActionRequired(String code, List<CommandReference> references) {
            this(code, "", CommandConfirmation.none(), Set.of(), references);
        }

        public UserActionRequired(String code, String actionDescription, List<CommandReference> references) {
            this(code, actionDescription, CommandConfirmation.none(), Set.of(), references);
        }
    }

    private static String outcomeCode(String value) {
        if (value == null || !value.matches("[a-z][a-z0-9-]{0,127}")) {
            throw new IllegalArgumentException("command outcome code must be lowercase letters, digits, or hyphens: " + value);
        }
        return value;
    }

    private static List<CommandReference> copyReferences(List<CommandReference> value) {
        return List.copyOf(Objects.requireNonNull(value, "command references must not be null"));
    }

    private static Set<String> copyFlags(Set<String> value) {
        Objects.requireNonNull(value, "command outcome flags must not be null");
        if (value.size() > 16) {
            throw new IllegalArgumentException("command outcome flags must not contain more than 16 values");
        }
        for (String flag : value) {
            ConnectorProviderId.require(flag, "command outcome flag");
        }
        return Set.copyOf(value);
    }
}
