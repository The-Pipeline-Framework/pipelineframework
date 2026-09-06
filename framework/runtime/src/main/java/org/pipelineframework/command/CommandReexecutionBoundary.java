package org.pipelineframework.command;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import io.smallrye.mutiny.Multi;
import org.pipelineframework.orchestrator.ExecutionRedriveIntent;
import org.pipelineframework.orchestrator.TransitionWorkerCommand;

/**
 * Framework worker boundary for invocation-scoped Command retry and reissue admission.
 *
 * <p>The transition worker enters this boundary using its persisted command. The mutable
 * admission and its claim operations remain private to the Command runtime.</p>
 */
public final class CommandReexecutionBoundary {
    private CommandReexecutionBoundary() {
    }

    public static Multi<?> invokeTransitionWorker(
        TransitionWorkerCommand command,
        Supplier<Multi<?>> execution
    ) {
        Objects.requireNonNull(command, "command must not be null");
        Objects.requireNonNull(execution, "execution must not be null");
        return Multi.createFrom().deferred(() -> {
            CommandReexecutionScope.Snapshot previous = CommandReexecutionScope.capture();
            try {
                Optional<CommandReexecutionScope.AdmissionHandle> admission = install(command);
                Multi<?> result = Objects.requireNonNull(
                    execution.get(), "Command reexecution returned a null stream");
                return result
                    .onCompletion().invoke(() -> admission.ifPresent(
                        CommandReexecutionScope.AdmissionHandle::requireConsumed))
                    .onTermination().invoke((failure, cancelled) -> CommandReexecutionScope.restore(previous));
            } catch (Throwable failure) {
                CommandReexecutionScope.restore(previous);
                return Multi.createFrom().failure(failure);
            }
        });
    }

    private static Optional<CommandReexecutionScope.AdmissionHandle> install(TransitionWorkerCommand command) {
        ExecutionRedriveIntent intent = command.redriveIntent();
        return switch (intent) {
            case RETRY_FAILED_COMMAND -> Optional.of(CommandReexecutionScope.installRetry(
                command.redriveCommandId().orElseThrow(), command.transitionKey()));
            case REISSUE_COMMAND -> Optional.of(CommandReexecutionScope.installReissue(
                command.redriveCommandId().orElseThrow(),
                command.transitionKey(),
                command.redriveReason().orElseThrow(() -> new IllegalStateException(
                    "REISSUE_COMMAND worker command is missing its audit reason"))));
            case REPLAY -> {
                CommandReexecutionScope.clear();
                yield Optional.empty();
            }
        };
    }
}
