package org.pipelineframework.command;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.step.NonRetryableException;

/**
 * Runtime bridge used by generated command step beans.
 */
@ApplicationScoped
public class CommandStepSupport {
    @Inject
    Instance<CommandConnector<?, ?>> connectors;

    @Inject
    Instance<CommandEffectStore> stores;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    private Collection<CommandConnector<?, ?>> fixedConnectors;
    private Collection<CommandEffectStore> fixedStores;

    public CommandStepSupport() {
    }

    public CommandStepSupport(
        Collection<CommandConnector<?, ?>> connectors,
        Collection<CommandEffectStore> stores,
        PipelineOrchestratorConfig orchestratorConfig
    ) {
        this.fixedConnectors = connectors == null ? List.of() : connectors;
        this.fixedStores = stores == null ? List.of() : stores;
        this.orchestratorConfig = orchestratorConfig;
    }

    public <I, O> Uni<O> execute(
        Uni<CommandDescriptor> descriptor,
        CommandIdGenerator<? super I> commandIdGenerator,
        I input
    ) {
        if (descriptor == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        return descriptor.onItem().transformToUni(resolved -> execute(resolved, commandIdGenerator, input));
    }

    public <I, O> Uni<O> execute(
        CommandDescriptor descriptor,
        CommandIdGenerator<? super I> commandIdGenerator,
        I input
    ) {
        if (descriptor == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
        }
        if (commandIdGenerator == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("commandIdGenerator must not be null"));
        }
        AwaitExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException e) {
            return Uni.createFrom().failure(e);
        }
        String commandId;
        try {
            commandId = commandIdGenerator.commandId(descriptor, input);
        } catch (Throwable failure) {
            return Uni.createFrom().failure(failure);
        }
        if (commandId == null || commandId.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                "Command id generator " + descriptor.commandIdGenerator() + " returned a blank command id"));
        }
        if (!commandId.equals(commandId.trim())) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                "Command id generator " + descriptor.commandIdGenerator()
                    + " returned a command id with leading or trailing whitespace"));
        }
        CommandRequest<I> request = new CommandRequest<>(
            descriptor,
            commandId,
            input,
            context,
            descriptor.config());
        CommandEffectStore store = selectStore();
        CommandConnector<I, O> connector = selectConnector(descriptor.command());
        return store.find(context.tenantId(), request.commandId())
            .onItem().transformToUni(existing -> handleExistingOrExecute(existing, store, connector, request));
    }

    private <I, O> Uni<O> handleExistingOrExecute(
        Optional<CommandEffectRecord> existing,
        CommandEffectStore store,
        CommandConnector<I, O> connector,
        CommandRequest<I> request
    ) {
        if (existing.isPresent()) {
            CommandEffectRecord record = existing.get();
            if (record.status() == CommandEffectStatus.SUCCEEDED) {
                if (request.descriptor().duplicatePolicy() == CommandDuplicatePolicy.FAIL) {
                    CommandEffectMetrics.recordDuplicate(request.descriptor(), "rejected");
                    return Uni.createFrom().failure(new NonRetryableException(
                        "Duplicate command completion for commandId " + request.commandId()));
                }
                @SuppressWarnings("unchecked")
                O recorded = (O) record.output();
                CommandRecordedDuplicateMarker.mark(recorded);
                CommandEffectMetrics.recordDuplicate(request.descriptor(), "returned_recorded");
                return Uni.createFrom().item(recorded);
            }
            if (record.status() == CommandEffectStatus.PENDING || record.status() == CommandEffectStatus.DISPATCHING) {
                CommandEffectMetrics.recordDuplicate(request.descriptor(), "in_progress");
                return Uni.createFrom().failure(new CommandInProgressException(
                    "Command already in progress for commandId " + request.commandId()));
            }
        }
        // Each effect transition records its own wall-clock time so the store can show dispatch/write duration.
        long effectStartNanos = CommandEffectMetrics.startNanos();
        return store.createPending(request, System.currentTimeMillis())
            .invoke(ignored -> CommandEffectMetrics.recordTransition(
                request.descriptor(),
                CommandEffectStatus.PENDING))
            .onItem().transformToUni(ignored -> store.markDispatching(
                request.executionContext().tenantId(),
                request.commandId(),
                System.currentTimeMillis()))
            .invoke(ignored -> CommandEffectMetrics.recordTransition(
                request.descriptor(),
                CommandEffectStatus.DISPATCHING))
            .onItem().transformToUni(ignored -> connector.execute(request))
            .onItem().transformToUni(output -> store.markSucceeded(
                    request.executionContext().tenantId(),
                    request.commandId(),
                    output,
                    System.currentTimeMillis())
                .invoke(ignored -> CommandEffectMetrics.recordTerminalTransition(
                    request.descriptor(),
                    CommandEffectStatus.SUCCEEDED,
                    effectStartNanos))
                .replaceWith(output))
            .onFailure().call(failure -> recordFailure(
                    store,
                    request,
                    failure,
                    System.currentTimeMillis(),
                    effectStartNanos)
                .replaceWithVoid());
    }

    private Uni<CommandEffectRecord> recordFailure(
        CommandEffectStore store,
        CommandRequest<?> request,
        Throwable failure,
        long nowEpochMs,
        long effectStartNanos
    ) {
        if (isNonRetryable(failure)) {
            return store.markDlq(
                request.executionContext().tenantId(),
                request.commandId(),
                failure,
                nowEpochMs)
                .invoke(ignored -> CommandEffectMetrics.recordTerminalTransition(
                    request.descriptor(),
                    CommandEffectStatus.DLQ,
                    effectStartNanos));
        }
        return store.markFailed(
            request.executionContext().tenantId(),
            request.commandId(),
            failure,
            nowEpochMs)
            .invoke(ignored -> CommandEffectMetrics.recordTerminalTransition(
                request.descriptor(),
                CommandEffectStatus.FAILED_RETRYABLE,
                effectStartNanos));
    }

    private boolean isNonRetryable(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof NonRetryableException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private AwaitExecutionContext captureExecutionContext() {
        if (orchestratorConfig == null || orchestratorConfig.mode() != OrchestratorMode.QUEUE_ASYNC) {
            throw new IllegalStateException("Command steps require pipeline.orchestrator.mode=QUEUE_ASYNC.");
        }
        AwaitExecutionContext context = AwaitExecutionContextHolder.get();
        if (context == null) {
            throw new IllegalStateException("Command step executed without queue-async execution context.");
        }
        return new AwaitExecutionContext(context.tenantId(), context.executionId(), context.currentStepIndex());
    }

    @SuppressWarnings("unchecked")
    private <I, O> CommandConnector<I, O> selectConnector(String command) {
        if (fixedConnectors != null) {
            for (CommandConnector<?, ?> connector : fixedConnectors) {
                if (connector != null && command.equals(connector.command())) {
                    return (CommandConnector<I, O>) connector;
                }
            }
        }
        if (connectors != null) {
            for (CommandConnector<?, ?> connector : connectors) {
                if (connector != null && command.equals(connector.command())) {
                    return (CommandConnector<I, O>) connector;
                }
            }
        }
        throw new IllegalStateException("No CommandConnector found for command '" + command + "'");
    }

    /**
     * Returns the single configured effect store. Command v1 does not support store routing;
     * multiple stores are treated as a misconfiguration rather than silently picking one.
     */
    private CommandEffectStore selectStore() {
        CommandEffectStore fixedStore = selectSingleStore(fixedStores);
        if (fixedStore != null) {
            return fixedStore;
        }
        CommandEffectStore injectedStore = selectSingleStore(stores);
        if (injectedStore != null) {
            return injectedStore;
        }
        throw new IllegalStateException("No CommandEffectStore configured for command step");
    }

    private CommandEffectStore selectSingleStore(Iterable<CommandEffectStore> candidates) {
        if (candidates == null) {
            return null;
        }
        CommandEffectStore selected = null;
        for (CommandEffectStore store : candidates) {
            if (store == null) {
                continue;
            }
            if (selected != null) {
                throw new IllegalStateException(
                    "Multiple CommandEffectStore instances configured; command steps support a single effect store");
            }
            selected = store;
        }
        return selected;
    }
}
