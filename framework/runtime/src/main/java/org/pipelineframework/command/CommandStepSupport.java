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
import org.pipelineframework.connector.ConnectorRegistry;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.step.NonRetryableException;

/**
 * Runtime bridge used by generated command step beans.
 */
@ApplicationScoped
public class CommandStepSupport {
    @Inject
    Instance<CommandEffectStore> stores;

    @Inject
    ConnectorRegistry connectorRegistry;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    private Collection<CommandEffectStore> fixedStores;
    private ConnectorRegistry fixedConnectorRegistry;

    public CommandStepSupport() {
    }

    public CommandStepSupport(
        Collection<CommandConnector<?, ?>> connectors,
        Collection<CommandEffectStore> stores,
        PipelineOrchestratorConfig orchestratorConfig
    ) {
        this.fixedConnectorRegistry = LegacyCommandConnectorProvider.createRegistry(
            List.of(), connectors == null ? List.of() : connectors);
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
        if (commandIdGenerator == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("commandIdGenerator must not be null"));
        }
        AwaitExecutionContext context;
        try {
            context = captureExecutionContext();
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        }
        return descriptor.onItem().transformToUni(
            resolved -> execute(resolved, commandIdGenerator, input, context));
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
        return execute(descriptor, commandIdGenerator, input, context);
    }

    private <I, O> Uni<O> execute(
        CommandDescriptor descriptor,
        CommandIdGenerator<? super I> commandIdGenerator,
        I input,
        AwaitExecutionContext context
    ) {
        if (descriptor == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("descriptor must not be null"));
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
        return store.find(context.tenantId(), request.commandId())
            .onItem().transformToUni(existing -> handleExistingOrExecute(existing, store, request));
    }

    private <I, O> Uni<O> handleExistingOrExecute(
        Optional<CommandEffectRecord> existing,
        CommandEffectStore store,
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
            .onItem().<O>transformToUni(ignored -> dispatchLegacyConnector(request))
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
        return new AwaitExecutionContext(
            context.tenantId(),
            context.executionId(),
            context.currentStepIndex(),
            context.continuationMode(),
            context.terminalOutputOwnership());
    }

    private <I, O> Uni<O> dispatchLegacyConnector(CommandRequest<I> request) {
        ConnectorRegistry registry = fixedConnectorRegistry != null ? fixedConnectorRegistry : connectorRegistry;
        if (registry == null) {
            return Uni.createFrom().failure(new IllegalStateException("connector registry is not available for command execution"));
        }
        return Uni.createFrom().<O>completionStage(LegacyCommandConnectorProvider.<I, O>dispatch(registry, request));
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
