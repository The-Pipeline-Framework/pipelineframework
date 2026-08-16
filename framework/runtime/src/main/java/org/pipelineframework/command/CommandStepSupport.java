package org.pipelineframework.command;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.CommandReference;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorConfigurationBinder;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRegistry;
import org.pipelineframework.connector.ConnectorRuntimeContext;
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
    ConnectorBindingRegistry connectorBindingRegistry;

    @Inject
    ConnectorRuntimeContext connectorRuntimeContext;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    private Collection<CommandEffectStore> fixedStores;
    private ConnectorRegistry fixedConnectorRegistry;
    private ConnectorBindingRegistry fixedConnectorBindingRegistry;
    private ConnectorRuntimeContext fixedConnectorRuntimeContext;

    public CommandStepSupport() {
    }

    public CommandStepSupport(
        Collection<CommandConnector<?, ?>> connectors,
        Collection<CommandEffectStore> stores,
        PipelineOrchestratorConfig orchestratorConfig
    ) {
        this.fixedConnectorRegistry = LegacyCommandConnectorProvider.createRegistry(
            List.of(), connectors == null ? List.of() : connectors);
        this.fixedConnectorBindingRegistry = ConnectorBindingRegistry.empty();
        this.fixedConnectorRuntimeContext = ConnectorRuntimeContext.empty();
        this.fixedStores = stores == null ? List.of() : stores;
        this.orchestratorConfig = orchestratorConfig;
    }

    public CommandStepSupport(
        ConnectorRegistry registry,
        Collection<CommandEffectStore> stores,
        PipelineOrchestratorConfig orchestratorConfig
    ) {
        this.fixedConnectorRegistry = java.util.Objects.requireNonNull(registry, "connector registry must not be null");
        this.fixedConnectorBindingRegistry = ConnectorBindingRegistry.empty();
        this.fixedConnectorRuntimeContext = ConnectorRuntimeContext.empty();
        this.fixedStores = stores == null ? List.of() : stores;
        this.orchestratorConfig = orchestratorConfig;
    }

    public CommandStepSupport(
        ConnectorRegistry registry,
        ConnectorBindingRegistry bindingRegistry,
        Collection<CommandEffectStore> stores,
        PipelineOrchestratorConfig orchestratorConfig
    ) {
        this.fixedConnectorRegistry = java.util.Objects.requireNonNull(registry, "connector registry must not be null");
        this.fixedConnectorBindingRegistry = java.util.Objects.requireNonNull(
            bindingRegistry, "connector binding registry must not be null");
        this.fixedConnectorRuntimeContext = ConnectorRuntimeContext.empty();
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
            if (record.status() == CommandEffectStatus.FAILED_RETRYABLE) {
                return Uni.createFrom().failure(new CommandRetryableOutcomeException(recordedOutcomeCode(record)));
            }
            if (record.status() == CommandEffectStatus.DLQ
                || record.status() == CommandEffectStatus.AMBIGUOUS
                || record.status() == CommandEffectStatus.USER_ACTION_REQUIRED) {
                return Uni.createFrom().failure(new CommandOutcomeException(record.status(), recordedOutcomeCode(record)));
            }
            return Uni.createFrom().failure(new IllegalStateException(
                "Command effect " + request.commandId() + " has unsupported retained state " + record.status()));
        }
        return request.descriptor().nativeSelector().isPresent()
            ? executeNative(store, request, request.descriptor().nativeSelector().orElseThrow())
            : executeLegacy(store, request);
    }

    private <I, O> Uni<O> executeLegacy(CommandEffectStore store, CommandRequest<I> request) {
        LegacyCommandConnectorProvider.LegacyCommandOperation operation;
        try {
            operation = requireLegacyOperation(request.descriptor().command());
        } catch (IllegalStateException failure) {
            return Uni.createFrom().failure(failure);
        }
        long effectStartNanos = CommandEffectMetrics.startNanos();
        return beginDispatch(store, request)
            .onItem().<O>transformToUni(ignored -> dispatchLegacyConnector(operation, request))
            .onItem().transformToUni(output -> store.markSucceeded(
                    request.executionContext().tenantId(),
                    request.commandId(),
                    output,
                    System.currentTimeMillis())
                .invoke(ignored -> CommandEffectMetrics.recordTerminalTransition(
                    request.descriptor(), CommandEffectStatus.SUCCEEDED, effectStartNanos))
                .replaceWith(output))
            .onFailure().call(failure -> recordFailure(
                store, request, failure, System.currentTimeMillis(), effectStartNanos).replaceWithVoid());
    }

    private <I, O> Uni<O> executeNative(
        CommandEffectStore store,
        CommandRequest<I> request,
        NativeCommandSelector selector
    ) {
        if (!store.supportsNativeOutcomeSnapshots()) {
            return Uni.createFrom().failure(new IllegalStateException(
                "native command operation " + selector.operationIdentity()
                    + " requires a CommandEffectStore that persists outcome snapshots"));
        }
        long effectStartNanos = CommandEffectMetrics.startNanos();
        return activateBinding(selector)
            .onItem().transformToUni(ignored -> executeActivatedNative(
                store, request, selector, effectStartNanos));
    }

    private <I, O> Uni<O> executeActivatedNative(
        CommandEffectStore store,
        CommandRequest<I> request,
        NativeCommandSelector selector,
        long effectStartNanos
    ) {
        CommandOperation<?, ?, ?> operation;
        try {
            operation = selector.binding().isPresent()
                ? requireBoundCommandOperation(selector)
                : requireRegistry().requireCommandOperation(
                    selector.operationIdentity(), selector.providerMajorVersion(), selector.policy());
        } catch (IllegalStateException | IllegalArgumentException failure) {
            return Uni.createFrom().failure(failure);
        }
        ConnectorConfigurationDocument configuration = new ConnectorConfigurationDocument(request.config());
        Object boundConfiguration;
        ConnectorConfigurationSnapshot snapshot;
        try {
            ConnectorConfigSchema<?> schema = operation.configurationSchema().orElseThrow(() -> new IllegalStateException(
                "native command operation " + selector.operationIdentity() + " does not declare a configuration schema"));
            boundConfiguration = ConnectorConfigurationBinder.bind(
                schema, configuration, "native command operation " + selector.operationIdentity());
            snapshot = ConnectorConfigurationSnapshot.from(schema, configuration, false);
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        }
        return activateProviderFirst(selector)
            .onItem().transformToUni(ignored -> beginDispatch(store, request)
                .onItem().transformToUni(dispatched -> dispatchNative(operation, request, selector, boundConfiguration)
                    .onFailure(CommandStepSupport::isCancellation)
                    .recoverWithItem(new CommandOutcome.Ambiguous<>("provider-dispatch-cancelled", List.of())))
                .onItem().transformToUni(outcome -> applyNativeOutcome(
                    store, request, selector, snapshot, operation.capabilities(), selector.policy(), outcome, effectStartNanos))
                .onFailure().call(failure -> isTypedOutcomeFailure(failure)
                    ? Uni.createFrom().voidItem()
                    : recordFailure(store, request, failure, System.currentTimeMillis(), effectStartNanos).replaceWithVoid()))
            .map(value -> (O) value);
    }

    private <I> Uni<Void> beginDispatch(CommandEffectStore store, CommandRequest<I> request) {
        // Each effect transition records its own wall-clock time so the store can show dispatch/write duration.
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
            .replaceWithVoid();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <I> Uni<CommandOutcome<Object>> dispatchNative(
        CommandOperation<?, ?, ?> operation,
        CommandRequest<I> request,
        NativeCommandSelector selector,
        Object boundConfiguration
    ) {
        CommandOperation raw = operation;
        CompletionStage<CommandOutcome<Object>> stage;
        try {
            stage = raw.dispatch(new org.pipelineframework.connector.CommandInvocation<>(
                request.input(), boundConfiguration, connectorExecutionContext(request)));
        } catch (Throwable failure) {
            return Uni.createFrom().failure(unwrapTransportFailure(failure));
        }
        if (stage == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "native command operation " + selector.operationIdentity() + " returned a null CompletionStage"));
        }
        return Uni.createFrom().completionStage(stage)
            .onFailure().transform(CommandStepSupport::unwrapTransportFailure);
    }

    private <O> Uni<O> applyNativeOutcome(
        CommandEffectStore store,
        CommandRequest<?> request,
        NativeCommandSelector selector,
        ConnectorConfigurationSnapshot configuration,
        CommandCapabilities capabilities,
        CommandPolicy policy,
        CommandOutcome<Object> outcome,
        long effectStartNanos
    ) {
        if (outcome == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "native command operation " + selector.operationIdentity() + " returned a null outcome"));
        }
        if (outcome instanceof CommandOutcome.Succeeded<Object> succeeded) {
            Optional<ConfirmationBarrier> barrier = confirmationBarrier(policy, succeeded.confirmation());
            if (barrier.isPresent()) {
                ConfirmationBarrier resolved = barrier.orElseThrow();
                CommandOutcomeSnapshot snapshot = snapshot(
                    selector, configuration, capabilities, resolved.status(), resolved.code(), succeeded.flags(),
                    succeeded.confirmation(), succeeded.references());
                CommandOutcomeException failure = new CommandOutcomeException(resolved.status(), resolved.code());
                return store.markOutcome(
                        request.executionContext().tenantId(), request.commandId(), resolved.status(), failure, snapshot,
                        System.currentTimeMillis())
                    .invoke(ignored -> CommandEffectMetrics.recordTerminalTransition(
                        request.descriptor(), resolved.status(), effectStartNanos))
                    .onItem().transformToUni(ignored -> Uni.createFrom().failure(failure));
            }
            CommandOutcomeSnapshot snapshot = snapshot(
                selector, configuration, capabilities, CommandEffectStatus.SUCCEEDED, succeeded.code(), succeeded.flags(),
                succeeded.confirmation(), succeeded.references());
            @SuppressWarnings("unchecked")
            O output = (O) succeeded.output();
            return store.markSucceeded(
                    request.executionContext().tenantId(), request.commandId(), output, snapshot, System.currentTimeMillis())
                .invoke(ignored -> CommandEffectMetrics.recordTerminalTransition(
                    request.descriptor(), CommandEffectStatus.SUCCEEDED, effectStartNanos))
                .replaceWith(output);
        }
        CommandEffectStatus status = outcomeStatus(outcome);
        CommandOutcomeSnapshot snapshot = snapshot(
            selector, configuration, capabilities, status, outcome.code(), outcome.flags(), outcome.confirmation(), outcome.references());
        Throwable failure = status == CommandEffectStatus.FAILED_RETRYABLE
            ? new CommandRetryableOutcomeException(outcome.code())
            : new CommandOutcomeException(status, outcome.code());
        return store.markOutcome(
                request.executionContext().tenantId(), request.commandId(), status, failure, snapshot, System.currentTimeMillis())
            .invoke(ignored -> CommandEffectMetrics.recordTerminalTransition(request.descriptor(), status, effectStartNanos))
            .onItem().transformToUni(ignored -> Uni.createFrom().failure(failure));
    }

    private static CommandOutcomeSnapshot snapshot(
        NativeCommandSelector selector,
        ConnectorConfigurationSnapshot configuration,
        CommandCapabilities capabilities,
        CommandEffectStatus status,
        String code,
        java.util.Set<String> flags,
        CommandConfirmation confirmation,
        List<CommandReference> references
    ) {
        java.util.Set<String> declared = capabilities.durableReferenceKinds();
        List<CommandReference> safeReferences = references.stream()
            .filter(reference -> declared.contains(reference.kind()))
            .toList();
        return new CommandOutcomeSnapshot(
            selector.operationIdentity(), selector.providerMajorVersion(), configuration, status, code, flags,
            confirmation.machineConfirmation(), confirmation.userConfirmed(), safeReferences);
    }

    private static Optional<ConfirmationBarrier> confirmationBarrier(
        CommandPolicy policy,
        CommandConfirmation achieved
    ) {
        Optional<CommandMachineConfirmation> requiredMachine = policy.minimumMachineConfirmation();
        if (requiredMachine.isPresent() && !achieved.machineConfirmation().satisfies(requiredMachine.orElseThrow())) {
            return Optional.of(new ConfirmationBarrier(
                CommandEffectStatus.AMBIGUOUS,
                "machine-confirmation-insufficient"));
        }
        if (policy.requireUserConfirmation() && !achieved.userConfirmed()) {
            return Optional.of(new ConfirmationBarrier(
                CommandEffectStatus.USER_ACTION_REQUIRED,
                "user-confirmation-required"));
        }
        return Optional.empty();
    }

    private static CommandEffectStatus outcomeStatus(CommandOutcome<?> outcome) {
        if (outcome instanceof CommandOutcome.RetryableFailure<?>) {
            return CommandEffectStatus.FAILED_RETRYABLE;
        }
        if (outcome instanceof CommandOutcome.TerminalFailure<?>) {
            return CommandEffectStatus.DLQ;
        }
        if (outcome instanceof CommandOutcome.Ambiguous<?>) {
            return CommandEffectStatus.AMBIGUOUS;
        }
        if (outcome instanceof CommandOutcome.UserActionRequired<?>) {
            return CommandEffectStatus.USER_ACTION_REQUIRED;
        }
        throw new IllegalStateException("unsupported native command outcome " + outcome.getClass().getName());
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

    private static boolean isTypedOutcomeFailure(Throwable failure) {
        return failure instanceof CommandOutcomeException || failure instanceof CommandRetryableOutcomeException;
    }

    private static String recordedOutcomeCode(CommandEffectRecord record) {
        return record.outcome()
            .map(CommandOutcomeSnapshot::outcomeCode)
            .orElseGet(() -> switch (record.status()) {
                case FAILED_RETRYABLE -> "recorded-retryable-failure";
                case DLQ -> "recorded-terminal-failure";
                case AMBIGUOUS -> "recorded-ambiguous";
                case USER_ACTION_REQUIRED -> "recorded-user-action-required";
                default -> "recorded-command-effect";
            });
    }

    private static Throwable unwrapTransportFailure(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static boolean isCancellation(Throwable failure) {
        return unwrapTransportFailure(failure) instanceof CancellationException;
    }

    private record ConfirmationBarrier(CommandEffectStatus status, String code) {
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

    private LegacyCommandConnectorProvider.LegacyCommandOperation requireLegacyOperation(String command) {
        return LegacyCommandConnectorProvider.requireOperation(requireRegistry(), command);
    }

    private ConnectorRegistry requireRegistry() {
        ConnectorRegistry registry = fixedConnectorRegistry != null ? fixedConnectorRegistry : connectorRegistry;
        if (registry == null) {
            throw new IllegalStateException("connector registry is not available for command execution");
        }
        return registry;
    }

    private CommandOperation<?, ?, ?> requireBoundCommandOperation(NativeCommandSelector selector) {
        ConnectorBindingRegistry registry = fixedConnectorBindingRegistry != null
            ? fixedConnectorBindingRegistry
            : connectorBindingRegistry;
        if (registry == null) {
            throw new IllegalStateException("connector binding registry is not available for command execution");
        }
        org.pipelineframework.connector.ConnectorBindingName binding = selector.binding().orElseThrow();
        org.pipelineframework.connector.ConnectorProvider<?> provider = registry.requireProvider(binding);
        if (!provider.id().equals(selector.operationIdentity().providerId())
            || provider.version().major() != selector.providerMajorVersion()) {
            throw new IllegalStateException(
                "connector binding '" + binding.value() + "' resolves provider " + provider.id().value()
                    + " v" + provider.version().major() + " but command descriptor requires "
                    + selector.operationIdentity().providerId().value() + " v" + selector.providerMajorVersion());
        }
        return registry.requireCommandOperation(
            binding,
            selector.operationIdentity().operationId(),
            selector.operationIdentity().majorVersion(),
            selector.policy());
    }

    private Uni<Void> activateBinding(NativeCommandSelector selector) {
        if (selector.binding().isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        ConnectorRuntimeContext context = fixedConnectorRuntimeContext != null
            ? fixedConnectorRuntimeContext
            : connectorRuntimeContext;
        if (context == null) {
            return Uni.createFrom().failure(
                new IllegalStateException("connector runtime context is not available for command execution"));
        }
        ConnectorBindingRegistry registry = fixedConnectorBindingRegistry != null
            ? fixedConnectorBindingRegistry
            : connectorBindingRegistry;
        if (registry == null) {
            return Uni.createFrom().failure(
                new IllegalStateException("connector binding registry is not available for command execution"));
        }
        return Uni.createFrom().completionStage(registry.activate(selector.binding().orElseThrow(), context));
    }

    private Uni<Void> activateProviderFirst(NativeCommandSelector selector) {
        if (selector.binding().isPresent()) {
            return Uni.createFrom().voidItem();
        }
        ConnectorRuntimeContext context = fixedConnectorRuntimeContext != null
            ? fixedConnectorRuntimeContext
            : connectorRuntimeContext;
        if (context == null) {
            return Uni.createFrom().failure(
                new IllegalStateException("connector runtime context is not available for command execution"));
        }
        return Uni.createFrom().completionStage(
            requireRegistry().activate(selector.operationIdentity().providerId(), context));
    }

    private static ConnectorExecutionContext connectorExecutionContext(CommandRequest<?> request) {
        AwaitExecutionContext context = request.executionContext();
        return new ConnectorExecutionContext(
            Optional.of(context.tenantId()),
            Optional.of(context.executionId()),
            Optional.of(request.descriptor().stepId()),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private <I, O> Uni<O> dispatchLegacyConnector(
        LegacyCommandConnectorProvider.LegacyCommandOperation operation,
        CommandRequest<I> request
    ) {
        return Uni.createFrom().<O>completionStage(operation.dispatchOutput(request));
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
