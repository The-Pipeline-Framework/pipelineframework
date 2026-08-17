package org.pipelineframework.command;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRegistry;

/**
 * Runtime-only bridge that registers existing Mutiny-based {@link CommandConnector} beans as
 * command operations. Its operation identities address this registry only; command IDs and
 * command-effect identities remain owned by the existing command runtime.
 */
public final class LegacyCommandConnectorProvider implements ConnectorProvider<Void> {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("tpf.legacy.command");

    private final List<LegacyCommandOperation> operations;
    private final Map<String, LegacyCommandOperation> operationsByCommand;

    private LegacyCommandConnectorProvider(List<LegacyCommandOperation> operations) {
        this.operations = List.copyOf(operations);
        Map<String, LegacyCommandOperation> byCommand = new LinkedHashMap<>();
        for (LegacyCommandOperation operation : operations) {
            byCommand.put(operation.command, operation);
        }
        operationsByCommand = Map.copyOf(byCommand);
    }

    /**
     * Creates the synthetic provider when legacy connectors are present.
     */
    public static Optional<LegacyCommandConnectorProvider> from(Collection<? extends CommandConnector<?, ?>> connectors) {
        Objects.requireNonNull(connectors, "legacy command connectors must not be null");
        List<LegacyConnectorCandidate> candidates = new ArrayList<>();
        for (CommandConnector<?, ?> connector : connectors) {
            if (connector == null) {
                continue;
            }
            candidates.add(candidate(connector));
        }
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        candidates.sort(Comparator
            .comparing(LegacyConnectorCandidate::command)
            .thenComparing(LegacyConnectorCandidate::className));
        validateDistinctCommands(candidates);
        return Optional.of(new LegacyCommandConnectorProvider(operations(candidates)));
    }

    /**
     * Creates a registry that includes native providers and, when present, the legacy bridge.
     */
    public static ConnectorRegistry createRegistry(
        Collection<? extends ConnectorProvider<?>> nativeProviders,
        Collection<? extends CommandConnector<?, ?>> legacyConnectors
    ) {
        Objects.requireNonNull(nativeProviders, "native connector providers must not be null");
        Optional<LegacyCommandConnectorProvider> legacyProvider = from(legacyConnectors);
        List<ConnectorProvider<?>> providers = new ArrayList<>(nativeProviders);
        if (legacyProvider.isEmpty()) {
            return new ConnectorRegistry(providers);
        }
        providers.add(legacyProvider.orElseThrow());
        return ConnectorRegistry.withFrameworkProviders(providers, List.of(PROVIDER_ID));
    }

    /**
     * Dispatches one legacy connector through its registered command operation.
     */
    public static <I, O> CompletionStage<O> dispatch(ConnectorRegistry registry, CommandRequest<I> request) {
        Objects.requireNonNull(registry, "connector registry must not be null");
        Objects.requireNonNull(request, "command request must not be null");
        try {
            return requireOperation(registry, request.descriptor().command()).dispatchOutput(request);
        } catch (IllegalStateException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    static LegacyCommandOperation requireOperation(ConnectorRegistry registry, String command) {
        Objects.requireNonNull(registry, "connector registry must not be null");
        ConnectorProvider<?> provider = registry.providers().get(PROVIDER_ID);
        if (!(provider instanceof LegacyCommandConnectorProvider legacyProvider)) {
            throw noConnector(command);
        }
        return legacyProvider.requireOperation(command);
    }

    @Override
    public ConnectorProviderId id() {
        return PROVIDER_ID;
    }

    @Override
    public ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    @Override
    public Collection<? extends ConnectorOperation> operations() {
        return operations;
    }

    private LegacyCommandOperation requireOperation(String command) {
        LegacyCommandOperation operation = operationsByCommand.get(command);
        if (operation == null) {
            throw noConnector(command);
        }
        return operation;
    }

    private static LegacyConnectorCandidate candidate(CommandConnector<?, ?> connector) {
        String command;
        try {
            command = connector.command();
        } catch (Throwable failure) {
            throw new IllegalArgumentException(
                "legacy CommandConnector " + connector.getClass().getName() + " threw while declaring its command", failure);
        }
        if (command == null || command.isBlank()) {
            throw new IllegalArgumentException(
                "legacy CommandConnector " + connector.getClass().getName() + " command must not be blank");
        }
        return new LegacyConnectorCandidate(command, connector.getClass().getName(), connector);
    }

    private static void validateDistinctCommands(List<LegacyConnectorCandidate> candidates) {
        for (int index = 1; index < candidates.size(); index++) {
            LegacyConnectorCandidate previous = candidates.get(index - 1);
            LegacyConnectorCandidate current = candidates.get(index);
            if (previous.command().equals(current.command())) {
                throw new IllegalArgumentException(
                    "duplicate legacy CommandConnector command '" + current.command() + "': "
                        + previous.className() + " and " + current.className());
            }
        }
    }

    private static List<LegacyCommandOperation> operations(List<LegacyConnectorCandidate> candidates) {
        Map<String, List<LegacyConnectorCandidate>> byReadableId = new LinkedHashMap<>();
        for (LegacyConnectorCandidate candidate : candidates) {
            byReadableId.computeIfAbsent(readableOperationId(candidate.command()), ignored -> new ArrayList<>()).add(candidate);
        }
        List<LegacyCommandOperation> operations = new ArrayList<>();
        for (LegacyConnectorCandidate candidate : candidates) {
            String readableId = readableOperationId(candidate.command());
            String operationId = byReadableId.get(readableId).size() == 1
                ? readableId
                : readableId + ".c" + hex(candidate.command());
            operations.add(new LegacyCommandOperation(operationId, candidate.command(), candidate.connector()));
        }
        return List.copyOf(operations);
    }

    private static String readableOperationId(String command) {
        if (isConnectorIdentifier(command)) {
            return "legacy." + command;
        }
        String hyphenSeparated = command.replace('-', '.');
        if (isConnectorIdentifier(hyphenSeparated)) {
            return "legacy." + hyphenSeparated;
        }
        return "legacy.command.c" + hex(command);
    }

    private static boolean isConnectorIdentifier(String value) {
        try {
            ConnectorProviderId.of(value);
            return true;
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    private static String hex(String value) {
        StringBuilder encoded = new StringBuilder();
        for (byte current : value.getBytes(StandardCharsets.UTF_8)) {
            encoded.append(Character.forDigit((current >>> 4) & 0xf, 16));
            encoded.append(Character.forDigit(current & 0xf, 16));
        }
        return encoded.toString();
    }

    private static IllegalStateException noConnector(String command) {
        return new IllegalStateException("No CommandConnector found for command '" + command + "'");
    }

    private record LegacyConnectorCandidate(String command, String className, CommandConnector<?, ?> connector) {
    }

    private record LegacyCommandRequestConfiguration(CommandRequest<?> request) {
        private LegacyCommandRequestConfiguration {
            request = Objects.requireNonNull(request, "legacy command request must not be null");
        }
    }

    static final class LegacyCommandOperation implements CommandOperation<Object, LegacyCommandRequestConfiguration, Object> {
        private final String operationId;
        private final String command;
        private final CommandConnector<Object, Object> connector;

        @SuppressWarnings("unchecked")
        private LegacyCommandOperation(String operationId, String command, CommandConnector<?, ?> connector) {
            this.operationId = operationId;
            this.command = command;
            this.connector = (CommandConnector<Object, Object>) connector;
        }

        @Override
        public String id() {
            return operationId;
        }

        @Override
        public CompletionStage<CommandOutcome<Object>> dispatch(
            CommandInvocation<Object, LegacyCommandRequestConfiguration> invocation
        ) {
            CommandRequest<?> request = invocation.configuration().request();
            return adapt(request);
        }

        @SuppressWarnings("unchecked")
        <I, O> CompletionStage<O> dispatchOutput(CommandRequest<I> request) {
            return (CompletionStage<O>) invoke(request);
        }

        private CompletionStage<CommandOutcome<Object>> adapt(CommandRequest<?> request) {
            return LegacyCompletionStages.map(invoke(request), value -> new CommandOutcome.Succeeded<>(
                value, org.pipelineframework.connector.CommandConfirmation.none(), List.of()));
        }

        private CompletionStage<Object> invoke(CommandRequest<?> request) {
            Uni<Object> result;
            try {
                result = connector.execute(cast(request));
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(unwrap(failure));
            }
            if (result == null) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                    "legacy CommandConnector " + connector.getClass().getName() + " returned null Uni for command '" + command + "'"));
            }
            CompletableFuture<Object> stage;
            try {
                stage = result.subscribeAsCompletionStage();
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(unwrap(failure));
            }
            if (stage == null) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                    "legacy CommandConnector " + connector.getClass().getName()
                        + " returned a Uni without a CompletionStage for command '" + command + "'"));
            }
            return stage;
        }

        private static ConnectorExecutionContext executionContext(CommandRequest<?> request) {
            AwaitExecutionContext context = request.executionContext();
            return new ConnectorExecutionContext(
                Optional.of(context.tenantId()),
                Optional.of(context.executionId()),
                Optional.of(request.descriptor().stepId()),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }

        @SuppressWarnings("unchecked")
        private static CommandRequest<Object> cast(CommandRequest<?> request) {
            return (CommandRequest<Object>) request;
        }

    }

    private static final class LegacyCompletionStages {
        private LegacyCompletionStages() {
        }

        private static <T, R> CompletionStage<R> map(CompletionStage<T> source, Function<? super T, ? extends R> mapper) {
            CompletableFuture<T> sourceFuture = source.toCompletableFuture();
            ForwardingCompletableFuture<R> target = new ForwardingCompletableFuture<>(sourceFuture);
            sourceFuture.whenComplete((value, failure) -> {
                if (failure != null) {
                    target.completeFromSourceFailure(unwrap(failure));
                    return;
                }
                try {
                    target.complete(mapper.apply(value));
                } catch (Throwable mappingFailure) {
                    target.completeFromSourceFailure(unwrap(mappingFailure));
                }
            });
            return target;
        }
    }

    private static final class ForwardingCompletableFuture<T> extends CompletableFuture<T> {
        private final CompletableFuture<?> source;

        private ForwardingCompletableFuture(CompletableFuture<?> source) {
            this.source = source;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled) {
                source.cancel(mayInterruptIfRunning);
            }
            return cancelled;
        }

        private boolean completeFromSourceFailure(Throwable failure) {
            return super.completeExceptionally(failure);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
