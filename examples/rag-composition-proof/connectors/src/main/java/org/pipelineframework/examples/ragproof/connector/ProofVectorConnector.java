package org.pipelineframework.examples.ragproof.connector;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.vector.VectorMatch;
import org.pipelineframework.connector.vector.VectorSearchQueryOperation;
import org.pipelineframework.connector.vector.VectorSearchRequest;
import org.pipelineframework.connector.vector.VectorSearchResult;
import org.pipelineframework.connector.vector.VectorUpsertCommandOperation;
import org.pipelineframework.connector.vector.VectorUpsertRequest;
import org.pipelineframework.connector.vector.VectorUpsertResult;

/** Binding-local deterministic in-memory vector index. */
@ApplicationScoped
public class ProofVectorConnector implements ConnectorProvider<ProofVectorConnector.ProviderConfiguration> {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("proof.vector");
    private static final ConnectorConfigSchema<ProviderConfiguration> PROVIDER_CONFIGURATION =
        ConnectorConfigSchema.record(ProviderConfiguration.class, "proof.vector.provider", 1);

    @Inject
    RagProofInvocationRecorder recorder;

    private final Map<String, Entry> entries = new HashMap<>();
    private final Map<String, VectorUpsertRequest> commands = new HashMap<>();
    private volatile int dimensions;
    private final UpsertOperation upsert = new UpsertOperation();
    private final SearchOperation search = new SearchOperation();

    @Override public ConnectorProviderId id() { return PROVIDER_ID; }
    @Override public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
    @Override public Optional<ConnectorConfigSchema<ProviderConfiguration>> configurationSchema() {
        return Optional.of(PROVIDER_CONFIGURATION);
    }
    @Override public Collection<? extends ConnectorOperation> operations() { return List.of(upsert, search); }

    @Override
    public CompletionStage<Void> start(ConnectorRuntimeContext context, ProviderConfiguration configuration) {
        dimensions = configuration.dimensions();
        synchronized (entries) {
            entries.clear();
            commands.clear();
        }
        return CompletableFuture.completedStage(null);
    }

    public record ProviderConfiguration(Integer dimensions) {
        public ProviderConfiguration {
            if (dimensions == null || dimensions <= 0) {
                throw new IllegalArgumentException("proof vector dimensions must be positive");
            }
        }
    }

    public record UpsertConfiguration() {
    }

    private final class UpsertOperation implements VectorUpsertCommandOperation<UpsertConfiguration> {
        private static final ConnectorConfigSchema<UpsertConfiguration> CONFIGURATION =
            ConnectorConfigSchema.record(UpsertConfiguration.class, "proof.vector.upsert", 1);

        @Override public Optional<ConnectorConfigSchema<UpsertConfiguration>> configurationSchema() {
            return Optional.of(CONFIGURATION);
        }

        @Override public CommandCapabilities capabilities() {
            return new CommandCapabilities(true, true, false, CommandExecutionPosture.AUTOMATED,
                CommandMachineConfirmation.READ_AFTER_WRITE_VERIFIED, false, Set.of());
        }

        @Override
        public CompletionStage<CommandOutcome<VectorUpsertResult>> dispatch(
            CommandInvocation<VectorUpsertRequest, UpsertConfiguration> invocation
        ) {
            validateDimensions(invocation.input().values());
            String commandId = invocation.dispatchIdentity().orElseThrow(() ->
                new IllegalArgumentException("proof vector upsert requires a command dispatch identity")).commandId();
            synchronized (entries) {
                VectorUpsertRequest previous = commands.putIfAbsent(commandId, invocation.input());
                if (previous != null) {
                    if (!previous.equals(invocation.input())) {
                        throw new IllegalStateException("command ID was reused with different vector content: " + commandId);
                    }
                    return CompletableFuture.completedStage(success(invocation.input()));
                }
                entries.put(invocation.input().itemId(), new Entry(
                    invocation.input().itemId(), invocation.input().content(), invocation.input().values()));
            }
            java.util.Objects.requireNonNull(recorder, "proof recorder must be injected").upsert();
            return CompletableFuture.completedStage(success(invocation.input()));
        }

        private CommandOutcome<VectorUpsertResult> success(VectorUpsertRequest request) {
            return new CommandOutcome.Succeeded<>(new VectorUpsertResult(request.itemId()),
                new CommandConfirmation(CommandMachineConfirmation.READ_AFTER_WRITE_VERIFIED, false), List.of());
        }
    }

    private final class SearchOperation implements VectorSearchQueryOperation {
        @Override
        public CompletionStage<QueryOutcome<VectorSearchResult>> query(
            QueryInvocation<VectorSearchRequest, org.pipelineframework.connector.ConnectorConfigurationDocument,
                VectorSearchResult> invocation
        ) {
            validateDimensions(invocation.input().values());
            List<Entry> snapshot;
            synchronized (entries) { snapshot = List.copyOf(entries.values()); }
            List<VectorMatch> matches = snapshot.stream()
                .map(entry -> new VectorMatch(entry.itemId(), entry.content(), cosine(invocation.input().values(), entry.values())))
                .sorted(Comparator.comparing(VectorMatch::score).reversed().thenComparing(VectorMatch::itemId))
                .limit(invocation.input().limit())
                .toList();
            java.util.Objects.requireNonNull(recorder, "proof recorder must be injected").search();
            return CompletableFuture.completedStage(new QueryOutcome.Found<>(new VectorSearchResult(
                invocation.input().queryId(), invocation.input().queryText(), matches)));
        }
    }

    private void validateDimensions(List<Float> values) {
        if (values.size() != dimensions) {
            throw new IllegalArgumentException("vector dimensions must be " + dimensions + " but were " + values.size());
        }
    }

    private static float cosine(List<Float> left, List<Float> right) {
        double dot = 0.0d, leftNorm = 0.0d, rightNorm = 0.0d;
        for (int index = 0; index < left.size(); index++) {
            double l = left.get(index), r = right.get(index);
            dot += l * r;
            leftNorm += l * l;
            rightNorm += r * r;
        }
        if (leftNorm == 0.0d || rightNorm == 0.0d) return 0.0f;
        return (float) (dot / (StrictMath.sqrt(leftNorm) * StrictMath.sqrt(rightNorm)));
    }

    private record Entry(String itemId, String content, List<Float> values) {
        private Entry { values = List.copyOf(values); }
    }
}
