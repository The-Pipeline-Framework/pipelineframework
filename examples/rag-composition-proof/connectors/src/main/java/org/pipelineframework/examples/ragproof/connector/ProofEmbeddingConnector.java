package org.pipelineframework.examples.ragproof.connector;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.Normalizer;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.embedding.EmbeddingProviderConfiguration;
import org.pipelineframework.connector.embedding.EmbeddingQueryOperation;
import org.pipelineframework.connector.embedding.EmbeddingRequest;
import org.pipelineframework.connector.embedding.EmbeddingResult;

/** Offline deterministic embedding provider used only by the proof application. */
@ApplicationScoped
public class ProofEmbeddingConnector implements ConnectorProvider<EmbeddingProviderConfiguration> {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("proof.embedding");
    private static final ConnectorConfigSchema<EmbeddingProviderConfiguration> CONFIGURATION =
        ConnectorConfigSchema.record(EmbeddingProviderConfiguration.class, "proof.embedding.provider", 1);

    @Inject
    RagProofInvocationRecorder recorder;

    private volatile int dimensions;
    private final EmbeddingQueryOperation operation = new Operation();

    @Override public ConnectorProviderId id() { return PROVIDER_ID; }
    @Override public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
    @Override public Optional<ConnectorConfigSchema<EmbeddingProviderConfiguration>> configurationSchema() {
        return Optional.of(CONFIGURATION);
    }
    @Override public Collection<? extends ConnectorOperation> operations() { return List.of(operation); }

    @Override
    public CompletionStage<Void> start(ConnectorRuntimeContext context, EmbeddingProviderConfiguration configuration) {
        dimensions = configuration.dimensions().orElse(8);
        return CompletableFuture.completedStage(null);
    }

    private final class Operation implements EmbeddingQueryOperation {
        @Override
        public CompletionStage<QueryOutcome<EmbeddingResult>> query(
            QueryInvocation<EmbeddingRequest, org.pipelineframework.connector.ConnectorConfigurationDocument,
                EmbeddingResult> invocation
        ) {
            RagProofInvocationRecorder evidence = java.util.Objects.requireNonNull(recorder, "proof recorder must be injected");
            evidence.embedding();
            return CompletableFuture.completedStage(new QueryOutcome.Found<>(new EmbeddingResult(
                invocation.input().itemId(), invocation.input().text(), embed(invocation.input().text(), dimensions))));
        }
    }

    static List<Float> embed(String text, int dimensions) {
        if (dimensions <= 0) {
            throw new IllegalArgumentException("proof embedding dimensions must be positive");
        }
        String normalized = Normalizer.normalize(text, Normalizer.Form.NFKC).toLowerCase(Locale.ROOT).trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("proof embedding text must not be blank");
        }
        double[] totals = new double[dimensions];
        for (String token : normalized.split("[^\\p{L}\\p{N}]+")) {
            if (!token.isEmpty()) {
                byte[] digest = sha256(token);
                int bucket = Math.floorMod(ByteBuffer.wrap(digest, 0, Integer.BYTES).getInt(), dimensions);
                totals[bucket] += (digest[Integer.BYTES] & 1) == 0 ? 1.0d : -1.0d;
            }
        }
        double norm = 0.0d;
        for (double total : totals) norm += total * total;
        if (norm == 0.0d) {
            totals[0] = 1.0d;
            norm = 1.0d;
        }
        double divisor = StrictMath.sqrt(norm);
        java.util.ArrayList<Float> values = new java.util.ArrayList<>(dimensions);
        for (double total : totals) values.add((float) (total / divisor));
        return List.copyOf(values);
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
