package org.pipelineframework.examples.ragproof.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.CommandDispatchIdentity;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.embedding.EmbeddingProviderConfiguration;
import org.pipelineframework.connector.embedding.EmbeddingQueryOperation;
import org.pipelineframework.connector.embedding.EmbeddingRequest;
import org.pipelineframework.connector.embedding.EmbeddingResult;
import org.pipelineframework.connector.vector.VectorSearchQueryOperation;
import org.pipelineframework.connector.vector.VectorSearchRequest;
import org.pipelineframework.connector.vector.VectorSearchResult;
import org.pipelineframework.connector.vector.VectorUpsertCommandOperation;
import org.pipelineframework.connector.vector.VectorUpsertRequest;
import org.pipelineframework.connector.vector.VectorUpsertResult;

class ProofConnectorsTest {
    private RagProofInvocationRecorder recorder;
    private ProofEmbeddingConnector embedding;
    private ProofVectorConnector vectors;

    @BeforeEach
    void startProviders() {
        recorder = new RagProofInvocationRecorder();
        embedding = new ProofEmbeddingConnector();
        embedding.recorder = recorder;
        embedding.start(ConnectorRuntimeContext.empty(),
            new EmbeddingProviderConfiguration("proof", Optional.of(8), Optional.empty())).toCompletableFuture().join();
        vectors = new ProofVectorConnector();
        vectors.recorder = recorder;
        vectors.start(ConnectorRuntimeContext.empty(), new ProofVectorConnector.ProviderConfiguration(8))
            .toCompletableFuture().join();
    }

    @Test
    void embeddingIsDeterministicTypedAndDimensioned() {
        EmbeddingResult first = embed("id", "Alpha beta beta");
        EmbeddingResult second = embed("id", "Alpha beta beta");

        assertEquals(first, second);
        assertEquals(8, first.values().size());
        assertEquals("id", first.itemId());
        assertEquals("Alpha beta beta", first.text());
        assertEquals(2, recorder.embeddingCount());
    }

    @Test
    void upsertsAndSearchesWithDeterministicOrderAndConflictDetection() {
        upsert("command-a", new VectorUpsertRequest("b", "second", vector(1.0f, 0.0f)));
        upsert("command-b", new VectorUpsertRequest("a", "first", vector(1.0f, 0.0f)));
        upsert("command-a", new VectorUpsertRequest("b", "second", vector(1.0f, 0.0f)));
        VectorSearchResult result = search(new VectorSearchRequest("q", "question", vector(1.0f, 0.0f), 1));

        assertEquals(List.of("a"), result.matches().stream().map(match -> match.itemId()).toList());
        assertEquals(1, result.matches().size());
        assertEquals(2, recorder.upsertCount());
        assertThrows(IllegalStateException.class,
            () -> upsert("command-a", new VectorUpsertRequest("b", "changed", vector(1.0f, 0.0f))));
    }

    @Test
    void emptySearchIsFoundAndDimensionMismatchFails() {
        QueryOutcome<VectorSearchResult> outcome = querySearch(
            new VectorSearchRequest("q", "question", vector(1.0f, 0.0f), 3));
        QueryOutcome.Found<?> found = assertInstanceOf(QueryOutcome.Found.class, outcome);
        assertEquals(List.of(), ((VectorSearchResult) found.output()).matches());
        assertThrows(IllegalArgumentException.class,
            () -> search(new VectorSearchRequest("q", "question", List.of(1.0f), 3)));
    }

    private EmbeddingResult embed(String id, String text) {
        EmbeddingQueryOperation operation = (EmbeddingQueryOperation) embedding.operations().iterator().next();
        var outcome = operation.query(new QueryInvocation<>(new EmbeddingRequest(id, text),
            ConnectorConfigurationDocument.empty(), EmbeddingResult.class, ConnectorExecutionContext.empty()))
            .toCompletableFuture().join();
        return (EmbeddingResult) assertInstanceOf(QueryOutcome.Found.class, outcome).output();
    }

    @SuppressWarnings("unchecked")
    private void upsert(String commandId, VectorUpsertRequest request) {
        var operation = (VectorUpsertCommandOperation<ProofVectorConnector.UpsertConfiguration>) vectors.operations().stream()
            .filter(candidate -> candidate.id().equals("upsert")).findFirst().orElseThrow();
        var outcome = operation.dispatch(new CommandInvocation<>(request, new ProofVectorConnector.UpsertConfiguration(),
            ConnectorExecutionContext.empty(), Optional.of(new CommandDispatchIdentity(commandId, commandId + "-attempt"))))
            .toCompletableFuture().join();
        assertInstanceOf(CommandOutcome.Succeeded.class, outcome);
    }

    private VectorSearchResult search(VectorSearchRequest request) {
        return (VectorSearchResult) assertInstanceOf(QueryOutcome.Found.class, querySearch(request)).output();
    }

    private QueryOutcome<VectorSearchResult> querySearch(VectorSearchRequest request) {
        VectorSearchQueryOperation operation = (VectorSearchQueryOperation) vectors.operations().stream()
            .filter(candidate -> candidate.id().equals("search")).findFirst().orElseThrow();
        return operation.query(new QueryInvocation<>(request, ConnectorConfigurationDocument.empty(),
            VectorSearchResult.class, ConnectorExecutionContext.empty())).toCompletableFuture().join();
    }

    private static List<Float> vector(float first, float second) {
        return List.of(first, second, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f);
    }
}
