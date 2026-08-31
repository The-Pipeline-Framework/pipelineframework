package org.pipelineframework.examples.ragproof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.pipelineframework.LocalPipelineControlPlane;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.connector.embedding.EmbeddingRequest;
import org.pipelineframework.connector.vector.VectorUpsertRequest;
import org.pipelineframework.examples.ragproof.connector.RagProofInvocationRecorder;
import org.pipelineframework.examples.ragproof.domain.Answer;
import org.pipelineframework.examples.ragproof.domain.Document;
import org.pipelineframework.examples.ragproof.domain.IndexReceipt;
import org.pipelineframework.examples.ragproof.domain.Question;
import org.pipelineframework.examples.ragproof.pipeline.ProcessBuildRetrievedContextLocalClientStep;
import org.pipelineframework.examples.ragproof.pipeline.ProcessPrepareQuestionEmbeddingLocalClientStep;
import org.pipelineframework.examples.ragproof.pipeline.ProcessPrepareVectorSearchLocalClientStep;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import org.pipelineframework.pipeline.service.pipeline.ProcessAnswerQuestionQueryClientStep;
import org.pipelineframework.pipeline.service.pipeline.ProcessEmbedQuestionQueryClientStep;
import org.pipelineframework.pipeline.service.pipeline.ProcessSearchVectorsQueryClientStep;
import org.pipelineframework.pipeline.service.pipeline.ProcessUpsertVectorCommandClientStep;
import org.pipelineframework.query.QueryStepDescriptorFactory;
import org.pipelineframework.query.QueryStepSupport;

@QuarkusTest
class RagCompositionProofIT {
    @Inject RagProofInvocationRecorder recorder;
    @Inject LocalPipelineControlPlane controlPlane;
    @Inject PipelineRunner pipelineRunner;
    @Inject ProcessPrepareQuestionEmbeddingLocalClientStep prepareQuestionEmbedding;
    @Inject ProcessEmbedQuestionQueryClientStep embedQuestion;
    @Inject ProcessPrepareVectorSearchLocalClientStep prepareVectorSearch;
    @Inject ProcessSearchVectorsQueryClientStep searchVectors;
    @Inject ProcessBuildRetrievedContextLocalClientStep buildRetrievedContext;
    @Inject ProcessAnswerQuestionQueryClientStep answerQuestion;
    @Inject ProcessUpsertVectorCommandClientStep upsertStep;
    @Inject PipelineInvocationRuntime invocationRuntime;
    @Inject QueryStepDescriptorFactory queryDescriptors;
    @Inject QueryStepSupport querySupport;

    @BeforeEach
    void resetEvidence() {
        recorder.reset();
    }

    @AfterEach
    void clearContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void indexesRetrievesAnswersAndReplaysWithoutExternalInfrastructure() {
        String identity = UUID.randomUUID().toString();
        Document document = new Document("doc#revision-" + identity,
            "TPF keeps business transformations typed and deterministic while connectors capture external observations and effects");

        IndexReceipt receipt = executeIndex(document, "index-" + identity);
        assertEquals(document.documentId(), receipt.documentId());
        assertEquals(3, receipt.chunks());
        assertEquals(3, recorder.embeddingCount());
        assertEquals(3, recorder.upsertCount());

        Question question = new Question("question-" + identity, "What does TPF keep typed and deterministic?");
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("proof-tenant", "ask-" + identity, 0));
        Answer answer = executeRetrieve(question);
        assertEquals(question.questionId(), answer.questionId());
        assertTrue(answer.text().contains("typed and"), answer.text());
        assertTrue(answer.text().contains("deterministic"), answer.text());
        assertEquals(4, recorder.embeddingCount());
        assertEquals(1, recorder.searchCount());
        assertEquals(1, recorder.answerCount());
    }

    @Test
    void generatedEmbeddingQueryAndVectorCommandUseCaptureAndEffectReplay() {
        String identity = UUID.randomUUID().toString();
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("proof-tenant", "replay-" + identity, 0));
        EmbeddingRequest embedding = new EmbeddingRequest("item-" + identity, "repeatable observation");

        var descriptor = queryDescriptors.descriptor(
            "ProcessEmbedQuestionService", EmbeddingRequest.class.getName(),
            org.pipelineframework.connector.embedding.EmbeddingResult.class.getName()).await().indefinitely();
        var first = querySupport.queryOneToOne(descriptor, embedding,
            org.pipelineframework.connector.embedding.EmbeddingResult.class).await().atMost(Duration.ofSeconds(5));
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("proof-tenant", "replay-" + identity, 0));
        var replay = querySupport.queryOneToOne(descriptor, embedding,
            org.pipelineframework.connector.embedding.EmbeddingResult.class).await().atMost(Duration.ofSeconds(5));
        assertEquals(first, replay);
        assertEquals(1, recorder.embeddingCount());

        VectorUpsertRequest upsert = new VectorUpsertRequest(first.itemId(), first.text(), first.values());
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("proof-tenant", "replay-" + identity, 0));
        var indexed = invoke(() -> upsertStep.applyOneToOne(upsert));
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("proof-tenant", "replay-" + identity, 0));
        var recorded = invoke(() -> upsertStep.applyOneToOne(upsert));
        assertEquals(indexed, recorded);
        assertEquals(1, recorder.upsertCount());
    }

    @Test
    void generatedMetadataExposesOnlyExistingQueryCommandAndServiceSemantics() throws Exception {
        String contract = metadata("pipeline-contract.json");
        String bindings = metadata("connector-bindings.json");
        String providers = providerMetadata();

        assertTrue(contract.contains("tpf.embedding.EmbeddingRequest"), contract);
        assertTrue(contract.contains("tpf.embedding.EmbeddingResult"), contract);
        assertTrue(contract.contains("tpf.vector.VectorSearchResult"), contract);
        assertTrue(bindings.contains("\"operation\": \"embed\""), bindings);
        assertTrue(bindings.contains("\"operation\": \"upsert\""), bindings);
        assertTrue(bindings.contains("\"operation\": \"search\""), bindings);
        assertTrue(providers.contains("\"id\":\"embed\""), providers);
        assertTrue(providers.contains("\"id\":\"upsert\""), providers);
        assertTrue(providers.contains("\"id\":\"search\""), providers);
        assertTrue(contract.contains("\"repeated\": true"), contract);
        assertTrue(contract.contains("\"id\": \"float32\""), contract);
        assertTrue(contract.contains("tpf.vector.VectorMatch"), contract);

        String all = contract + bindings + providers;
        assertFalse(all.contains("RAGRuntime"));
        assertFalse(all.contains("AgentRuntime"));
        assertFalse(all.contains("Retriever"));
        assertFalse(all.contains("Memory"));
        assertFalse(all.contains("\"kind\": \"rag\""));
    }

    private IndexReceipt executeIndex(Document input, String clientKey) {
        RunAsyncAcceptedDto accepted = controlPlane.executePipelineAsync(
            input, "proof-tenant", clientKey, false).await().atMost(Duration.ofSeconds(10));
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        RuntimeException latest = null;
        while (System.nanoTime() < deadline) {
            try {
                return (IndexReceipt) controlPlane.getExecutionResult(
                    "proof-tenant", accepted.executionId(), IndexReceipt.class, false)
                    .await().atMost(Duration.ofMillis(300));
            } catch (RuntimeException failure) {
                latest = failure;
                try {
                    Thread.sleep(25);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while awaiting proof execution", interrupted);
                }
            }
        }
        throw new AssertionError("proof execution did not produce a result", latest);
    }

    @SuppressWarnings("unchecked")
    private Answer executeRetrieve(Question question) {
        Object result = pipelineRunner.runNestedWithContext(Uni.createFrom().item(question), List.of(
            prepareQuestionEmbedding,
            embedQuestion,
            prepareVectorSearch,
            searchVectors,
            buildRetrievedContext,
            answerQuestion), "retrieve", -1).result();
        return ((Uni<Answer>) result).await().atMost(Duration.ofSeconds(5));
    }

    private <T> T invoke(java.util.function.Supplier<Uni<T>> operation) {
        return invocationRuntime.invokeStepUni(null, null, operation).await().atMost(Duration.ofSeconds(5));
    }

    private static String metadata(String name) throws Exception {
        return Files.readString(Path.of("target/classes/META-INF/pipeline", name));
    }

    private static String providerMetadata() throws Exception {
        try (InputStream stream = Thread.currentThread().getContextClassLoader()
            .getResourceAsStream("META-INF/pipeline/connector-providers.json")) {
            if (stream == null) throw new AssertionError("connector provider metadata is missing");
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
