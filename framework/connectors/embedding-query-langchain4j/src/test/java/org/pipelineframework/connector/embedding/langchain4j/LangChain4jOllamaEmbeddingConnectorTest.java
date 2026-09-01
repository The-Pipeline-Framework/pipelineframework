package org.pipelineframework.connector.embedding.langchain4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.embedding.EmbeddingProviderConfiguration;
import org.pipelineframework.connector.embedding.EmbeddingQueryOperation;
import org.pipelineframework.connector.embedding.EmbeddingRequest;
import org.pipelineframework.connector.embedding.EmbeddingResult;

class LangChain4jOllamaEmbeddingConnectorTest {
    @Test void preservesContinuationFieldsAndReturnsRealFloats() {
        EmbeddingModel model = mock(EmbeddingModel.class);
        when(model.embed("some text")).thenReturn(Response.from(Embedding.from(new float[]{0.25f, -0.5f})));
        var connector = new LangChain4jOllamaEmbeddingConnector(ignored -> model,
            new LangChain4jOllamaEmbeddingConnector.RuntimeSettings("http://ollama", Duration.ofSeconds(2)));
        connector.start(ConnectorRuntimeContext.empty(), configuration(2)).toCompletableFuture().join();

        EmbeddingResult result = query(connector, new EmbeddingRequest("item", "some text"));

        assertEquals("item", result.itemId());
        assertEquals("some text", result.text());
        assertEquals(List.of(0.25f, -0.5f), result.values());
    }

    @Test void rejectsDimensionMismatchAndInvalidRuntimeSettings() {
        EmbeddingModel model = mock(EmbeddingModel.class);
        when(model.embed("text")).thenReturn(Response.from(Embedding.from(new float[]{1.0f})));
        var connector = new LangChain4jOllamaEmbeddingConnector(ignored -> model,
            LangChain4jOllamaEmbeddingConnector.RuntimeSettings.defaults());
        connector.start(ConnectorRuntimeContext.empty(), configuration(2)).toCompletableFuture().join();
        assertThrows(Exception.class, () -> query(connector, new EmbeddingRequest("id", "text")));
        assertThrows(IllegalArgumentException.class, () ->
            new LangChain4jOllamaEmbeddingConnector.RuntimeSettings(" ", Duration.ofSeconds(1)));
    }

    private static EmbeddingProviderConfiguration configuration(int dimensions) {
        return new EmbeddingProviderConfiguration("proof", Optional.of(dimensions), Optional.empty());
    }

    private static EmbeddingResult query(LangChain4jOllamaEmbeddingConnector connector, EmbeddingRequest request) {
        var operation = (EmbeddingQueryOperation) connector.operations().iterator().next();
        var outcome = operation.query(new QueryInvocation<>(request, ConnectorConfigurationDocument.empty(),
            EmbeddingResult.class, ConnectorExecutionContext.empty())).toCompletableFuture().join();
        return (EmbeddingResult) assertInstanceOf(QueryOutcome.Found.class, outcome).output();
    }
}
