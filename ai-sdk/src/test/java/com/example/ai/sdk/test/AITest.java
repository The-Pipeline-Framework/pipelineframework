package com.example.ai.sdk.test;

import com.example.ai.sdk.entity.Chunk;
import com.example.ai.sdk.entity.Document;
import com.example.ai.sdk.entity.Prompt;
import com.example.ai.sdk.service.DocumentChunkingService;
import com.example.ai.sdk.service.EmbeddingService;
import com.example.ai.sdk.service.LLMCompletionService;
import com.example.ai.sdk.service.SimilaritySearchService;
import com.example.ai.sdk.service.VectorStoreService;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AITest {

    @Test
    void sdkPipelineWorks() {
        DocumentChunkingService chunkingService = new DocumentChunkingService();
        EmbeddingService embeddingService = new EmbeddingService();
        VectorStoreService vectorStoreService = new VectorStoreService();
        SimilaritySearchService similaritySearchService = new SimilaritySearchService();
        LLMCompletionService llmService = new LLMCompletionService();

        Document doc = new Document(
            "doc1",
            "This is a sample document with multiple sentences. " +
                "We will chunk this document into smaller pieces. " +
                "Then we will generate embeddings for each chunk. " +
                "Finally, we will store the embeddings in a vector store."
        );

        List<Chunk> chunks = chunkingService.process(doc).collect().asList().await().atMost(Duration.ofSeconds(30));
        assertFalse(chunks.isEmpty(), "Chunking should produce at least one chunk");

        for (Chunk chunk : chunks) {
            var embedding = embeddingService.process(chunk.content()).await().atMost(Duration.ofSeconds(30));
            assertNotNull(embedding, "Embedding result must not be null");
            assertNotNull(embedding.values(), "Embedding values must not be null");

            var storeResult = vectorStoreService.process(embedding).await().atMost(Duration.ofSeconds(30));
            assertTrue(storeResult.success(), "Vector store should return success");
        }

        var queryEmbedding = embeddingService.process("Find information about document processing")
            .await().atMost(Duration.ofSeconds(30));
        var similarChunks = similaritySearchService.process(queryEmbedding)
            .collect().asList().await().atMost(Duration.ofSeconds(30));
        assertFalse(similarChunks.isEmpty(), "Similarity search should return at least one result");

        var prompt = new Prompt("prompt1", "Summarize document processing and vector storage.", 0.7);
        var completion = llmService.process(prompt).await().atMost(Duration.ofSeconds(30));
        assertNotNull(completion.content(), "Completion content must not be null");
        assertFalse(completion.content().isEmpty(), "Completion content must not be empty");
    }
}
