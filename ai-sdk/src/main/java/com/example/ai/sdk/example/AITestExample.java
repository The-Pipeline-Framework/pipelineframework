package com.example.ai.sdk.example;

import com.example.ai.sdk.entity.Chunk;
import com.example.ai.sdk.entity.Document;
import com.example.ai.sdk.entity.Prompt;
import com.example.ai.sdk.service.DocumentChunkingService;
import com.example.ai.sdk.service.DocumentChunkingUnaryService;
import com.example.ai.sdk.service.EmbeddingService;
import com.example.ai.sdk.service.ChunkEmbeddingService;
import com.example.ai.sdk.service.LLMCompletionService;
import com.example.ai.sdk.service.SimilaritySearchService;
import com.example.ai.sdk.service.SimilaritySearchUnaryService;
import com.example.ai.sdk.service.ScoredChunkPromptService;
import com.example.ai.sdk.service.VectorStoreService;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Example usage of the AI SDK.
 * Demonstrates how the SDK can be used independently of TPF.
 */
public class AITestExample {
    
    /**
     * Example entry point that demonstrates a complete SDK workflow: document chunking, embedding generation,
     * storing embeddings in a vector store, similarity search, and LLM completion.
     *
     * <p>The method runs a sequential demo using the SDK services and prints progress and results to standard output.
     */
    public static void main(String[] args) {
        // Initialize services
        DocumentChunkingService chunkingService = new DocumentChunkingService();
        EmbeddingService embeddingService = new EmbeddingService();
        VectorStoreService vectorStoreService = new VectorStoreService();
        SimilaritySearchService similaritySearchService = new SimilaritySearchService();
        LLMCompletionService llmService = new LLMCompletionService();
        
        // Example 1: Process a document through the pipeline
        System.out.println("=== Document Processing Pipeline ===");
        
        Document doc = new Document("doc1", "This is a sample document with multiple sentences. " +
                "We will chunk this document into smaller pieces. " +
                "Then we will generate embeddings for each chunk. " +
                "Finally, we will store the embeddings in a vector store.");
        
        // Chunk the document
        List<Chunk> chunks = chunkingService.process(doc).collect().asList().await().atMost(Duration.ofSeconds(30));
        System.out.println("Chunked document into " + chunks.size() + " chunks");
        
        // Process each chunk: generate embedding and store
        for (int i = 0; i < chunks.size(); i++) {
            Chunk chunk = chunks.get(i);
            String content = Objects.toString(chunk.content(), "");
            System.out.println("Processing chunk " + i + ": " + content.substring(0, Math.min(30, content.length())) + "...");
            
            // Generate embedding
            var embedding = embeddingService.process(chunk.content()).await().atMost(Duration.ofSeconds(30));
            System.out.println("Generated embedding with " + embedding.values().size() + " dimensions");
            
            // Store the embedding
            var storeResult = vectorStoreService.process(embedding).await().atMost(Duration.ofSeconds(30));
            System.out.println("Stored embedding: " + storeResult.success());
        }
        
        // Example 2: Perform similarity search
        System.out.println("\n=== Similarity Search ===");
        var queryEmbedding = embeddingService.process("Find information about document processing").await().atMost(Duration.ofSeconds(30));
        var similarChunks = similaritySearchService.process(queryEmbedding).collect().asList().await().atMost(Duration.ofSeconds(30));
        System.out.println("Found " + similarChunks.size() + " similar chunks");
        
        // Example 3: Generate LLM completion
        System.out.println("\n=== LLM Completion ===");
        var prompt = new Prompt("prompt1", "Summarize the key concepts of document processing and vector storage.", 0.7); // Temperature controls output randomness.
        var completion = llmService.process(prompt).await().atMost(Duration.ofSeconds(30));
        System.out.println("LLM Response: " + completion.content());

        // Example 4: Phase-1 unary operator-style pipeline (1->1 services only)
        System.out.println("\n=== Phase 1 Unary Operator Pipeline ===");
        DocumentChunkingUnaryService unaryChunking = new DocumentChunkingUnaryService();
        ChunkEmbeddingService chunkEmbedding = new ChunkEmbeddingService();
        SimilaritySearchUnaryService unarySearch = new SimilaritySearchUnaryService();
        ScoredChunkPromptService promptFromChunk = new ScoredChunkPromptService();

        var topChunk = unaryChunking.process(doc).await().atMost(Duration.ofSeconds(30));
        var topChunkEmbedding = chunkEmbedding.process(topChunk).await().atMost(Duration.ofSeconds(30));
        var topMatch = unarySearch.process(topChunkEmbedding).await().atMost(Duration.ofSeconds(30));
        var generatedPrompt = promptFromChunk.process(topMatch).await().atMost(Duration.ofSeconds(30));
        var unaryCompletion = llmService.process(generatedPrompt).await().atMost(Duration.ofSeconds(30));

        System.out.println("Top chunk id: " + topChunk.id());
        System.out.println("Top similarity score: " + topMatch.score());
        System.out.println("Unary pipeline completion: " + unaryCompletion.content());
        
        System.out.println("\n=== SDK Demo Complete ===");
    }
}
