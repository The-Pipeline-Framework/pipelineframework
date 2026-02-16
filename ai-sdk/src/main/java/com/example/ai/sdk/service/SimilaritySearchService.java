package com.example.ai.sdk.service;

import com.example.ai.sdk.dto.ChunkDto;
import com.example.ai.sdk.dto.ScoredChunkDto;
import com.example.ai.sdk.entity.ScoredChunk;
import com.example.ai.sdk.entity.Vector;
import com.example.ai.sdk.entity.Chunk;
import io.smallrye.mutiny.Multi;
import org.pipelineframework.service.ReactiveStreamingService;

import java.util.ArrayList;
import java.util.List;

/**
 * Similarity search service that finds similar vectors.
 * Implements Uni -> Multi cardinality (UnaryMany).
 */
public class SimilaritySearchService implements ReactiveStreamingService<Vector, ScoredChunk> {

    @Override
    public Multi<ScoredChunk> process(Vector input) {
        if (input == null || input.values() == null || input.values().isEmpty()) {
            return Multi.createFrom().empty();
        }

        // In a real implementation, this would query the vector store
        // For simulation, we'll return some dummy results with calculated similarity scores

        List<ScoredChunk> results = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            Chunk chunk = new Chunk(
                "dummy_chunk_" + i,
                "dummy_doc_" + i,
                "This is a sample chunk content for similarity search " + i,
                i
            );

            // Calculate a fake similarity score based on the input vector
            float score = calculateSimilarity(input, i);

            results.add(new ScoredChunk(chunk, score));
        }

        return Multi.createFrom().iterable(results);
    }

    private float calculateSimilarity(Vector queryVector, int index) {
        // Simple deterministic similarity calculation
        float sum = 0;
        for (int i = 0; i < Math.min(queryVector.values().size(), 10); i++) {
            sum += queryVector.values().get(i) * (index + 1) * 0.1f;
        }
        return 1.0f / (1.0f + Math.abs(sum)); // Convert to similarity score
    }

    /**
     * Alternative method that accepts DTOs directly for TPF delegation
     */
    public Multi<ScoredChunkDto> processDto(Vector input) {
        return process(input).map(sc -> new ScoredChunkDto(
            new ChunkDto(sc.chunk().id(), sc.chunk().documentId(), sc.chunk().content(), sc.chunk().position()),
            sc.score()
        ));
    }
}
