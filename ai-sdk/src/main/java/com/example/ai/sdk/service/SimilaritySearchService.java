package com.example.ai.sdk.service;

import com.example.ai.sdk.dto.ChunkDto;
import com.example.ai.sdk.dto.ScoredChunkDto;
import com.example.ai.sdk.entity.ScoredChunk;
import com.example.ai.sdk.entity.Vector;
import com.example.ai.sdk.entity.Chunk;
import com.example.ai.sdk.mapper.ScoredChunkMapper;
import io.smallrye.mutiny.Multi;
import org.pipelineframework.service.ReactiveStreamingService;

import java.util.ArrayList;
import java.util.List;

/**
 * Similarity search service that finds similar vectors.
 * Implements Uni -> Multi cardinality (UnaryMany).
 */
public class SimilaritySearchService implements ReactiveStreamingService<Vector, ScoredChunk> {

    /**
     * Performs a similarity search for the given query vector and streams scored chunks.
     *
     * @param input the query vector whose values are used to compute deterministic, dummy similarity scores;
     *              if `input` is null, `input.values()` is null, or `input.values()` is empty, no results are emitted
     * @return a Multi that emits up to five dummy ScoredChunk instances (constructed with dummy Chunk data)
     *         whose scores are computed deterministically from the provided vector; emits no items for invalid input
     */
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

    /**
     * Compute a deterministic similarity score for a query vector using a per-index weighting.
     *
     * @param queryVector the vector whose components are used to compute the score
     * @param index       an integer used to scale components (varies the weighting)
     * @return            a float similarity score in the range (0, 1], where higher values indicate greater similarity
     */
    private float calculateSimilarity(Vector queryVector, int index) {
        // Simple deterministic similarity calculation
        float sum = 0;
        for (int i = 0; i < Math.min(queryVector.values().size(), 10); i++) {
            sum += queryVector.values().get(i) * (index + 1) * 0.1f;
        }
        return 1.0f / (1.0f + Math.abs(sum)); // Convert to similarity score
    }

    /**
     * Produce a stream of ScoredChunkDto objects representing similarity search results for the given query vector.
     *
     * @param input the query vector used to perform the similarity search; if null or its values are null or empty, the resulting stream is empty
     * @return a Multi that emits ScoredChunkDto items corresponding to scored chunks produced from the query
     */
    public Multi<ScoredChunkDto> processDto(Vector input) {
        return process(input).map(ScoredChunkMapper.INSTANCE::toDto);
    }
}