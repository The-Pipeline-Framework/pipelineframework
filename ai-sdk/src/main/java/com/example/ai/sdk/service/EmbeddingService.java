package com.example.ai.sdk.service;

import com.example.ai.sdk.dto.VectorDto;
import com.example.ai.sdk.entity.Vector;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;

import java.util.ArrayList;
import java.util.List;

/**
 * Embedding service that generates vector embeddings from text.
 * Implements Uni -> Uni cardinality (UnaryUnary).
 */
public class EmbeddingService implements ReactiveService<String, Vector> {

    @Override
    public Uni<Vector> process(String input) {
        String safeInput = input == null ? "" : input;
        // Generate deterministic embedding based on text hash
        List<Float> embedding = generateDeterministicEmbedding(safeInput);
        int nonNegativeHash = safeInput.hashCode() & 0x7fffffff;
        String id = "embedding_" + nonNegativeHash;
        Vector vector = new Vector(id, embedding);

        return Uni.createFrom().item(vector);
    }

    private List<Float> generateDeterministicEmbedding(String text) {
        List<Float> embedding = new ArrayList<>(128);
        int seed = text.hashCode();

        // Generate 128-dimensional embedding
        for (int i = 0; i < 128; i++) {
            float value = (float) Math.sin(seed + i) * 0.1f;
            embedding.add(value);
        }

        return embedding;
    }

    /**
     * Alternative method that accepts DTOs directly for TPF delegation
     */
    public Uni<VectorDto> processDto(String input) {
        return process(input).map(v -> new VectorDto(v.id(), v.values()));
    }
}
