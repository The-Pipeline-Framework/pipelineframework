package com.example.ai.sdk.service;

import com.example.ai.sdk.entity.Chunk;
import com.example.ai.sdk.entity.Vector;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;

/**
 * Phase-1 unary adapter from Chunk to embedding Vector.
 */
public class ChunkEmbeddingService implements ReactiveService<Chunk, Vector> {

    private final EmbeddingService embeddingService;

    public ChunkEmbeddingService() {
        this.embeddingService = new EmbeddingService();
    }

    @Override
    public Uni<Vector> process(Chunk input) {
        if (input == null || input.content() == null || input.content().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("Chunk content must be non-blank"));
        }
        return embeddingService.process(input.content());
    }
}
