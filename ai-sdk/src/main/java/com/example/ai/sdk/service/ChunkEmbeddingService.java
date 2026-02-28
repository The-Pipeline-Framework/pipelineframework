package com.example.ai.sdk.service;

import com.example.ai.sdk.entity.Chunk;
import com.example.ai.sdk.entity.Vector;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;

import java.util.Objects;

/**
 * Phase-1 unary adapter from Chunk to embedding Vector.
 */
public class ChunkEmbeddingService implements ReactiveService<Chunk, Vector> {

    private final EmbeddingService embeddingService;

    public ChunkEmbeddingService() {
        this(new EmbeddingService());
    }

    public ChunkEmbeddingService(EmbeddingService embeddingService) {
        this.embeddingService = Objects.requireNonNull(embeddingService, "embeddingService must not be null");
    }

    @Override
    public Uni<Vector> process(Chunk input) {
        if (input == null || input.content() == null || input.content().isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("Chunk content must be non-blank"));
        }
        return embeddingService.process(input.content());
    }
}
