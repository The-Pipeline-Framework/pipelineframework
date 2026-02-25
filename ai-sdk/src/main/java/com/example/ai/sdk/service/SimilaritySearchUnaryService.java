package com.example.ai.sdk.service;

import com.example.ai.sdk.entity.ScoredChunk;
import com.example.ai.sdk.entity.Vector;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;

import java.util.Comparator;

/**
 * Phase-1 unary facade for similarity search.
 * Adapts the streaming service (Vector -> Multi<ScoredChunk>) to Vector -> Uni<ScoredChunk>.
 */
public class SimilaritySearchUnaryService implements ReactiveService<Vector, ScoredChunk> {

    private final SimilaritySearchService similaritySearchService;

    public SimilaritySearchUnaryService() {
        this.similaritySearchService = new SimilaritySearchService();
    }

    @Override
    public Uni<ScoredChunk> process(Vector input) {
        return similaritySearchService.process(input)
                .collect().asList()
                .onItem().transform(chunks -> chunks.stream()
                        .max(Comparator.comparingDouble(ScoredChunk::score))
                        .orElseThrow(() -> new IllegalStateException("No similarity results produced")));
    }
}
