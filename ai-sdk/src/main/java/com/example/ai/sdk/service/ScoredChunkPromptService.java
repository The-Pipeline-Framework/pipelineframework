package com.example.ai.sdk.service;

import com.example.ai.sdk.entity.Prompt;
import com.example.ai.sdk.entity.ScoredChunk;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;

/**
 * Unary adapter that converts top similarity matches into an LLM prompt.
 */
public class ScoredChunkPromptService implements ReactiveService<ScoredChunk, Prompt> {

    @Override
    public Uni<Prompt> process(ScoredChunk input) {
        if (input == null || input.chunk() == null || input.chunk().id() == null || input.chunk().content() == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("ScoredChunk, chunk id, and chunk content must be non-null"));
        }
        int nonNegativeHash = input.chunk().id().hashCode() & 0x7fffffff;
        String id = "prompt_" + nonNegativeHash;
        String content = "Using this retrieved context: \"" + input.chunk().content()
                + "\". Provide a concise summary for a business user.";
        return Uni.createFrom().item(new Prompt(id, content, 0.3d));
    }
}
