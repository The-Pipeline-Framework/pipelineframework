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
        if (input == null || input.chunk() == null || input.chunk().content() == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("ScoredChunk and chunk content must be non-null"));
        }
        String id = "prompt_" + Math.abs(input.chunk().id().hashCode());
        String content = "Using this retrieved context: \"" + input.chunk().content()
                + "\". Provide a concise summary for a business user.";
        return Uni.createFrom().item(new Prompt(id, content, 0.3d));
    }
}
