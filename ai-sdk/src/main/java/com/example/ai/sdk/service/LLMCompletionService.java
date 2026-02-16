package com.example.ai.sdk.service;

import com.example.ai.sdk.dto.CompletionDto;
import com.example.ai.sdk.dto.PromptDto;
import com.example.ai.sdk.entity.Completion;
import com.example.ai.sdk.entity.Prompt;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;

/**
 * LLM completion service that generates text completions.
 * Implements Uni -> Uni cardinality (UnaryUnary).
 */
public class LLMCompletionService implements ReactiveService<Prompt, Completion> {

    @Override
    public Uni<Completion> process(Prompt input) {
        if (input == null || input.id() == null || input.content() == null) {
            return Uni.createFrom().failure(
                new IllegalArgumentException("input, input.id(), and input.content() must be non-null")
            );
        }

        // Generate deterministic completion based on the prompt
        String completionContent = generateDeterministicCompletion(input.content(), input.id());

        int nonNegativeHash = input.id().hashCode() & 0x7fffffff;
        String completionId = "completion_" + nonNegativeHash;
        String model = "fake-llm-model-v1";
        long timestamp = System.currentTimeMillis();

        Completion completion = new Completion(completionId, completionContent, model, timestamp);

        return Uni.createFrom().item(completion);
    }

    private String generateDeterministicCompletion(String prompt, String promptId) {
        // Generate a deterministic completion based on the prompt and ID
        int seed = prompt.hashCode() + promptId.hashCode();
        StringBuilder sb = new StringBuilder();

        // Create a response that relates to the prompt
        sb.append("Based on your query: \"").append(prompt).append("\", ");
        sb.append("here is a deterministic response generated with seed ").append(seed).append(". ");
        sb.append("This response simulates what an LLM might return. ");
        sb.append("The response length is limited for demonstration purposes.");

        return sb.toString();
    }

    /**
     * Alternative method that accepts DTOs directly for TPF delegation
     */
    public Uni<CompletionDto> processDto(PromptDto input) {
        Prompt entity = new Prompt(input.id(), input.content(), input.temperature());
        return process(entity).map(c -> new CompletionDto(c.id(), c.content(), c.model(), c.timestamp()));
    }
}
