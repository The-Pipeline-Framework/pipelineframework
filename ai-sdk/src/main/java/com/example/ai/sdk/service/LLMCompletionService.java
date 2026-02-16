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

    /**
     * Generate a deterministic Completion for the given Prompt.
     *
     * <p>Validates that {@code input}, {@code input.id()}, and {@code input.content()} are non-null;
     * if any are null the returned {@code Uni} will fail with an {@link IllegalArgumentException}.</p>
     *
     * @param input the prompt to process; must have a non-null id and non-null content
     * @return a Completion populated with deterministic content derived from the prompt and prompt id,
     *         a deterministic completion id, the model label "fake-llm-model-v1", and a generation timestamp
     */
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

    /**
     * Constructs a deterministic, human-readable completion string derived from a prompt and its identifier.
     *
     * @param prompt   the user's prompt text used as the basis of the completion
     * @param promptId the prompt's identifier used to seed deterministic content
     * @return         a deterministic completion message that references the prompt and includes a numeric seed
     */
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
     * Processes a prompt represented as a DTO and returns the resulting completion as a DTO.
     *
     * @param input the prompt data transfer object containing id, content, and temperature
     * @return a CompletionDto containing the generated completion's id, content, model, and timestamp
     */
    public Uni<CompletionDto> processDto(PromptDto input) {
        Prompt entity = new Prompt(input.id(), input.content(), input.temperature());
        return process(entity).map(c -> new CompletionDto(c.id(), c.content(), c.model(), c.timestamp()));
    }
}