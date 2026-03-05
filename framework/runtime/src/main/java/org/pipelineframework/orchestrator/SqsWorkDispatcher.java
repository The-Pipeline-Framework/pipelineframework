package org.pipelineframework.orchestrator;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * SQS-backed work dispatcher for queue-async orchestration.
 */
@ApplicationScoped
public class SqsWorkDispatcher implements WorkDispatcher {

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    Event<ExecutionWorkItem> executionWorkEvent;

    private volatile SqsClient client;

    /**
     * Default constructor for CDI.
     */
    public SqsWorkDispatcher() {
    }

    SqsWorkDispatcher(SqsClient client, PipelineOrchestratorConfig orchestratorConfig, Event<ExecutionWorkItem> executionWorkEvent) {
        this.client = client;
        this.orchestratorConfig = orchestratorConfig;
        this.executionWorkEvent = executionWorkEvent;
    }

    @Override
    public String providerName() {
        return "sqs";
    }

    @Override
    public int priority() {
        return -1000;
    }

    @Override
    public Optional<String> startupValidationError(PipelineOrchestratorConfig config) {
        if (config == null || config.queueUrl().isEmpty() || config.queueUrl().get().isBlank()) {
            return Optional.of("pipeline.orchestrator.queue-url must be configured when dispatcher-provider=sqs.");
        }
        return Optional.empty();
    }

    @Override
    public Uni<Void> enqueueNow(ExecutionWorkItem item) {
        return enqueue(item, Duration.ZERO);
    }

    @Override
    public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
        Duration normalizedDelay = delay == null ? Duration.ZERO : delay;
        return enqueue(item, normalizedDelay);
    }

    private Uni<Void> enqueue(ExecutionWorkItem item, Duration delay) {
        String queueUrl = orchestratorConfig.queueUrl()
            .filter(url -> !url.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.orchestrator.queue-url must be configured when dispatcher-provider=sqs."));
        int delaySeconds = clampDelaySeconds(delay);
        String messageBody = toMessage(item);
        return blocking(() -> {
            sqsClient().sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .delaySeconds(delaySeconds)
                .messageBody(messageBody)
                .build());
            return null;
        }).onItem().transformToUni(ignored -> localLoopback(item));
    }

    private Uni<Void> localLoopback(ExecutionWorkItem item) {
        if (!orchestratorConfig.sqs().localLoopback() || executionWorkEvent == null) {
            return Uni.createFrom().voidItem();
        }
        CompletionStage<ExecutionWorkItem> stage = executionWorkEvent.fireAsync(item);
        return Uni.createFrom().completionStage(() -> stage).replaceWithVoid();
    }

    private static int clampDelaySeconds(Duration delay) {
        long seconds = Math.max(0L, delay.toSeconds());
        return (int) Math.min(900L, seconds);
    }

    private static String toMessage(ExecutionWorkItem item) {
        try {
            return PipelineJson.mapper().writeValueAsString(item);
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing execution work item for SQS dispatch.", e);
        }
    }

    private SqsClient sqsClient() {
        SqsClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            if (client == null) {
                var builder = SqsClient.builder();
                orchestratorConfig.sqs().region()
                    .filter(region -> !region.isBlank())
                    .ifPresent(region -> builder.region(Region.of(region)));
                orchestratorConfig.sqs().endpointOverride()
                    .filter(endpoint -> !endpoint.isBlank())
                    .ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
                client = builder.build();
            }
            return client;
        }
    }

    private static <T> Uni<T> blocking(Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }
}
