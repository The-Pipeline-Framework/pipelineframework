package org.pipelineframework.awaitable.kafka;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.pipelineframework.awaitable.AwaitInteractionNotFoundException;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionAdmissionFailures;
import org.pipelineframework.awaitable.AwaitCompletionMetrics;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.PipelineExecutionService;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * SmallRye Reactive Messaging consumer that admits Kafka await completions.
 */
@ApplicationScoped
@IfBuildProperty(name = "tpf.await.kafka.reactive-messaging.enabled", stringValue = "true")
public class KafkaAwaitCompletionConsumer {

    public static final String INCOMING_CHANNEL = "tpf-await-kafka-responses";
    private static final Logger LOG = Logger.getLogger(KafkaAwaitCompletionConsumer.class);
    private static final Duration DEFAULT_NOT_FOUND_RETRY_DELAY = Duration.ofMillis(100);
    private static final int DEFAULT_NOT_FOUND_RETRY_ATTEMPTS = 30;

    @Inject
    PipelineExecutionService executionService;
    Duration notFoundRetryDelay = DEFAULT_NOT_FOUND_RETRY_DELAY;
    int notFoundRetryAttempts = DEFAULT_NOT_FOUND_RETRY_ATTEMPTS;

    public KafkaAwaitCompletionConsumer() {
    }

    KafkaAwaitCompletionConsumer(PipelineExecutionService executionService) {
        this.executionService = executionService;
    }

    KafkaAwaitCompletionConsumer(
        PipelineExecutionService executionService,
        Duration notFoundRetryDelay,
        int notFoundRetryAttempts
    ) {
        this.executionService = executionService;
        this.notFoundRetryDelay = notFoundRetryDelay == null ? DEFAULT_NOT_FOUND_RETRY_DELAY : notFoundRetryDelay;
        this.notFoundRetryAttempts = Math.max(0, notFoundRetryAttempts);
    }

    @Incoming(INCOMING_CHANNEL)
    public CompletionStage<Void> consume(Message<String> message) {
        Objects.requireNonNull(message, "message must not be null");
        return Uni.createFrom().item(() -> parseEnvelope(message.getPayload()))
            .runSubscriptionOn(Infrastructure.getDefaultExecutor())
            .onItem().transformToUni(this::admitCompletion)
            .replaceWithVoid()
            .subscribeAsCompletionStage()
            .thenCompose(ignored -> message.ack())
            .exceptionallyCompose(failure -> {
                if (isNotFoundFailure(failure)) {
                    AwaitCompletionMetrics.recordDroppedCompletion("kafka", "not_found");
                    LOG.warnf(
                        failure,
                        "Dropping unresolved Kafka await completion after interaction lookup retries");
                    return message.ack();
                }
                if (AwaitCompletionAdmissionFailures.isDeterministic(failure)) {
                    String reason = AwaitCompletionAdmissionFailures.reason(failure);
                    AwaitCompletionMetrics.recordDroppedCompletion("kafka", reason);
                    LOG.warnf(failure, "Dropping deterministic Kafka await completion message: reason=%s", reason);
                    return message.ack();
                }
                return message.nack(failure);
            });
    }

    private Uni<AwaitCompletionResult> admitCompletion(KafkaAwaitCompletionEnvelope envelope) {
        Uni<AwaitCompletionResult> admission = Uni.createFrom().deferred(() -> executionService.completeAwaitInteraction(new AwaitCompletionCommand(
                envelope.tenantId(),
                envelope.interactionId(),
                envelope.correlationId(),
                envelope.resumeToken(),
                envelope.idempotencyKey(),
                envelope.responsePayload(),
                envelope.actor(),
                System.currentTimeMillis())));
        if (notFoundRetryAttempts <= 0) {
            return admission;
        }
        return admission
            .onFailure(KafkaAwaitCompletionConsumer::isNotFoundFailure)
            .retry()
            .withBackOff(notFoundRetryDelay, notFoundRetryDelay)
            .atMost(notFoundRetryAttempts);
    }

    private static boolean isNotFoundFailure(Throwable failure) {
        Throwable current = failure;
        while (current != null && current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current instanceof AwaitInteractionNotFoundException;
    }

    private static KafkaAwaitCompletionEnvelope parseEnvelope(String payload) {
        try {
            return PipelineJson.mapper().readValue(payload, KafkaAwaitCompletionEnvelope.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Kafka await completion envelope", e);
        }
    }
}
