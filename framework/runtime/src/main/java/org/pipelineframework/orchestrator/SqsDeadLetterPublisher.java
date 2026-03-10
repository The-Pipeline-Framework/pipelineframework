package org.pipelineframework.orchestrator;

import java.net.URI;
import java.util.Optional;
import java.util.function.Supplier;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * SQS-backed dead-letter publisher for queue-async orchestrator mode.
 */
@ApplicationScoped
public class SqsDeadLetterPublisher implements DeadLetterPublisher {

    private static final Logger LOG = Logger.getLogger(SqsDeadLetterPublisher.class);

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    private volatile SqsClient client;
    private volatile boolean shuttingDown;

    /**
     * Default constructor for CDI.
     */
    public SqsDeadLetterPublisher() {
    }

    SqsDeadLetterPublisher(SqsClient client, PipelineOrchestratorConfig orchestratorConfig) {
        this.client = client;
        this.orchestratorConfig = orchestratorConfig;
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
        if (config == null || config.dlqUrl().isEmpty() || config.dlqUrl().get().isBlank()) {
            return Optional.of("pipeline.orchestrator.dlq-url must be configured when dlq-provider=sqs.");
        }
        return Optional.empty();
    }

    @Override
    public Uni<Void> publish(DeadLetterEnvelope envelope) {
        String queueUrl = orchestratorConfig.dlqUrl()
            .filter(url -> !url.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.orchestrator.dlq-url must be configured when dlq-provider=sqs."));
        String messageBody = toMessage(envelope);
        return blocking(() -> {
            sqsClient().sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build());
            try {
                DeadLetterMetrics.record(providerName(), envelope);
            } catch (RuntimeException metricFailure) {
                LOG.warnf(metricFailure,
                    "DeadLetterMetrics recording failed for execution=%s. Continuing after successful DLQ publish.",
                    envelope.executionId());
            }
            return null;
        }).replaceWithVoid();
    }

    @PreDestroy
    void closeClient() {
        synchronized (this) {
            shuttingDown = true;
            SqsClient active = client;
            if (active == null) {
                return;
            }
            try {
                active.close();
            } catch (Exception e) {
                LOG.debug("Failed closing SQS client for dead-letter publisher.", e);
            } finally {
                client = null;
            }
        }
    }

    private SqsClient sqsClient() {
        if (shuttingDown) {
            throw new IllegalStateException("SqsDeadLetterPublisher is shutting down.");
        }
        SqsClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            if (shuttingDown) {
                throw new IllegalStateException("SqsDeadLetterPublisher is shutting down.");
            }
            if (client == null) {
                var builder = SqsClient.builder();
                builder.httpClientBuilder(UrlConnectionHttpClient.builder());
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

    private static String toMessage(DeadLetterEnvelope envelope) {
        try {
            return PipelineJson.mapper().writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing dead-letter envelope for SQS publish.", e);
        }
    }

    private static <T> Uni<T> blocking(Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
}
