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

    /**
     * Package-private constructor that initializes the publisher with a preconfigured SqsClient and orchestrator configuration.
     *
     * @param client             the SqsClient to use for SQS operations (may be provided for testing or custom wiring)
     * @param orchestratorConfig the pipeline orchestrator configuration containing DLQ and SQS settings
     */
    SqsDeadLetterPublisher(SqsClient client, PipelineOrchestratorConfig orchestratorConfig) {
        this.client = client;
        this.orchestratorConfig = orchestratorConfig;
    }

    /**
     * Identifies this dead-letter publisher's provider as Amazon SQS.
     *
     * @return the provider name "sqs"
     */
    @Override
    public String providerName() {
        return "sqs";
    }

    /**
     * Specifies this publisher's selection priority among dead-letter providers.
     *
     * @return the priority value; lower numbers indicate higher precedence (this provider returns -1000)
     */
    @Override
    public int priority() {
        return -1000;
    }

    /**
     * Validates that an SQS dead-letter queue URL is configured in the orchestrator config.
     *
     * @param config the pipeline orchestrator configuration; may be {@code null}
     * @return an {@link Optional} containing an error message if the DLQ URL is missing or blank, {@link Optional#empty()} otherwise
     */
    @Override
    public Optional<String> startupValidationError(PipelineOrchestratorConfig config) {
        if (config == null || config.dlqUrl().isEmpty() || config.dlqUrl().get().isBlank()) {
            return Optional.of("pipeline.orchestrator.dlq-url must be configured when dlq-provider=sqs.");
        }
        return Optional.empty();
    }

    /**
     * Publishes a dead-letter envelope to the configured SQS dead-letter queue.
     *
     * @param envelope the dead-letter envelope to publish
     * @return a Uni that yields no value (Void) after the envelope has been sent to SQS
     */
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
            return null;
        }).replaceWithVoid();
    }

    /**
     * Marks the publisher as shutting down and closes the internal SQS client, releasing its resources.
     *
     * After this method runs, the internal client reference is cleared and further attempts to access the client
     * will fail with an IllegalStateException.
     */
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

    /**
     * Lazily initialize and return the SqsClient used to publish dead-letter messages.
     *
     * Builds a client on first use, applying an optional region and endpoint override from
     * the orchestrator configuration; subsequent calls return the cached instance.
     *
     * @return the initialized SqsClient instance
     * @throws IllegalStateException if the publisher is shutting down
     */
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

    /**
     * Convert a DeadLetterEnvelope into its JSON message body for SQS.
     *
     * @param envelope the dead-letter envelope to serialize
     * @return the JSON representation of the envelope
     * @throws IllegalStateException if serialization fails
     */
    private static String toMessage(DeadLetterEnvelope envelope) {
        try {
            return PipelineJson.mapper().writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing dead-letter envelope for SQS publish.", e);
        }
    }

    /**
     * Execute a blocking Supplier on the Quarkus default worker pool and produce a Uni of its result.
     *
     * @param <T>      the type of the supplied result
     * @param supplier a blocking supplier whose execution will be scheduled on the default worker pool
     * @return         a Uni that emits the value returned by the supplier when execution completes
     */
    private static <T> Uni<T> blocking(Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
}
