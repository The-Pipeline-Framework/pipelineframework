package org.pipelineframework.orchestrator;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * SQS-backed work dispatcher for queue-async orchestration.
 */
@ApplicationScoped
public class SqsWorkDispatcher implements WorkDispatcher {
    private static final Logger LOG = Logger.getLogger(SqsWorkDispatcher.class);

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

    /**
     * Package-private constructor used for explicit wiring and tests; initializes the dispatcher with the given SQS client, configuration, and optional event hook.
     *
     * @param client the preconfigured SqsClient to use for sending messages
     * @param orchestratorConfig configuration values for the pipeline orchestrator (used to resolve queue URL, region, and endpoint overrides)
     * @param executionWorkEvent CDI event used for local loopback delivery of ExecutionWorkItem instances (may be null if loopback is not required)
     */
    SqsWorkDispatcher(SqsClient client, PipelineOrchestratorConfig orchestratorConfig, Event<ExecutionWorkItem> executionWorkEvent) {
        this.client = client;
        this.orchestratorConfig = orchestratorConfig;
        this.executionWorkEvent = executionWorkEvent;
    }

    /**
     * Provides the provider identifier for this work dispatcher.
     *
     * @return the provider identifier "sqs".
     */
    @Override
    public String providerName() {
        return "sqs";
    }

    @Override
    public int priority() {
        return -1000;
    }

    /**
     * Validates that the orchestrator configuration contains a non-blank SQS queue URL.
     *
     * @param config the pipeline orchestrator configuration to validate; may be {@code null}
     * @return an {@link Optional} containing an error message if the SQS queue URL is missing or blank; otherwise an empty {@link Optional}
     */
    @Override
    public Optional<String> startupValidationError(PipelineOrchestratorConfig config) {
        if (config == null || config.queueUrl().isEmpty() || config.queueUrl().get().isBlank()) {
            return Optional.of("pipeline.orchestrator.queue-url must be configured when dispatcher-provider=sqs.");
        }
        return Optional.empty();
    }

    /**
     * Enqueues the given work item for immediate dispatch.
     *
     * @param item the work item to enqueue
     * @return a Uni that completes when the enqueue operation has finished
     */
    @Override
    public Uni<Void> enqueueNow(ExecutionWorkItem item) {
        return enqueue(item, Duration.ZERO);
    }

    /**
     * Schedules a work item to be dispatched after the specified delay.
     *
     * @param item  the work item to enqueue
     * @param delay the delay before dispatch; if `null`, treated as zero duration
     * @return a Uni that completes with no value when the work item has been queued for dispatch
     */
    @Override
    public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
        Duration normalizedDelay = delay == null ? Duration.ZERO : delay;
        return enqueue(item, normalizedDelay);
    }

    /**
     * Send the given work item to the configured SQS queue and, when configured and not delayed, trigger a local loopback.
     *
     * @param item  the execution work item to enqueue
     * @param delay the requested delivery delay; converted to seconds and clamped to the SQS range 0–900
     * @return      a Uni that completes when the SQS send has finished (and when the local loopback has finished if one is performed)
     * @throws IllegalStateException if the orchestrator queue URL is not configured or is blank
     * Note: failures from the local loopback path are logged and suppressed after a successful SQS send.
     */
    private Uni<Void> enqueue(ExecutionWorkItem item, Duration delay) {
        String queueUrl = orchestratorConfig.queueUrl()
            .filter(url -> !url.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.orchestrator.queue-url must be configured when dispatcher-provider=sqs."));
        int delaySeconds = clampDelaySeconds(delay);
        String messageBody = toMessage(item);
        Uni<Void> sendUni = blocking(() -> {
            sqsClient().sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .delaySeconds(delaySeconds)
                .messageBody(messageBody)
                .build());
            return null;
        }).replaceWithVoid();

        if (!orchestratorConfig.sqs().localLoopback() || executionWorkEvent == null || delaySeconds > 0) {
            return sendUni;
        }
        return sendUni.chain(() -> localLoopback(item)
            .onFailure().invoke(error -> LOG.debug("Ignoring local loopback failure after SQS send success.", error))
            .onFailure().recoverWithNull()
            .replaceWithVoid());
    }

    /**
     * Triggers a local asynchronous dispatch of the given work item when local loopback is enabled.
     *
     * If loopback is enabled and an `executionWorkEvent` is available, fires the event asynchronously
     * and returns a Uni that completes when the event's CompletionStage completes; otherwise returns
     * a Uni that is already completed.
     *
     * @param item the work item to dispatch locally
     * @return a Uni that completes after the local dispatch completes (or immediately when loopback is disabled or no event is available)
     */
    private Uni<Void> localLoopback(ExecutionWorkItem item) {
        if (!orchestratorConfig.sqs().localLoopback() || executionWorkEvent == null) {
            return Uni.createFrom().voidItem();
        }
        CompletionStage<ExecutionWorkItem> stage = executionWorkEvent.fireAsync(item);
        return Uni.createFrom().completionStage(() -> stage).replaceWithVoid();
    }

    /**
     * Clamp a duration to an integer number of seconds in the range 0–900.
     *
     * @param delay the duration to clamp; negative durations are treated as zero
     * @return the duration expressed in whole seconds, clamped to be between 0 and 900 inclusive
     */
    private static int clampDelaySeconds(Duration delay) {
        long seconds = Math.max(0L, delay.toSeconds());
        return (int) Math.min(900L, seconds);
    }

    /**
     * Serialize an ExecutionWorkItem to a JSON string for sending to SQS.
     *
     * @param item the execution work item to serialize
     * @return a JSON representation of the provided work item
     * @throws IllegalStateException if serialization fails
     */
    private static String toMessage(ExecutionWorkItem item) {
        try {
            return PipelineJson.mapper().writeValueAsString(item);
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing execution work item for SQS dispatch.", e);
        }
    }

    /**
     * Lazily creates and caches an SqsClient configured from orchestrator settings.
     *
     * Uses a thread-safe double-checked pattern to initialize a single SqsClient instance;
     * when created, applies optional region and endpointOverride values from the configured
     * SQS settings if they are present and non-blank.
     *
     * @return the cached SqsClient instance (newly created if not already initialized)
     */
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

    /**
     * Closes and clears the cached SQS client if one exists.
     *
     * This method is safe to call multiple times and from concurrent threads; it attempts to close the current
     * client, logs failures at debug level, and ensures the cached reference is cleared.
     */
    @PreDestroy
    void closeClient() {
        SqsClient active = client;
        if (active == null) {
            return;
        }
        synchronized (this) {
            active = client;
            if (active == null) {
                return;
            }
            try {
                active.close();
            } catch (Exception e) {
                LOG.debug("Failed closing SQS client during shutdown.", e);
            } finally {
                client = null;
            }
        }
    }

    /**
     * Executes a blocking supplier on the default reactive executor and exposes its result as a Uni.
     *
     * @param <T> the type produced by the supplier
     * @param supplier a blocking supplier whose result will be emitted by the returned Uni
     * @return a Uni that emits the supplier's produced value when the supplier completes
     */
    private static <T> Uni<T> blocking(Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }
}
