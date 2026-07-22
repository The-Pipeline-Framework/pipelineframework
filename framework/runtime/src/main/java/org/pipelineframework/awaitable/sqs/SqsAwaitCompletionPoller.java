package org.pipelineframework.awaitable.sqs;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.AwaitCompletionAdmissionFailures;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionMetrics;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * Polls an SQS response queue and admits await completions.
 */
@ApplicationScoped
public class SqsAwaitCompletionPoller {

    private static final Logger LOG = Logger.getLogger(SqsAwaitCompletionPoller.class);
    private static final Duration DEFAULT_POLL_START_DELAY = Duration.ZERO;
    private static final Duration DEFAULT_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_COMPLETION_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration INITIAL_FAILURE_BACKOFF = Duration.ofSeconds(1);
    private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(30);
    private static final Duration EXECUTOR_SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private static final int DEFAULT_WAIT_TIME_SECONDS = 1;
    private static final int DEFAULT_MAX_MESSAGES = 1;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineExecutionService executionService;

    private volatile SqsClient client;
    private volatile ExecutorService pollExecutor;
    private volatile Future<?> pollFuture;
    private volatile boolean running;
    private final AtomicInteger consecutivePollFailures = new AtomicInteger();

    public SqsAwaitCompletionPoller() {
    }

    SqsAwaitCompletionPoller(
        PipelineOrchestratorConfig orchestratorConfig,
        PipelineExecutionService executionService,
        SqsClient client
    ) {
        this.orchestratorConfig = orchestratorConfig;
        this.executionService = executionService;
        this.client = client;
    }

    void onStartup(@Observes StartupEvent event) {
        SqsAwaitPollerConfig config = SqsAwaitPollerConfig.fromRuntime();
        if (!config.enabled()) {
            return;
        }
        ensureExecutor();
        running = true;
        schedulePoll(config.pollStartDelay());
        LOG.infof("SQS await completion poller enabled for responseQueueUrl=%s", config.responseQueueUrl().orElse("<missing>"));
    }

    @PreDestroy
    void shutdown() {
        running = false;
        Future<?> activePollFuture = pollFuture;
        if (activePollFuture != null) {
            activePollFuture.cancel(true);
        }
        shutdownExecutor(pollExecutor);
        pollExecutor = null;
        SqsClient activeClient = client;
        if (activeClient == null) {
            return;
        }
        try {
            activeClient.close();
        } catch (Exception e) {
            LOG.debug("Failed closing SQS client for await completion poller.", e);
        } finally {
            client = null;
        }
    }

    Uni<Boolean> pollOnce(SqsAwaitPollerConfig config) {
        if (!config.enabled()) {
            return Uni.createFrom().item(true);
        }
        ensureExecutor();
        String responseQueueUrl = config.responseQueueUrl()
            .filter(url -> !url.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "tpf.await.sqs.response-queue-url must be configured when tpf.await.sqs.poller.enabled=true."));

        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
            .queueUrl(responseQueueUrl)
            .maxNumberOfMessages(config.maxMessages())
            .waitTimeSeconds(config.waitTimeSeconds())
            .visibilityTimeout(visibilityTimeoutSeconds(config.visibilityTimeout(), "tpf.await.sqs.visibility-timeout"))
            .build();
        return Uni.createFrom().item(() -> sqsClient().receiveMessage(request).messages())
            .runSubscriptionOn(pollExecutor)
            .onFailure().invoke(e -> LOG.errorf(e,
                "Failed receiving SQS await completions from queueUrl=%s", responseQueueUrl))
            .onItem().transformToMulti(messages -> Multi.createFrom().iterable(messages)
                .onItem().transformToUni(message -> handleMessage(responseQueueUrl, message, config))
                .merge(config.maxMessages()))
            .collect().asList()
            .replaceWith(true);
    }

    private void pollLoop() {
        if (!running || Thread.currentThread().isInterrupted()) {
            return;
        }
        pollOnce(SqsAwaitPollerConfig.fromRuntime()).subscribe().with(
            ignored -> {
                consecutivePollFailures.set(0);
                schedulePoll(Duration.ZERO);
            },
            failure -> schedulePoll(failureBackoff()));
    }

    private Uni<Void> handleMessage(String queueUrl, Message message, SqsAwaitPollerConfig config) {
        if (message == null || message.receiptHandle() == null) {
            return Uni.createFrom().voidItem();
        }
        if (message.body() == null) {
            LOG.warnf("Leaving SQS await completion message with null body for queue redrive id=%s", message.messageId());
            return Uni.createFrom().voidItem();
        }
        SqsAwaitCompletionEnvelope envelope;
        try {
            envelope = PipelineJson.mapper().readValue(message.body(), SqsAwaitCompletionEnvelope.class);
        } catch (Exception e) {
            LOG.warnf(e, "Leaving malformed SQS await completion message for queue redrive id=%s", message.messageId());
            return Uni.createFrom().voidItem();
        }

        return executionService.completeAwaitInteraction(new AwaitCompletionCommand(
                envelope.tenantId(),
                envelope.interactionId(),
                envelope.correlationId(),
                envelope.resumeToken(),
                envelope.idempotencyKey(),
                envelope.responsePayload(),
                envelope.actor(),
                System.currentTimeMillis()))
            .ifNoItem().after(config.completionTimeout()).fail()
            .onItem().transformToUni(ignored -> deleteMessageSafely(queueUrl, message.receiptHandle(), "processed"))
            .onFailure().recoverWithUni(failure -> handleAdmissionFailure(
                queueUrl,
                message.receiptHandle(),
                envelope,
                failure))
            .replaceWithVoid();
    }

    private Uni<Void> handleAdmissionFailure(
        String queueUrl,
        String receiptHandle,
        SqsAwaitCompletionEnvelope envelope,
        Throwable failure
    ) {
        if (AwaitCompletionAdmissionFailures.isDeterministic(failure)) {
            String reason = AwaitCompletionAdmissionFailures.reason(failure);
            AwaitCompletionMetrics.recordDroppedCompletion("sqs", reason);
            LOG.warnf(failure, "Dropping deterministic SQS await completion message: reason=%s", reason);
            return deleteMessageSafely(queueUrl, receiptHandle, "deterministic-" + reason);
        }
        LOG.errorf(failure, "Failed admitting SQS await completion interactionId=%s correlationId=%s",
            envelope.interactionId(),
            envelope.correlationId());
        return Uni.createFrom().voidItem();
    }

    private Uni<Void> deleteMessageSafely(String queueUrl, String receiptHandle, String disposition) {
        return Uni.createFrom().item(() -> {
            sqsClient().deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build());
            return true;
        })
            .runSubscriptionOn(pollExecutor)
            .onFailure().invoke(e -> LOG.warnf(e,
                "Failed deleting SQS await completion message after %s disposition", disposition))
            .onFailure().recoverWithItem(false)
            .replaceWithVoid();
    }

    private SqsClient sqsClient() {
        SqsClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            if (client == null) {
                var builder = SqsClient.builder();
                builder.httpClientBuilder(UrlConnectionHttpClient.builder());
                if (orchestratorConfig != null) {
                    orchestratorConfig.sqs().region()
                        .filter(region -> !region.isBlank())
                        .ifPresent(region -> builder.region(Region.of(region)));
                    orchestratorConfig.sqs().endpointOverride()
                        .filter(endpoint -> !endpoint.isBlank())
                        .ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
                }
                client = builder.build();
            }
            return client;
        }
    }

    private synchronized void ensureExecutor() {
        if (pollExecutor == null) {
            pollExecutor = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "tpf-sqs-await-completion-poller");
                thread.setDaemon(true);
                return thread;
            });
        }
    }

    private void schedulePoll(Duration delay) {
        if (!running) {
            return;
        }
        ExecutorService executor = pollExecutor;
        if (executor == null) {
            return;
        }
        try {
            pollFuture = executor.submit(() -> {
                sleep(delay);
                pollLoop();
            });
        } catch (RejectedExecutionException rejected) {
            LOG.debug("SQS await completion poll scheduling rejected during shutdown", rejected);
        }
    }

    private Duration failureBackoff() {
        int failures = consecutivePollFailures.incrementAndGet();
        long delayMillis = Math.min(
            INITIAL_FAILURE_BACKOFF.toMillis() * (1L << Math.min(10, Math.max(0, failures - 1))),
            MAX_FAILURE_BACKOFF.toMillis());
        return Duration.ofMillis(delayMillis);
    }

    private static void shutdownExecutor(ExecutorService executor) {
        if (executor == null) {
            return;
        }
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                LOG.warnf("SQS await completion poller executor did not terminate within %s", EXECUTOR_SHUTDOWN_TIMEOUT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warnf(e, "Interrupted while shutting down SQS await completion poller executor");
        }
    }

    private static void sleep(Duration duration) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            return;
        }
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int visibilityTimeoutSeconds(Duration visibilityTimeout, String configKey) {
        long seconds = visibilityTimeout.toSeconds();
        if (seconds < 0 || seconds > 43_200) {
            throw new IllegalArgumentException(configKey + " must be between PT0S and PT43200S.");
        }
        return (int) seconds;
    }

    record SqsAwaitPollerConfig(
        boolean enabled,
        Optional<String> responseQueueUrl,
        Duration pollStartDelay,
        Duration visibilityTimeout,
        Duration completionTimeout,
        int waitTimeSeconds,
        int maxMessages
    ) {
        static SqsAwaitPollerConfig fromRuntime() {
            var config = ConfigProvider.getConfig();
            return new SqsAwaitPollerConfig(
                config.getOptionalValue("tpf.await.sqs.poller.enabled", Boolean.class).orElse(false),
                config.getOptionalValue("tpf.await.sqs.response-queue-url", String.class),
                config.getOptionalValue("tpf.await.sqs.poll-start-delay", Duration.class).orElse(DEFAULT_POLL_START_DELAY),
                config.getOptionalValue("tpf.await.sqs.visibility-timeout", Duration.class).orElse(DEFAULT_VISIBILITY_TIMEOUT),
                config.getOptionalValue("tpf.await.sqs.completion-timeout", Duration.class).orElse(DEFAULT_COMPLETION_TIMEOUT),
                Math.max(1, Math.min(20,
                    config.getOptionalValue("tpf.await.sqs.wait-time-seconds", Integer.class).orElse(DEFAULT_WAIT_TIME_SECONDS))),
                Math.max(1, Math.min(10,
                    config.getOptionalValue("tpf.await.sqs.max-messages", Integer.class).orElse(DEFAULT_MAX_MESSAGES))));
        }
    }
}
