package org.pipelineframework.orchestrator;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import io.quarkus.runtime.StartupEvent;
import org.jboss.logging.Logger;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * Polls SQS-backed work queues when queue async mode is configured without local loopback dispatch.
 */
@ApplicationScoped
public class SqsWorkPoller {

    private static final Logger LOG = Logger.getLogger(SqsWorkPoller.class);
    private static final int MAX_MESSAGES_PER_POLL = 1;
    private static final int WAIT_TIME_SECONDS = 1;
    private static final Duration PROCESS_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration INITIAL_FAILURE_BACKOFF = Duration.ofSeconds(1);
    private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(30);
    private static final Duration EXECUTOR_SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration DELETE_RETRY_BASE_DELAY = Duration.ofMillis(250);
    private static final Duration DELETE_RETRY_MAX_DELAY = Duration.ofSeconds(2);
    private static final int DELETE_MAX_ATTEMPTS = 3;
    private static final int VISIBILITY_EXTENSION_SECONDS = 30;
    private static final long VISIBILITY_EXTENSION_PERIOD_SECONDS = 20L;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineExecutionService pipelineExecutionService;

    private volatile SqsClient client;
    private volatile ExecutorService pollExecutor;
    private volatile ScheduledExecutorService visibilityExecutor;
    private volatile Future<?> pollFuture;
    private volatile boolean running;
    private final AtomicInteger consecutivePollFailures = new AtomicInteger();

    /**
     * Default constructor for CDI.
     */
    public SqsWorkPoller() {
    }

    SqsWorkPoller(
        PipelineOrchestratorConfig orchestratorConfig,
        PipelineExecutionService pipelineExecutionService,
        SqsClient client
    ) {
        this.orchestratorConfig = orchestratorConfig;
        this.pipelineExecutionService = pipelineExecutionService;
        this.client = client;
    }

    void onStartup(@Observes StartupEvent event) {
        if (!enabled()) {
            return;
        }
        ensureExecutors();
        running = true;
        pollFuture = pollExecutor.submit(() -> {
            sleep(orchestratorConfig.sqs().pollStartDelay());
            pollLoop();
        });
        LOG.infof("SQS work poller enabled for queueUrl=%s", orchestratorConfig.queueUrl().orElse("<missing>"));
    }

    @PreDestroy
    void shutdown() {
        running = false;
        Future<?> activePollFuture = pollFuture;
        if (activePollFuture != null) {
            activePollFuture.cancel(true);
        }
        shutdownExecutor(pollExecutor, "poll");
        pollExecutor = null;
        shutdownExecutor(visibilityExecutor, "visibility-extension");
        visibilityExecutor = null;
        SqsClient activeClient = client;
        if (activeClient == null) {
            return;
        }
        try {
            activeClient.close();
        } catch (Exception e) {
            LOG.debug("Failed closing SQS client for work poller.", e);
        } finally {
            client = null;
        }
    }

    boolean pollOnce() {
        if (!enabled()) {
            return true;
        }
        String queueUrl = orchestratorConfig.queueUrl()
            .filter(url -> !url.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.orchestrator.queue-url must be configured when dispatcher-provider=sqs."));

        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
            .queueUrl(queueUrl)
            .maxNumberOfMessages(MAX_MESSAGES_PER_POLL)
            .waitTimeSeconds(WAIT_TIME_SECONDS)
            .visibilityTimeout(VISIBILITY_EXTENSION_SECONDS)
            .build();
        List<Message> messages;
        try {
            messages = sqsClient().receiveMessage(request).messages();
        } catch (RuntimeException e) {
            LOG.errorf(e, "Failed receiving SQS work items from queueUrl=%s", queueUrl);
            sleepFailureBackoff();
            return false;
        }
        for (Message message : messages) {
            handleMessage(queueUrl, message);
        }
        return true;
    }

    private void pollLoop() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                if (pollOnce()) {
                    consecutivePollFailures.set(0);
                }
            } catch (Exception e) {
                LOG.error("SQS work poll failed.", e);
                sleepFailureBackoff();
            }
        }
    }

    private void handleMessage(String queueUrl, Message message) {
        if (message == null || message.receiptHandle() == null) {
            return;
        }
        if (message.body() == null) {
            LOG.warnf("Dropping SQS work message with null body id=%s", message.messageId());
            deleteMessage(queueUrl, message.receiptHandle());
            return;
        }
        ExecutionWorkItem workItem;
        try {
            workItem = PipelineJson.mapper().readValue(message.body(), ExecutionWorkItem.class);
        } catch (Exception e) {
            LOG.warnf(e, "Dropping malformed SQS work message id=%s", message.messageId());
            deleteMessage(queueUrl, message.receiptHandle());
            return;
        }

        ScheduledFuture<?> visibilityExtension = startVisibilityExtension(queueUrl, message.receiptHandle());
        try {
            pipelineExecutionService.processExecutionWorkItem(workItem)
                .await().atMost(PROCESS_TIMEOUT);
            deleteMessage(queueUrl, message.receiptHandle());
        } catch (RuntimeException e) {
            LOG.errorf(e, "Failed processing SQS work item executionId=%s", workItem.executionId());
        } finally {
            cancelVisibilityExtension(visibilityExtension);
        }
    }

    private void deleteMessage(String queueUrl, String receiptHandle) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
            .queueUrl(queueUrl)
            .receiptHandle(receiptHandle)
            .build();
        for (int attempt = 1; attempt <= DELETE_MAX_ATTEMPTS; attempt++) {
            try {
                sqsClient().deleteMessage(request);
                return;
            } catch (RuntimeException e) {
                if (attempt == DELETE_MAX_ATTEMPTS) {
                    LOG.errorf(e,
                        "Failed deleting SQS work message after %d attempts queueUrl=%s receiptHandle=%s",
                        attempt,
                        queueUrl,
                        receiptHandle);
                    return;
                }
                LOG.warnf(e,
                    "Delete SQS work message attempt %d/%d failed queueUrl=%s receiptHandle=%s",
                    attempt,
                    DELETE_MAX_ATTEMPTS,
                    queueUrl,
                    receiptHandle);
                sleep(deleteRetryDelay(attempt));
            }
        }
    }

    private boolean enabled() {
        return orchestratorConfig != null
            && orchestratorConfig.mode() == OrchestratorMode.QUEUE_ASYNC
            && "sqs".equalsIgnoreCase(orchestratorConfig.dispatcherProvider())
            && !orchestratorConfig.sqs().localLoopback()
            && orchestratorConfig.queueUrl().filter(url -> !url.isBlank()).isPresent();
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

    private void sleepFailureBackoff() {
        int failures = consecutivePollFailures.incrementAndGet();
        long delayMillis = Math.min(
            INITIAL_FAILURE_BACKOFF.toMillis() * (1L << Math.min(10, Math.max(0, failures - 1))),
            MAX_FAILURE_BACKOFF.toMillis());
        sleep(Duration.ofMillis(delayMillis));
    }

    private ScheduledFuture<?> startVisibilityExtension(String queueUrl, String receiptHandle) {
        ScheduledExecutorService activeVisibilityExecutor = visibilityExecutor;
        if (activeVisibilityExecutor == null) {
            return null;
        }
        return activeVisibilityExecutor.scheduleAtFixedRate(() -> {
            try {
                sqsClient().changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .visibilityTimeout(VISIBILITY_EXTENSION_SECONDS)
                    .build());
            } catch (RuntimeException e) {
                LOG.warnf(e,
                    "Failed extending SQS work message visibility queueUrl=%s receiptHandle=%s",
                    queueUrl,
                    receiptHandle);
            }
        }, VISIBILITY_EXTENSION_PERIOD_SECONDS, VISIBILITY_EXTENSION_PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    private static void cancelVisibilityExtension(ScheduledFuture<?> visibilityExtension) {
        if (visibilityExtension != null) {
            visibilityExtension.cancel(true);
        }
    }

    private void ensureExecutors() {
        if (pollExecutor == null) {
            pollExecutor = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "tpf-sqs-work-poller");
                thread.setDaemon(true);
                return thread;
            });
        }
        if (visibilityExecutor == null) {
            visibilityExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "tpf-sqs-visibility-extender");
                thread.setDaemon(true);
                return thread;
            });
        }
    }

    private void shutdownExecutor(ExecutorService executor, String executorName) {
        if (executor == null) {
            return;
        }
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                LOG.warnf("SQS %s executor did not terminate within %s", executorName, EXECUTOR_SHUTDOWN_TIMEOUT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warnf(e, "Interrupted while shutting down SQS %s executor", executorName);
        }
    }

    private static Duration deleteRetryDelay(int attempt) {
        long delayMillis = Math.min(
            DELETE_RETRY_BASE_DELAY.toMillis() * (1L << Math.max(0, attempt - 1)),
            DELETE_RETRY_MAX_DELAY.toMillis());
        return Duration.ofMillis(delayMillis);
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
}
