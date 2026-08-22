package org.pipelineframework.orchestrator;

import org.pipelineframework.orchestrator.worker.PipelineWorkerCapability;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.invocation.TransportBoundaryDescriptor;
import org.pipelineframework.invocation.TransportBoundaryInvocation;

/**
 * REST client adapter for transition workers.
 */
@ApplicationScoped
public class RestPipelineTransitionWorker implements PipelineTransitionWorker, TransportBoundaryInvocation {

    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final Logger LOG = Logger.getLogger(RestPipelineTransitionWorker.class);
    private static final AtomicInteger THREAD_SEQUENCE = new AtomicInteger();
    private static final TransportBoundaryDescriptor BOUNDARY =
        new TransportBoundaryDescriptor("rest", "transition-worker.execute");

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    ControlPlaneSecretResolver secretResolver;

    @Inject
    PipelineInvocationRuntime invocationRuntime;

    private final ExecutorService blockingExecutor = Executors.newCachedThreadPool(task -> {
        Thread thread = new Thread(task, "rest-transition-worker-client-" + THREAD_SEQUENCE.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });
    private final ScheduledExecutorService deadlineWarningExecutor = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "rest-transition-worker-deadline-warning-" + THREAD_SEQUENCE.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });

    private volatile HttpClient httpClient;

    public RestPipelineTransitionWorker() {
        this(null);
    }

    RestPipelineTransitionWorker(HttpClient httpClient) {
        this(httpClient, null);
    }

    RestPipelineTransitionWorker(HttpClient httpClient, PipelineInvocationRuntime invocationRuntime) {
        this.httpClient = httpClient;
        this.invocationRuntime = invocationRuntime;
    }

    @Override
    public Uni<TransitionResultEnvelope> executeTransition(TransitionCommandEnvelope command) {
        return invocationRuntime().invokeTransportUni(this, () ->
            Uni.createFrom().deferred(() -> executeRequest(command)));
    }

    private Uni<TransitionResultEnvelope> executeRequest(TransitionCommandEnvelope command) {
        Duration deadline = orchestratorConfig.workerRest().requestTimeout();
        long deadlineMillis = Math.max(1L, deadline.toMillis());
        long startedAtNanos = System.nanoTime();
        String target = workerTarget();
        ScheduledFuture<?> warning = scheduleDeadlineWarning(command, target, deadlineMillis);
        return Uni.createFrom().completionStage(() -> CompletableFuture.supplyAsync(() -> request(command), blockingExecutor)
                .thenCompose(request -> httpClient().sendAsync(request, HttpResponse.BodyHandlers.ofString()))
                .thenCompose(response -> CompletableFuture.supplyAsync(
                    () -> decodeResponse(response, command),
                    blockingExecutor)))
            .onItemOrFailure().invoke((ignored, failure) -> warning.cancel(false))
            .onFailure().transform(failure -> classifyFailure(failure, target, startedAtNanos, deadlineMillis));
    }

    private ScheduledFuture<?> scheduleDeadlineWarning(
        TransitionCommandEnvelope command,
        String target,
        long deadlineMillis
    ) {
        long warningDelayMillis = Math.max(1L, deadlineMillis * 3L / 4L);
        return deadlineWarningExecutor.schedule(() -> LOG.warnf(
            "event=remote_transition_deadline_warning executionId=%s attempt=%d transitionKey=%s "
                + "protocol=rest target=%s elapsedMs=%d deadlineMs=%d state=running",
            command.executionId(),
            command.attempt(),
            command.transitionKey(),
            target,
            warningDelayMillis,
            deadlineMillis), warningDelayMillis, TimeUnit.MILLISECONDS);
    }

    private Throwable classifyFailure(
        Throwable failure,
        String target,
        long startedAtNanos,
        long deadlineMillis
    ) {
        Throwable unwrapped = unwrapFailure(failure);
        if (unwrapped instanceof HttpTimeoutException) {
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
            return new RemoteTransitionOutcomeUnknownException(
                "rest",
                target,
                elapsedMillis,
                deadlineMillis,
                unwrapped);
        }
        return unwrapped;
    }

    /**
     * Fetches worker release capabilities from the REST worker.
     *
     * @return remote worker capabilities
     */
    public Uni<PipelineWorkerCapability> capabilities() {
        return Uni.createFrom().completionStage(() -> CompletableFuture.supplyAsync(this::capabilitiesRequest, blockingExecutor)
            .thenCompose(request -> httpClient().sendAsync(request, HttpResponse.BodyHandlers.ofString()))
            .thenCompose(response -> CompletableFuture.supplyAsync(
                () -> decodeCapabilitiesResponse(response),
                blockingExecutor)))
            .onFailure().transform(this::unwrapFailure);
    }

    @Override
    public String providerName() {
        return "rest";
    }

    @Override
    public TransportBoundaryDescriptor transportBoundary() {
        return BOUNDARY;
    }

    @Override
    public Optional<String> startupValidationError(PipelineOrchestratorConfig config) {
        if (!config.workerRest().isEnabled()) {
            return Optional.empty();
        }
        Optional<String> secretError = WorkerSecretSupport.validationError(
            config.workerRest().sharedSecret(),
            config.workerRest().sharedSecretRef(),
            "pipeline.orchestrator.worker.rest.shared-secret",
            "pipeline.orchestrator.worker.rest.shared-secret-ref",
            "pipeline.orchestrator.worker.rest.base-url is configured");
        if (secretError.isPresent()) {
            return secretError;
        }
        return config.workerRest().baseUrl()
            .flatMap(value -> {
                try {
                    URI uri = URI.create(value);
                    if (!uri.isAbsolute() || uri.getHost() == null) {
                        return Optional.of("Invalid pipeline.orchestrator.worker.rest.base-url: " + value);
                    }
                    return Optional.empty();
                } catch (IllegalArgumentException e) {
                    return Optional.of("Invalid pipeline.orchestrator.worker.rest.base-url: " + value);
                }
            });
    }

    private TransitionResultEnvelope decodeResponse(HttpResponse<String> response, TransitionCommandEnvelope command) {
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new TransitionWorkerFailureException(
                "REST transition worker returned HTTP " + response.statusCode()
                    + " for execution " + command.executionId()
                    + " with response body omitted");
        }
        try {
            TransitionResultEnvelope envelope = JSON.readValue(response.body(), TransitionResultEnvelope.class);
            if (envelope == null) {
                throw new TransitionWorkerFailureException(
                    "REST transition worker returned an empty result for execution "
                        + command.executionId()
                        + " with HTTP " + response.statusCode()
                        + " and response body omitted");
            }
            return envelope;
        } catch (IOException e) {
            throw new TransitionWorkerFailureException(
                "REST transition worker returned malformed JSON for execution "
                    + command.executionId()
                    + " with HTTP " + response.statusCode()
                    + " and response body omitted",
                e);
        }
    }

    private HttpRequest request(TransitionCommandEnvelope command) {
        try {
            byte[] body = JSON.writeValueAsBytes(command);
            URI uri = workerUri(orchestratorConfig.workerRest().path());
            String timestamp = Instant.now().toString();
            String nonce = UUID.randomUUID().toString();
            String signature = TransitionWorkerSignature.sign(
                sharedSecret(),
                "POST",
                orchestratorConfig.workerRest().path(),
                timestamp,
                nonce,
                body);
            return HttpRequest.newBuilder(uri)
                .timeout(orchestratorConfig.workerRest().requestTimeout())
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
                .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
                .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        } catch (IOException e) {
            throw new IllegalStateException("Failed encoding REST transition worker command", e);
        }
    }

    private HttpRequest capabilitiesRequest() {
        URI uri = workerUri(orchestratorConfig.workerRest().capabilitiesPath());
        byte[] body = new byte[0];
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            sharedSecret(),
            "GET",
            orchestratorConfig.workerRest().capabilitiesPath(),
            timestamp,
            nonce,
            body);
        return HttpRequest.newBuilder(uri)
            .timeout(orchestratorConfig.workerRest().requestTimeout())
            .header("Accept", "application/json")
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature)
            .GET()
            .build();
    }

    private PipelineWorkerCapability decodeCapabilitiesResponse(HttpResponse<String> response) {
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new TransitionWorkerFailureException(
                "REST transition worker capabilities returned HTTP " + response.statusCode()
                    + " with response body omitted");
        }
        try {
            PipelineWorkerCapability capability = JSON.readValue(response.body(), PipelineWorkerCapability.class);
            if (capability == null) {
                throw new TransitionWorkerFailureException(
                    "REST transition worker capabilities returned an empty response");
            }
            return capability;
        } catch (IOException e) {
            throw new TransitionWorkerFailureException(
                "REST transition worker capabilities returned malformed JSON",
                e);
        }
    }

    private URI workerUri(String path) {
        String baseUrl = orchestratorConfig.workerRest().baseUrl()
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.orchestrator.worker.rest.base-url is required for REST transition worker"));
        String normalizedBase = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        return URI.create(normalizedBase + normalizedPath);
    }

    private String workerTarget() {
        URI uri = workerUri(orchestratorConfig.workerRest().path());
        String host = uri.getHost() == null ? "<unknown>" : uri.getHost();
        return uri.getScheme() + "://" + host + (uri.getPort() < 0 ? "" : ":" + uri.getPort()) + uri.getPath();
    }

    private String sharedSecret() {
        return WorkerSecretSupport.resolve(
            orchestratorConfig.workerRest().sharedSecret(),
            orchestratorConfig.workerRest().sharedSecretRef(),
            secretResolver,
            "pipeline.orchestrator.worker.rest.shared-secret",
            "pipeline.orchestrator.worker.rest.shared-secret-ref");
    }

    private HttpClient httpClient() {
        HttpClient client = httpClient;
        if (client != null) {
            return client;
        }
        synchronized (this) {
            if (httpClient == null) {
                httpClient = HttpClient.newBuilder()
                    .connectTimeout(orchestratorConfig.workerRest().connectTimeout())
                    .build();
            }
            return httpClient;
        }
    }

    private PipelineInvocationRuntime invocationRuntime() {
        if (invocationRuntime == null) {
            throw new IllegalStateException("PipelineInvocationRuntime was not injected into "
                + "RestPipelineTransitionWorker.invocationRuntime");
        }
        return invocationRuntime;
    }

    private Throwable unwrapFailure(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return unwrapFailure(failure.getCause());
        }
        return failure;
    }

    @PreDestroy
    void close() {
        deadlineWarningExecutor.shutdownNow();
        blockingExecutor.shutdownNow();
    }
}
