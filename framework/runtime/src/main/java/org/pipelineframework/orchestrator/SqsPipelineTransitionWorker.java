package org.pipelineframework.orchestrator;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.invocation.TransportBoundaryDescriptor;
import org.pipelineframework.invocation.TransportBoundaryInvocation;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * SQS request/reply client adapter for transition workers.
 */
@ApplicationScoped
public class SqsPipelineTransitionWorker implements PipelineTransitionWorker, TransportBoundaryInvocation {

    private static final Logger LOG = Logger.getLogger(SqsPipelineTransitionWorker.class);
    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final int MAX_RESPONSE_MESSAGES_PER_POLL = 10;
    private static final int RESPONSE_WAIT_TIME_SECONDS = 1;
    private static final TransportBoundaryDescriptor BOUNDARY =
        new TransportBoundaryDescriptor("sqs", "transition-worker.execute");

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    ControlPlaneSecretResolver secretResolver;

    @Inject
    PipelineInvocationRuntime invocationRuntime;

    private volatile SqsClient client;

    public SqsPipelineTransitionWorker() {
    }

    SqsPipelineTransitionWorker(SqsClient client, PipelineOrchestratorConfig orchestratorConfig) {
        this(client, orchestratorConfig, null);
    }

    SqsPipelineTransitionWorker(
        SqsClient client,
        PipelineOrchestratorConfig orchestratorConfig,
        PipelineInvocationRuntime invocationRuntime
    ) {
        this.client = client;
        this.orchestratorConfig = orchestratorConfig;
        this.invocationRuntime = invocationRuntime;
    }

    @Override
    public Uni<TransitionResultEnvelope> executeTransition(TransitionCommandEnvelope command) {
        return invocationRuntime().invokeTransportUni(this, () -> blocking(() -> executeBlocking(command)));
    }

    @Override
    public String providerName() {
        return "sqs";
    }

    @Override
    public TransportBoundaryDescriptor transportBoundary() {
        return BOUNDARY;
    }

    @Override
    public Optional<String> startupValidationError(PipelineOrchestratorConfig config) {
        if (!config.workerSqs().isEnabled()) {
            return Optional.empty();
        }
        if (config.workerSqs().responseQueueUrl().filter(value -> !value.isBlank()).isEmpty()) {
            return Optional.of("pipeline.orchestrator.worker.sqs.response-queue-url is required when "
                + "pipeline.orchestrator.worker.sqs.request-queue-url is configured");
        }
        Optional<String> secretError = WorkerSecretSupport.validationError(
            config.workerSqs().sharedSecret(),
            config.workerSqs().sharedSecretRef(),
            "pipeline.orchestrator.worker.sqs.shared-secret",
            "pipeline.orchestrator.worker.sqs.shared-secret-ref",
            "pipeline.orchestrator.worker.sqs.request-queue-url is configured");
        if (secretError.isPresent()) {
            return secretError;
        }
        if (visibilityTimeoutSeconds(config) < requestTimeoutSeconds(config)) {
            return Optional.of("pipeline.orchestrator.worker.sqs.visibility-timeout must be greater than or equal to "
                + "pipeline.orchestrator.worker.sqs.request-timeout");
        }
        return Optional.empty();
    }

    private TransitionResultEnvelope executeBlocking(TransitionCommandEnvelope command) {
        String requestId = UUID.randomUUID().toString();
        sendRequest(requestId, command);
        return awaitResponse(requestId, command);
    }

    private void sendRequest(String requestId, TransitionCommandEnvelope command) {
        String commandJson;
        try {
            commandJson = JSON.writeValueAsString(command);
        } catch (Exception e) {
            throw new IllegalStateException("Failed encoding SQS transition worker command", e);
        }
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            sharedSecret(),
            SqsTransitionWorkerProtocol.SIGNATURE_METHOD,
            SqsTransitionWorkerProtocol.REQUEST_SIGNATURE_PATH,
            timestamp,
            nonce,
            SqsTransitionWorkerProtocol.signedBytes(requestId, commandJson));
        SqsTransitionWorkerRequest request = new SqsTransitionWorkerRequest(
            requestId,
            SqsTransitionWorkerProtocol.PROTOCOL_VERSION,
            SqsTransitionWorkerProtocol.PAYLOAD_ENCODING,
            commandJson,
            timestamp,
            nonce,
            signature);
        try {
            sqsClient().sendMessage(SendMessageRequest.builder()
                .queueUrl(requestQueueUrl())
                .messageBody(JSON.writeValueAsString(request))
                .build());
        } catch (Exception e) {
            throw new TransitionWorkerFailureException(
                "Failed sending SQS transition worker request for execution " + command.executionId(), e);
        }
    }

    private TransitionResultEnvelope awaitResponse(String requestId, TransitionCommandEnvelope command) {
        Instant deadline = Instant.now().plus(orchestratorConfig.workerSqs().requestTimeout());
        while (Instant.now().isBefore(deadline)) {
            ReceiveMessageRequest receive = ReceiveMessageRequest.builder()
                .queueUrl(responseQueueUrl())
                .maxNumberOfMessages(MAX_RESPONSE_MESSAGES_PER_POLL)
                .waitTimeSeconds(RESPONSE_WAIT_TIME_SECONDS)
                .visibilityTimeout(visibilityTimeoutSeconds())
                .build();
            List<Message> messages = sqsClient().receiveMessage(receive).messages();
            if (messages.isEmpty()) {
                continue;
            }
            for (Message message : messages) {
                SqsTransitionWorkerResponse response;
                try {
                    response = decodeResponseMessage(message, command);
                } catch (TransitionWorkerFailureException e) {
                    LOG.warnf(e, "Dropping malformed SQS transition worker response id=%s", message.messageId());
                    deleteMessage(responseQueueUrl(), message.receiptHandle());
                    continue;
                }
                if (!requestId.equals(response.requestId())) {
                    restoreVisibility(message);
                    continue;
                }
                try {
                    verifyResponse(response, command);
                    TransitionResultEnvelope result = decodeResult(response, command);
                    deleteMessage(responseQueueUrl(), message.receiptHandle());
                    return result;
                } catch (TransitionWorkerFailureException e) {
                    deleteMessage(responseQueueUrl(), message.receiptHandle());
                    throw e;
                }
            }
        }
        throw new TransitionWorkerFailureException(
            "Timed out waiting for SQS transition worker response for execution " + command.executionId());
    }

    private SqsTransitionWorkerResponse decodeResponseMessage(Message message, TransitionCommandEnvelope command) {
        try {
            SqsTransitionWorkerResponse response = JSON.readValue(message.body(), SqsTransitionWorkerResponse.class);
            if (response == null) {
                throw new TransitionWorkerFailureException(
                    "SQS transition worker returned empty response for execution " + command.executionId());
            }
            return response;
        } catch (Exception e) {
            if (e instanceof TransitionWorkerFailureException failure) {
                throw failure;
            }
            throw new TransitionWorkerFailureException(
                "SQS transition worker returned malformed response for execution " + command.executionId(), e);
        }
    }

    private void verifyResponse(SqsTransitionWorkerResponse response, TransitionCommandEnvelope command) {
        if (!SqsTransitionWorkerProtocol.PROTOCOL_VERSION.equals(response.protocolVersion())
            || !SqsTransitionWorkerProtocol.PAYLOAD_ENCODING.equals(response.resultEncoding())) {
            throw new TransitionWorkerFailureException(
                "SQS transition worker returned unsupported protocol envelope for execution "
                    + command.executionId());
        }
        verifySignature(
            response.requestId(),
            response.resultEnvelope(),
            response.timestamp(),
            response.nonce(),
            response.signature(),
            SqsTransitionWorkerProtocol.RESPONSE_SIGNATURE_PATH,
            command.executionId());
    }

    private TransitionResultEnvelope decodeResult(SqsTransitionWorkerResponse response, TransitionCommandEnvelope command) {
        try {
            return JSON.readValue(response.resultEnvelope(), TransitionResultEnvelope.class);
        } catch (Exception e) {
            throw new TransitionWorkerFailureException(
                "SQS transition worker returned malformed result envelope for execution " + command.executionId(), e);
        }
    }

    private void verifySignature(
        String requestId,
        String envelopeJson,
        String timestamp,
        String nonce,
        String signature,
        String signaturePath,
        String executionId
    ) {
        long timestampEpochMs;
        try {
            timestampEpochMs = TransitionWorkerSignature.parseTimestamp(timestamp);
        } catch (IllegalArgumentException e) {
            throw new TransitionWorkerFailureException(
                "SQS transition worker response has invalid signature timestamp for execution " + executionId, e);
        }
        long now = System.currentTimeMillis();
        long toleranceMs = Math.max(0L, orchestratorConfig.workerSqs().signatureTolerance().toMillis());
        if (Math.abs(now - timestampEpochMs) > toleranceMs) {
            throw new TransitionWorkerFailureException(
                "SQS transition worker response signature timestamp is outside tolerance for execution " + executionId);
        }
        String expected = TransitionWorkerSignature.sign(
            sharedSecret(),
            SqsTransitionWorkerProtocol.SIGNATURE_METHOD,
            signaturePath,
            timestamp,
            nonce,
            SqsTransitionWorkerProtocol.signedBytes(requestId, envelopeJson));
        if (!TransitionWorkerSignature.matches(expected, signature)) {
            throw new TransitionWorkerFailureException(
                "SQS transition worker response signature mismatch for execution " + executionId);
        }
    }

    private void deleteMessage(String queueUrl, String receiptHandle) {
        if (receiptHandle == null || receiptHandle.isBlank()) {
            return;
        }
        sqsClient().deleteMessage(DeleteMessageRequest.builder()
            .queueUrl(queueUrl)
            .receiptHandle(receiptHandle)
            .build());
    }

    private void restoreVisibility(Message message) {
        if (message == null || message.receiptHandle() == null || message.receiptHandle().isBlank()) {
            return;
        }
        sqsClient().changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
            .queueUrl(responseQueueUrl())
            .receiptHandle(message.receiptHandle())
            .visibilityTimeout(0)
            .build());
    }

    private String requestQueueUrl() {
        return orchestratorConfig.workerSqs().requestQueueUrl()
            .filter(url -> !url.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.orchestrator.worker.sqs.request-queue-url is required for SQS transition worker"));
    }

    private String responseQueueUrl() {
        return orchestratorConfig.workerSqs().responseQueueUrl()
            .filter(url -> !url.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.orchestrator.worker.sqs.response-queue-url is required for SQS transition worker"));
    }

    private String sharedSecret() {
        return WorkerSecretSupport.resolve(
            orchestratorConfig.workerSqs().sharedSecret(),
            orchestratorConfig.workerSqs().sharedSecretRef(),
            secretResolver,
            "pipeline.orchestrator.worker.sqs.shared-secret",
            "pipeline.orchestrator.worker.sqs.shared-secret-ref");
    }

    private PipelineInvocationRuntime invocationRuntime() {
        if (invocationRuntime == null) {
            throw new IllegalStateException("PipelineInvocationRuntime was not injected into "
                + "SqsPipelineTransitionWorker.invocationRuntime");
        }
        return invocationRuntime;
    }

    private int visibilityTimeoutSeconds() {
        long seconds = Math.max(1L, visibilityTimeoutSeconds(orchestratorConfig));
        return (int) Math.min(Integer.MAX_VALUE, seconds);
    }

    private static long visibilityTimeoutSeconds(PipelineOrchestratorConfig config) {
        return Math.max(1L, config.workerSqs().visibilityTimeout().toSeconds());
    }

    private static long requestTimeoutSeconds(PipelineOrchestratorConfig config) {
        return Math.max(1L, config.workerSqs().requestTimeout().toSeconds());
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
            } finally {
                client = null;
            }
        }
    }

    private static <T> Uni<T> blocking(Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }
}
