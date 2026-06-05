package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqsPipelineTransitionWorkerTest {

    private final JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();
    private SqsClient client;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.SqsWorkerConfig sqsWorkerConfig;
    private PipelineOrchestratorConfig.SqsConfig sqsConfig;
    private SqsPipelineTransitionWorker worker;
    private AtomicReference<SqsTransitionWorkerRequest> sentRequest;

    @BeforeEach
    void setUp() {
        client = mock(SqsClient.class);
        config = mock(PipelineOrchestratorConfig.class);
        sqsWorkerConfig = mock(PipelineOrchestratorConfig.SqsWorkerConfig.class);
        sqsConfig = mock(PipelineOrchestratorConfig.SqsConfig.class);
        sentRequest = new AtomicReference<>();
        when(config.workerSqs()).thenReturn(sqsWorkerConfig);
        when(config.sqs()).thenReturn(sqsConfig);
        when(sqsWorkerConfig.requestQueueUrl()).thenReturn(Optional.of("https://sqs.local/request"));
        when(sqsWorkerConfig.responseQueueUrl()).thenReturn(Optional.of("https://sqs.local/response"));
        when(sqsWorkerConfig.requestTimeout()).thenReturn(Duration.ofSeconds(2));
        when(sqsWorkerConfig.visibilityTimeout()).thenReturn(Duration.ofSeconds(30));
        when(sqsWorkerConfig.signatureTolerance()).thenReturn(Duration.ofMinutes(2));
        when(sqsWorkerConfig.sharedSecret()).thenReturn(Optional.of("worker-secret"));
        when(sqsWorkerConfig.sharedSecretRef()).thenReturn(Optional.empty());
        when(sqsConfig.region()).thenReturn(Optional.empty());
        when(sqsConfig.endpointOverride()).thenReturn(Optional.empty());
        doAnswer(invocation -> {
            SendMessageRequest request = invocation.getArgument(0);
            sentRequest.set(PipelineJson.mapper()
                .readValue(request.messageBody(), SqsTransitionWorkerRequest.class));
            return null;
        }).when(client).sendMessage(any(SendMessageRequest.class));
        worker = new SqsPipelineTransitionWorker(client, config);
    }

    @Test
    void sendsRequestAndReturnsMatchingSignedResponse() {
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation ->
            ReceiveMessageResponse.builder()
                .messages(responseMessage("receipt-1", sentRequest.get().requestId(),
                    TransitionResultEnvelope.completed(payloadCodec, List.of("ok"))))
                .build());

        TransitionResultEnvelope result = worker.executeTransition(envelope()).await().indefinitely();

        assertEquals(TransitionWorkerOutcome.COMPLETED, result.outcome());
        assertEquals(List.of("ok"), result.decodeOutputItems(payloadCodec));
        assertEquals(SqsTransitionWorkerProtocol.PROTOCOL_VERSION, sentRequest.get().protocolVersion());
        assertEquals(SqsTransitionWorkerProtocol.PAYLOAD_ENCODING, sentRequest.get().commandEncoding());
        assertTrue(sentRequest.get().commandEnvelope().contains("\"transitionKey\":\"exec-1:0:0\""));
        verify(client).sendMessage(argThat((SendMessageRequest request) ->
            request.queueUrl().equals("https://sqs.local/request")));
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.queueUrl().equals("https://sqs.local/response")
                && request.receiptHandle().equals("receipt-1")));
    }

    @Test
    void sendsRequestWithSharedSecretReference() {
        when(sqsWorkerConfig.sharedSecret()).thenReturn(Optional.empty());
        when(sqsWorkerConfig.sharedSecretRef()).thenReturn(Optional.of("sys:tpf.sqs.worker.secret"));
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation ->
            ReceiveMessageResponse.builder()
                .messages(responseMessage("receipt-1", sentRequest.get().requestId(),
                    TransitionResultEnvelope.completed(payloadCodec, List.of("ok"))))
                .build());

        try {
            System.setProperty("tpf.sqs.worker.secret", "worker-secret");
            TransitionResultEnvelope result = worker.executeTransition(envelope()).await().indefinitely();

            assertEquals(TransitionWorkerOutcome.COMPLETED, result.outcome());
            assertTrue(sentRequest.get().signature() != null && !sentRequest.get().signature().isBlank());
        } finally {
            System.clearProperty("tpf.sqs.worker.secret");
        }
    }

    @Test
    void ignoresNonMatchingResponseUntilMatchingResponseArrives() {
        AtomicInteger polls = new AtomicInteger();
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer(invocation -> {
            if (polls.getAndIncrement() == 0) {
                return ReceiveMessageResponse.builder()
                    .messages(responseMessage("receipt-other", "other-request",
                        TransitionResultEnvelope.completed(payloadCodec, List.of("other"))))
                    .build();
            }
            return ReceiveMessageResponse.builder()
                .messages(responseMessage("receipt-match", sentRequest.get().requestId(),
                    TransitionResultEnvelope.completed(payloadCodec, List.of("ok"))))
                .build();
        });

        TransitionResultEnvelope result = worker.executeTransition(envelope()).await().indefinitely();

        assertEquals(List.of("ok"), result.decodeOutputItems(payloadCodec));
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.receiptHandle().equals("receipt-match")));
        verify(client, never()).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.receiptHandle().equals("receipt-other")));
        verify(client).changeMessageVisibility(argThat((ChangeMessageVisibilityRequest request) ->
            request.queueUrl().equals("https://sqs.local/response")
                && request.receiptHandle().equals("receipt-other")
                && request.visibilityTimeout() == 0));
    }

    @Test
    void dropsMalformedResponseMessageAndContinuesPolling() {
        when(sqsWorkerConfig.requestTimeout()).thenReturn(Duration.ofMillis(100));
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder()
                .messages(Message.builder().messageId("bad").receiptHandle("receipt-bad").body("{not-json").build())
                .build())
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());

        TransitionWorkerFailureException error = assertThrows(
            TransitionWorkerFailureException.class,
            () -> worker.executeTransition(envelope()).await().indefinitely());

        assertTrue(error.getMessage().contains("Timed out"));
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.receiptHandle().equals("receipt-bad")));
    }

    @Test
    void timesOutWaitingForMatchingResponse() {
        when(sqsWorkerConfig.requestTimeout()).thenReturn(Duration.ofMillis(150));
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(List.of())
            .build());

        TransitionWorkerFailureException error = assertThrows(
            TransitionWorkerFailureException.class,
            () -> worker.executeTransition(envelope()).await().indefinitely());

        assertTrue(error.getMessage().contains("Timed out"));
    }

    @Test
    void startupValidationRequiresResponseQueueAndSecret() {
        when(sqsWorkerConfig.isEnabled()).thenReturn(true);
        when(sqsWorkerConfig.responseQueueUrl()).thenReturn(Optional.empty());

        Optional<String> missingResponse = worker.startupValidationError(config);

        assertTrue(missingResponse.isPresent());
        assertTrue(missingResponse.get().contains("response-queue-url"));

        when(sqsWorkerConfig.responseQueueUrl()).thenReturn(Optional.of("https://sqs.local/response"));
        when(sqsWorkerConfig.sharedSecret()).thenReturn(Optional.empty());
        when(sqsWorkerConfig.sharedSecretRef()).thenReturn(Optional.empty());

        Optional<String> missingSecret = worker.startupValidationError(config);

        assertTrue(missingSecret.isPresent());
        assertTrue(missingSecret.get().contains("shared-secret"));
    }

    @Test
    void startupValidationAcceptsSecretReferenceAndRejectsAmbiguity() {
        when(sqsWorkerConfig.isEnabled()).thenReturn(true);
        when(sqsWorkerConfig.sharedSecret()).thenReturn(Optional.empty());
        when(sqsWorkerConfig.sharedSecretRef()).thenReturn(Optional.of("env:TPF_SQS_WORKER_SECRET"));

        Optional<String> accepted = worker.startupValidationError(config);

        assertTrue(accepted.isEmpty());

        when(sqsWorkerConfig.sharedSecret()).thenReturn(Optional.of("worker-secret"));
        Optional<String> ambiguous = worker.startupValidationError(config);

        assertTrue(ambiguous.isPresent());
        assertTrue(ambiguous.get().contains("Configure only one"));
    }

    private TransitionCommandEnvelope envelope() {
        return TransitionEnvelopeFixtures.envelope(payloadCodec);
    }

    private Message responseMessage(String receiptHandle, String requestId, TransitionResultEnvelope result) {
        try {
            String resultJson = PipelineJson.mapper().writeValueAsString(result);
            String timestamp = Instant.now().toString();
            String nonce = UUID.randomUUID().toString();
            String signature = TransitionWorkerSignature.sign(
                "worker-secret",
                SqsTransitionWorkerProtocol.SIGNATURE_METHOD,
                SqsTransitionWorkerProtocol.RESPONSE_SIGNATURE_PATH,
                timestamp,
                nonce,
                SqsTransitionWorkerProtocol.signedBytes(requestId, resultJson));
            SqsTransitionWorkerResponse response = new SqsTransitionWorkerResponse(
                requestId,
                SqsTransitionWorkerProtocol.PROTOCOL_VERSION,
                SqsTransitionWorkerProtocol.PAYLOAD_ENCODING,
                resultJson,
                timestamp,
                nonce,
                signature);
            return Message.builder()
                .messageId(requestId)
                .receiptHandle(receiptHandle)
                .body(PipelineJson.mapper().writeValueAsString(response))
                .build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
