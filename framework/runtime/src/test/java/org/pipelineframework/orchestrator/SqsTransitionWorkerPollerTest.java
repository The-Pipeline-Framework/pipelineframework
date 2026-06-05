package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqsTransitionWorkerPollerTest {

    private final JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();
    private SqsClient client;
    private PipelineExecutionService executionService;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.SqsWorkerConfig sqsWorkerConfig;
    private PipelineOrchestratorConfig.SqsConfig sqsConfig;
    private SqsTransitionWorkerPoller poller;

    @BeforeEach
    void setUp() {
        client = mock(SqsClient.class);
        executionService = mock(PipelineExecutionService.class);
        config = mock(PipelineOrchestratorConfig.class);
        sqsWorkerConfig = mock(PipelineOrchestratorConfig.SqsWorkerConfig.class);
        sqsConfig = mock(PipelineOrchestratorConfig.SqsConfig.class);
        when(config.workerSqs()).thenReturn(sqsWorkerConfig);
        when(config.sqs()).thenReturn(sqsConfig);
        when(sqsWorkerConfig.serverEnabled()).thenReturn(true);
        when(sqsWorkerConfig.requestQueueUrl()).thenReturn(Optional.of("https://sqs.local/request"));
        when(sqsWorkerConfig.responseQueueUrl()).thenReturn(Optional.of("https://sqs.local/response"));
        when(sqsWorkerConfig.requestTimeout()).thenReturn(Duration.ofSeconds(2));
        when(sqsWorkerConfig.visibilityTimeout()).thenReturn(Duration.ofSeconds(30));
        when(sqsWorkerConfig.signatureTolerance()).thenReturn(Duration.ofMinutes(2));
        when(sqsWorkerConfig.sharedSecret()).thenReturn(Optional.of("worker-secret"));
        when(sqsWorkerConfig.sharedSecretRef()).thenReturn(Optional.empty());
        when(sqsConfig.region()).thenReturn(Optional.empty());
        when(sqsConfig.endpointOverride()).thenReturn(Optional.empty());
        poller = new SqsTransitionWorkerPoller(config, executionService, client);
    }

    @Test
    void pollOnceDelegatesToLocalWorkerSendsResponseAndDeletesRequest() {
        TransitionCommandEnvelope envelope = envelope();
        TransitionResultEnvelope result = TransitionResultEnvelope.completed(payloadCodec, List.of("ok"));
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(requestMessage("receipt-1", "request-1", envelope))
            .build());
        when(executionService.executePortableTransition(envelope)).thenReturn(Uni.createFrom().item(result));

        poller.pollOnce();

        verify(executionService).executePortableTransition(envelope);
        verify(client).sendMessage(argThat((SendMessageRequest request) -> {
            SqsTransitionWorkerResponse response = decodeResponse(request.messageBody());
            return request.queueUrl().equals("https://sqs.local/response")
                && response.requestId().equals("request-1")
                && response.resultEnvelope().contains("\"outcome\":\"COMPLETED\"");
        }));
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.queueUrl().equals("https://sqs.local/request")
                && request.receiptHandle().equals("receipt-1")));
    }

    @Test
    void pollOnceSendsFailedOutcomeAndDeletesRequest() {
        TransitionCommandEnvelope envelope = envelope();
        TransitionResultEnvelope result = TransitionResultEnvelope.failed(new IllegalStateException("boom"));
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(requestMessage("receipt-failed", "request-failed", envelope))
            .build());
        when(executionService.executePortableTransition(envelope)).thenReturn(Uni.createFrom().item(result));

        poller.pollOnce();

        verify(executionService).executePortableTransition(envelope);
        verify(client).sendMessage(argThat((SendMessageRequest request) -> {
            SqsTransitionWorkerResponse response = decodeResponse(request.messageBody());
            return request.queueUrl().equals("https://sqs.local/response")
                && response.requestId().equals("request-failed")
                && response.resultEnvelope().contains("\"outcome\":\"FAILED\"");
        }));
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.queueUrl().equals("https://sqs.local/request")
                && request.receiptHandle().equals("receipt-failed")));
    }

    @Test
    void pollOnceAuthenticatesWithSharedSecretReference() {
        TransitionCommandEnvelope envelope = envelope();
        TransitionResultEnvelope result = TransitionResultEnvelope.completed(payloadCodec, List.of("ok"));
        when(sqsWorkerConfig.sharedSecret()).thenReturn(Optional.empty());
        when(sqsWorkerConfig.sharedSecretRef()).thenReturn(Optional.of("sys:tpf.sqs.worker.secret"));
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(requestMessage("receipt-1", "request-1", envelope))
            .build());
        when(executionService.executePortableTransition(envelope)).thenReturn(Uni.createFrom().item(result));

        try {
            System.setProperty("tpf.sqs.worker.secret", "worker-secret");
            poller.pollOnce();

            verify(executionService).executePortableTransition(envelope);
            verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
                request.receiptHandle().equals("receipt-1")));
        } finally {
            System.clearProperty("tpf.sqs.worker.secret");
        }
    }

    @Test
    void pollOnceLeavesRequestWhenResponseSendFails() {
        TransitionCommandEnvelope envelope = envelope();
        Message message = requestMessage("receipt-2", "request-2", envelope);
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(message)
            .build());
        when(executionService.executePortableTransition(envelope))
            .thenReturn(Uni.createFrom().item(TransitionResultEnvelope.completed(payloadCodec, List.of("ok"))));
        when(client.sendMessage(any(SendMessageRequest.class))).thenThrow(new IllegalStateException("send failed"));

        poller.pollOnce();

        verify(client, never()).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.receiptHandle().equals("receipt-2")));
    }

    @Test
    void pollOnceAllowsRedeliveryAfterResponseSendFailsBeforeDeletingRequest() {
        TransitionCommandEnvelope envelope = envelope();
        Message message = requestMessage("receipt-5", "request-5", envelope);
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(
            ReceiveMessageResponse.builder().messages(message).build(),
            ReceiveMessageResponse.builder().messages(message).build());
        when(executionService.executePortableTransition(envelope))
            .thenReturn(
                Uni.createFrom().item(TransitionResultEnvelope.completed(payloadCodec, List.of("ok"))),
                Uni.createFrom().item(TransitionResultEnvelope.completed(payloadCodec, List.of("ok"))));
        when(client.sendMessage(any(SendMessageRequest.class)))
            .thenThrow(new IllegalStateException("send failed"))
            .thenReturn(null);

        poller.pollOnce();
        poller.pollOnce();

        verify(executionService, org.mockito.Mockito.times(2)).executePortableTransition(envelope);
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.receiptHandle().equals("receipt-5")));
    }

    @Test
    void pollOnceDeletesBadSignatureWithoutExecuting() {
        TransitionCommandEnvelope envelope = envelope();
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(unsignedRequestMessage("receipt-3", "request-3", envelope))
            .build());

        poller.pollOnce();

        verify(executionService, never()).executePortableTransition(any());
        verify(client, never()).sendMessage(any(SendMessageRequest.class));
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.receiptHandle().equals("receipt-3")));
    }

    @Test
    void pollOnceDeletesMalformedRequestWithoutExecuting() {
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(Message.builder().messageId("bad").receiptHandle("receipt-4").body("{not-json").build())
            .build());

        poller.pollOnce();

        verify(executionService, never()).executePortableTransition(any());
        verify(client, never()).sendMessage(any(SendMessageRequest.class));
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.receiptHandle().equals("receipt-4")));
    }

    @Test
    void pollOnceSkipsWhenServerDisabled() {
        when(sqsWorkerConfig.serverEnabled()).thenReturn(false);

        poller.pollOnce();

        verify(client, never()).receiveMessage(any(ReceiveMessageRequest.class));
    }

    @Test
    void pollOnceUsesConfiguredVisibilityTimeout() {
        when(sqsWorkerConfig.visibilityTimeout()).thenReturn(Duration.ofSeconds(45));
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(List.of())
            .build());

        poller.pollOnce();

        verify(client).receiveMessage(argThat((ReceiveMessageRequest request) ->
            request.visibilityTimeout().equals(45)));
    }

    private TransitionCommandEnvelope envelope() {
        return TransitionEnvelopeFixtures.envelope(payloadCodec);
    }

    private Message requestMessage(String receiptHandle, String requestId, TransitionCommandEnvelope envelope) {
        try {
            String commandJson = PipelineJson.mapper().writeValueAsString(envelope);
            String timestamp = Instant.now().toString();
            String nonce = UUID.randomUUID().toString();
            String signature = TransitionWorkerSignature.sign(
                "worker-secret",
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
            return Message.builder()
                .messageId(requestId)
                .receiptHandle(receiptHandle)
                .body(PipelineJson.mapper().writeValueAsString(request))
                .build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Message unsignedRequestMessage(String receiptHandle, String requestId, TransitionCommandEnvelope envelope) {
        try {
            String commandJson = PipelineJson.mapper().writeValueAsString(envelope);
            SqsTransitionWorkerRequest request = new SqsTransitionWorkerRequest(
                requestId,
                SqsTransitionWorkerProtocol.PROTOCOL_VERSION,
                SqsTransitionWorkerProtocol.PAYLOAD_ENCODING,
                commandJson,
                Instant.now().toString(),
                UUID.randomUUID().toString(),
                "bad-signature");
            return Message.builder()
                .messageId(requestId)
                .receiptHandle(receiptHandle)
                .body(PipelineJson.mapper().writeValueAsString(request))
                .build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static SqsTransitionWorkerResponse decodeResponse(String body) {
        try {
            return PipelineJson.mapper().readValue(body, SqsTransitionWorkerResponse.class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
