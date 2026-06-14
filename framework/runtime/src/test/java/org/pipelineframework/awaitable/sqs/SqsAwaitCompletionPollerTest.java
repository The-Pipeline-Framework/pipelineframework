package org.pipelineframework.awaitable.sqs;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitInteractionNotFoundException;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

class SqsAwaitCompletionPollerTest {

    private SqsClient client;
    private PipelineExecutionService executionService;
    private SqsAwaitCompletionPoller poller;

    @BeforeEach
    void setUp() {
        client = mock(SqsClient.class);
        executionService = mock(PipelineExecutionService.class);
        poller = new SqsAwaitCompletionPoller(config(), executionService, client);
    }

    @Test
    void pollOnceDeletesMessageAfterSuccessfulCompletion() throws Exception {
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(message("receipt-1", completionJson()))
            .build());
        when(executionService.completeAwaitInteraction(any(AwaitCompletionCommand.class)))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(null, false)));

        assertDoesNotThrow(() -> poller.pollOnce(enabledConfig()));

        verify(executionService).completeAwaitInteraction(argThat(command ->
            command.tenantId().equals("tenant-1")
                && command.interactionId().equals("interaction-1")
                && command.correlationId().equals("corr-1")
                && command.resumeToken().equals("resume-token")
                && command.idempotencyKey().equals("idem-1")
                && command.actor().equals("csv-payments-sqs-provider")));
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.queueUrl().equals("http://sqs.local/responses")
                && request.receiptHandle().equals("receipt-1")));
    }

    @Test
    void pollOnceDoesNotRetryCompletionWhenDeleteFailsAfterSuccessfulCompletion() throws Exception {
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(message("receipt-1", completionJson()))
            .build());
        when(executionService.completeAwaitInteraction(any(AwaitCompletionCommand.class)))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(null, false)));
        when(client.deleteMessage(any(DeleteMessageRequest.class))).thenThrow(new IllegalStateException("sqs down"));

        assertDoesNotThrow(() -> poller.pollOnce(enabledConfig()));

        verify(executionService).completeAwaitInteraction(any(AwaitCompletionCommand.class));
        verify(client).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test
    void pollOnceDeletesDeterministicAdmissionFailure() throws Exception {
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(message("receipt-2", completionJson()))
            .build());
        when(executionService.completeAwaitInteraction(any(AwaitCompletionCommand.class)))
            .thenReturn(Uni.createFrom().failure(new AwaitInteractionNotFoundException("missing")));

        assertDoesNotThrow(() -> poller.pollOnce(enabledConfig()));

        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.queueUrl().equals("http://sqs.local/responses")
                && request.receiptHandle().equals("receipt-2")));
    }

    @Test
    void pollOnceDoesNotRetryDeterministicAdmissionFailureWhenDeleteFails() throws Exception {
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(message("receipt-2", completionJson()))
            .build());
        when(executionService.completeAwaitInteraction(any(AwaitCompletionCommand.class)))
            .thenReturn(Uni.createFrom().failure(new AwaitInteractionNotFoundException("missing")));
        when(client.deleteMessage(any(DeleteMessageRequest.class))).thenThrow(new IllegalStateException("sqs down"));

        assertDoesNotThrow(() -> poller.pollOnce(enabledConfig()));

        verify(client).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test
    void pollOnceKeepsMessageWhenCompletionFailsTransiently() throws Exception {
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(message("receipt-3", completionJson()))
            .build());
        when(executionService.completeAwaitInteraction(any(AwaitCompletionCommand.class)))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("store down")));

        assertDoesNotThrow(() -> poller.pollOnce(enabledConfig()));

        verify(client, never()).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test
    void pollOnceKeepsMalformedMessageForQueueRedrive() {
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(message("receipt-4", "{not-json"))
            .build());

        assertDoesNotThrow(() -> poller.pollOnce(enabledConfig()));

        verify(client, never()).deleteMessage(any(DeleteMessageRequest.class));
        verify(executionService, never()).completeAwaitInteraction(any(AwaitCompletionCommand.class));
    }

    @Test
    void pollOnceSkipsWhenDisabled() {
        assertDoesNotThrow(() -> poller.pollOnce(disabledConfig()));

        verify(client, never()).receiveMessage(any(ReceiveMessageRequest.class));
        verify(executionService, never()).completeAwaitInteraction(any(AwaitCompletionCommand.class));
    }

    @Test
    void pollOnceUsesConfiguredReceiveSettings() {
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());

        assertDoesNotThrow(() -> poller.pollOnce(enabledConfig()));

        verify(client).receiveMessage(argThat((ReceiveMessageRequest request) ->
            request.queueUrl().equals("http://sqs.local/responses")
                && request.visibilityTimeout().equals(45)
                && request.waitTimeSeconds().equals(2)
                && request.maxNumberOfMessages().equals(3)));
    }

    @Test
    void pollOnceRejectsVisibilityTimeoutAboveSqsLimit() {
        SqsAwaitCompletionPoller.SqsAwaitPollerConfig tooLarge = new SqsAwaitCompletionPoller.SqsAwaitPollerConfig(
            true,
            Optional.of("http://sqs.local/responses"),
            Duration.ZERO,
            Duration.ofSeconds(43_201),
            Duration.ofSeconds(5),
            2,
            3);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> poller.pollOnce(tooLarge));

        assertEquals("tpf.await.sqs.visibility-timeout must be between PT0S and PT43200S.", failure.getMessage());
        verify(client, never()).receiveMessage(any(ReceiveMessageRequest.class));
    }

    private static PipelineOrchestratorConfig config() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.SqsConfig sqs = mock(PipelineOrchestratorConfig.SqsConfig.class);
        when(config.sqs()).thenReturn(sqs);
        when(sqs.region()).thenReturn(Optional.empty());
        when(sqs.endpointOverride()).thenReturn(Optional.empty());
        return config;
    }

    private static SqsAwaitCompletionPoller.SqsAwaitPollerConfig enabledConfig() {
        return new SqsAwaitCompletionPoller.SqsAwaitPollerConfig(
            true,
            Optional.of("http://sqs.local/responses"),
            Duration.ZERO,
            Duration.ofSeconds(45),
            Duration.ofSeconds(5),
            2,
            3);
    }

    private static SqsAwaitCompletionPoller.SqsAwaitPollerConfig disabledConfig() {
        return new SqsAwaitCompletionPoller.SqsAwaitPollerConfig(
            false,
            Optional.of("http://sqs.local/responses"),
            Duration.ZERO,
            Duration.ofSeconds(45),
            Duration.ofSeconds(5),
            2,
            3);
    }

    private static Message message(String receiptHandle, String body) {
        return Message.builder()
            .messageId(receiptHandle)
            .receiptHandle(receiptHandle)
            .body(body)
            .build();
    }

    private static String completionJson() throws Exception {
        SqsAwaitCompletionEnvelope envelope = new SqsAwaitCompletionEnvelope(
            "tenant-1",
            "interaction-1",
            "corr-1",
            "resume-token",
            "idem-1",
            Map.of("status", "Completed"),
            "csv-payments-sqs-provider");
        return PipelineJson.mapper().writeValueAsString(envelope);
    }
}
