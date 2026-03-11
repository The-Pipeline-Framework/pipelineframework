package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

class SqsWorkPollerTest {

    @Test
    void pollOnceDeletesMessageAfterSuccessfulProcessing() {
        SqsClient client = mock(SqsClient.class);
        PipelineExecutionService pipelineExecutionService = mock(PipelineExecutionService.class);
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(validMessage("receipt-1", "exec-1"))
            .build());
        when(pipelineExecutionService.processExecutionWorkItem(new ExecutionWorkItem("tenant-a", "exec-1")))
            .thenReturn(Uni.createFrom().voidItem());
        SqsWorkPoller poller = new SqsWorkPoller(mockConfig(true), pipelineExecutionService, client);

        assertDoesNotThrow(poller::pollOnce);

        verify(pipelineExecutionService).processExecutionWorkItem(new ExecutionWorkItem("tenant-a", "exec-1"));
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.queueUrl().equals("http://elasticmq.local/queue/work")
                && request.receiptHandle().equals("receipt-1")));
    }

    @Test
    void pollOnceKeepsMessageWhenProcessingFails() {
        SqsClient client = mock(SqsClient.class);
        PipelineExecutionService pipelineExecutionService = mock(PipelineExecutionService.class);
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(validMessage("receipt-2", "exec-2"))
            .build());
        when(pipelineExecutionService.processExecutionWorkItem(new ExecutionWorkItem("tenant-a", "exec-2")))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("boom")));
        SqsWorkPoller poller = new SqsWorkPoller(mockConfig(true), pipelineExecutionService, client);

        assertDoesNotThrow(poller::pollOnce);

        verify(pipelineExecutionService).processExecutionWorkItem(new ExecutionWorkItem("tenant-a", "exec-2"));
        verify(client, never()).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test
    void pollOnceDeletesMalformedMessages() {
        SqsClient client = mock(SqsClient.class);
        PipelineExecutionService pipelineExecutionService = mock(PipelineExecutionService.class);
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(Message.builder()
                .messageId("malformed")
                .receiptHandle("receipt-3")
                .body("{not-json")
                .build())
            .build());
        SqsWorkPoller poller = new SqsWorkPoller(mockConfig(true), pipelineExecutionService, client);

        assertDoesNotThrow(poller::pollOnce);

        verifyNoInteractions(pipelineExecutionService);
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.queueUrl().equals("http://elasticmq.local/queue/work")
                && request.receiptHandle().equals("receipt-3")));
    }

    @Test
    void pollOnceSkipsWhenLocalLoopbackIsEnabled() {
        SqsClient client = mock(SqsClient.class);
        PipelineExecutionService pipelineExecutionService = mock(PipelineExecutionService.class);
        SqsWorkPoller poller = new SqsWorkPoller(mockConfig(false), pipelineExecutionService, client);

        assertDoesNotThrow(poller::pollOnce);

        verifyNoInteractions(client);
        verifyNoInteractions(pipelineExecutionService);
    }

    @Test
    void pollOnceDoesNothingWhenReceiveReturnsNoMessages() {
        SqsClient client = mock(SqsClient.class);
        PipelineExecutionService pipelineExecutionService = mock(PipelineExecutionService.class);
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());
        SqsWorkPoller poller = new SqsWorkPoller(mockConfig(true), pipelineExecutionService, client);

        assertDoesNotThrow(poller::pollOnce);

        verify(client, never()).deleteMessage(any(DeleteMessageRequest.class));
        verifyNoInteractions(pipelineExecutionService);
    }

    @Test
    void pollOnceSwallowsReceiveFailures() {
        SqsClient client = mock(SqsClient.class);
        PipelineExecutionService pipelineExecutionService = mock(PipelineExecutionService.class);
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(new IllegalStateException("receive failed"));
        SqsWorkPoller poller = new SqsWorkPoller(mockConfig(true), pipelineExecutionService, client);

        assertDoesNotThrow(poller::pollOnce);

        verify(client, never()).deleteMessage(any(DeleteMessageRequest.class));
        verifyNoInteractions(pipelineExecutionService);
    }

    @Test
    void pollOnceProcessesMultipleMessagesAndDeletesOnlyHandledOnes() {
        SqsClient client = mock(SqsClient.class);
        PipelineExecutionService pipelineExecutionService = mock(PipelineExecutionService.class);
        Message first = validMessage("receipt-4", "exec-4");
        Message malformed = Message.builder()
            .messageId("malformed")
            .receiptHandle("receipt-5")
            .body("{bad-json")
            .build();
        Message second = validMessage("receipt-6", "exec-6");
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder()
            .messages(first, malformed, second)
            .build());
        when(pipelineExecutionService.processExecutionWorkItem(new ExecutionWorkItem("tenant-a", "exec-4")))
            .thenReturn(Uni.createFrom().voidItem());
        when(pipelineExecutionService.processExecutionWorkItem(new ExecutionWorkItem("tenant-a", "exec-6")))
            .thenReturn(Uni.createFrom().voidItem());
        SqsWorkPoller poller = new SqsWorkPoller(mockConfig(true), pipelineExecutionService, client);

        assertDoesNotThrow(poller::pollOnce);

        verify(pipelineExecutionService).processExecutionWorkItem(new ExecutionWorkItem("tenant-a", "exec-4"));
        verify(pipelineExecutionService).processExecutionWorkItem(new ExecutionWorkItem("tenant-a", "exec-6"));
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.queueUrl().equals("http://elasticmq.local/queue/work")
                && request.receiptHandle().equals("receipt-4")));
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.queueUrl().equals("http://elasticmq.local/queue/work")
                && request.receiptHandle().equals("receipt-5")));
        verify(client).deleteMessage(argThat((DeleteMessageRequest request) ->
            request.queueUrl().equals("http://elasticmq.local/queue/work")
                && request.receiptHandle().equals("receipt-6")));
    }

    private static Message validMessage(String receiptHandle, String executionId) {
        String body;
        try {
            body = PipelineJson.mapper().writeValueAsString(new ExecutionWorkItem("tenant-a", executionId));
        } catch (Exception e) {
            throw new IllegalStateException("Failed creating test SQS message JSON.", e);
        }
        return Message.builder()
            .messageId(executionId)
            .receiptHandle(receiptHandle)
            .body(body)
            .build();
    }

    private static PipelineOrchestratorConfig mockConfig(boolean pollerEnabled) {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.SqsConfig sqs = mock(PipelineOrchestratorConfig.SqsConfig.class);
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(config.dispatcherProvider()).thenReturn("sqs");
        when(config.queueUrl()).thenReturn(Optional.of("http://elasticmq.local/queue/work"));
        when(config.sqs()).thenReturn(sqs);
        when(sqs.localLoopback()).thenReturn(!pollerEnabled);
        when(sqs.pollStartDelay()).thenReturn(Duration.ZERO);
        when(sqs.region()).thenReturn(Optional.of("eu-west-1"));
        when(sqs.endpointOverride()).thenReturn(Optional.empty());
        return config;
    }
}
