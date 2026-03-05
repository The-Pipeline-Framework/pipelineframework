package org.pipelineframework.orchestrator;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.time.Duration;
import jakarta.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

class SqsWorkDispatcherTest {

    @Test
    void providerNameIsSqs() {
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher();
        assertEquals("sqs", dispatcher.providerName());
    }

    @Test
    void startupValidationRequiresQueueUrl() {
        PipelineOrchestratorConfig config = mockConfig(Optional.empty(), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(null, config, null);

        var validationError = dispatcher.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("queue-url"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueNowSendsMessageAndFiresLoopbackEvent() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(event.fireAsync(any())).thenReturn(CompletableFuture.completedFuture(new ExecutionWorkItem("tenant-a", "exec-1")));
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1")).await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
        verify(event).fireAsync(new ExecutionWorkItem("tenant-a", "exec-1"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueDelayedSkipsLoopbackWhenDisabled() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-2"), java.time.Duration.ofSeconds(5))
            .await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
        verify(event, never()).fireAsync(any());
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueDelayedSkipsLoopbackWhenEnabled() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(event.fireAsync(any())).thenReturn(CompletableFuture.completedFuture(new ExecutionWorkItem("tenant-a", "exec-3")));
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-3"), Duration.ofSeconds(5))
            .await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
        verify(event, never()).fireAsync(any());
    }

    private static PipelineOrchestratorConfig mockConfig(Optional<String> queueUrl, boolean localLoopback) {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.SqsConfig sqs = mock(PipelineOrchestratorConfig.SqsConfig.class);
        when(config.queueUrl()).thenReturn(queueUrl);
        when(config.sqs()).thenReturn(sqs);
        when(sqs.localLoopback()).thenReturn(localLoopback);
        when(sqs.region()).thenReturn(Optional.empty());
        when(sqs.endpointOverride()).thenReturn(Optional.empty());
        return config;
    }

    @Test
    void priorityIsNegative() {
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher();
        assertEquals(-1000, dispatcher.priority());
    }

    @Test
    void startupValidationPassesWhenQueueUrlConfigured() {
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(null, config, null);

        var validationError = dispatcher.startupValidationError(config);

        assertTrue(validationError.isEmpty());
    }

    @Test
    void startupValidationRejectsBlankQueueUrl() {
        PipelineOrchestratorConfig config = mockConfig(Optional.of(""), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(null, config, null);

        var validationError = dispatcher.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("queue-url"));
    }

    @Test
    void startupValidationRejectsNullConfig() {
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher();

        var validationError = dispatcher.startupValidationError(null);

        assertTrue(validationError.isPresent());
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueNowSendsMessageWithZeroDelay() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(event.fireAsync(any())).thenReturn(CompletableFuture.completedFuture(new ExecutionWorkItem("tenant-a", "exec-1")));
        when(client.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId("msg-1").build());
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1")).await().indefinitely();

        verify(client).sendMessage(argThat(req -> req.delaySeconds() == 0));
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueDelayedSendsMessageWithClampedDelay() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(client.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId("msg-1").build());
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-1"), Duration.ofSeconds(10))
            .await().indefinitely();

        verify(client).sendMessage(argThat(req -> req.delaySeconds() == 10));
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueDelayedClampsDelayAbove900Seconds() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(client.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId("msg-1").build());
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-1"), Duration.ofSeconds(1000))
            .await().indefinitely();

        verify(client).sendMessage(argThat(req -> req.delaySeconds() == 900));
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueDelayedClampsNegativeDelayToZero() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(client.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId("msg-1").build());
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-1"), Duration.ofSeconds(-5))
            .await().indefinitely();

        verify(client).sendMessage(argThat(req -> req.delaySeconds() == 0));
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueHandlesNullDelayAsZero() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(client.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId("msg-1").build());
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-1"), null)
            .await().indefinitely();

        verify(client).sendMessage(argThat(req -> req.delaySeconds() == 0));
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueThrowsWhenQueueUrlNotConfigured() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.empty(), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        assertThrows(IllegalStateException.class, () ->
            dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1")).await().indefinitely());
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueSerializesWorkItemToJson() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(client.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId("msg-1").build());
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1")).await().indefinitely();

        verify(client).sendMessage(argThat(req ->
            req.messageBody() != null &&
                req.messageBody().contains("tenant-a") &&
                req.messageBody().contains("exec-1")));
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueNowSkipsLoopbackWhenDisabled() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(client.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId("msg-1").build());
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1")).await().indefinitely();

        verify(event, never()).fireAsync(any());
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueDelayedAlwaysSkipsLoopback() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(event.fireAsync(any())).thenReturn(CompletableFuture.completedFuture(new ExecutionWorkItem("tenant-a", "exec-1")));
        when(client.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId("msg-1").build());
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-1"), Duration.ofSeconds(1))
            .await().indefinitely();

        verify(event, never()).fireAsync(any());
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueSucceedsEvenWhenLoopbackFails() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(event.fireAsync(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("loopback error")));
        when(client.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId("msg-1").build());
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1")).await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
        verify(event).fireAsync(any());
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueUsesConfiguredQueueUrl() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(client.sendMessage(any(SendMessageRequest.class)))
            .thenReturn(SendMessageResponse.builder().messageId("msg-1").build());
        String expectedUrl = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue";
        PipelineOrchestratorConfig config = mockConfig(Optional.of(expectedUrl), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1")).await().indefinitely();

        verify(client).sendMessage(argThat(req -> expectedUrl.equals(req.queueUrl())));
    }
}