package org.pipelineframework.orchestrator;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.time.Duration;
import jakarta.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void startupValidationFailsWhenConfigIsNull() {
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher();

        var validationError = dispatcher.startupValidationError(null);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("queue-url"));
    }

    @Test
    void startupValidationFailsWhenQueueUrlIsBlank() {
        PipelineOrchestratorConfig config = mockConfig(Optional.of(""), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(null, config, null);

        var validationError = dispatcher.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("queue-url"));
    }

    @Test
    void enqueueNowThrowsExceptionWhenQueueUrlNotConfigured() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.empty(), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        Exception exception = null;
        try {
            dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1"))
                .await().indefinitely();
        } catch (Exception e) {
            exception = e;
        }

        assertTrue(exception instanceof IllegalStateException);
        assertTrue(exception.getMessage().contains("queue-url"));
        verify(client, never()).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void enqueueDelayedClampsDelayTo900Seconds() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-1"), Duration.ofSeconds(1000))
            .await().indefinitely();

        verify(client).sendMessage(argThat(request -> request.delaySeconds() == 900));
    }

    @Test
    void enqueueDelayedClampsNegativeDelayToZero() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-1"), Duration.ofSeconds(-5))
            .await().indefinitely();

        verify(client).sendMessage(argThat(request -> request.delaySeconds() == 0));
    }

    @Test
    void enqueueDelayedTreatsNullDelayAsZero() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-1"), null)
            .await().indefinitely();

        verify(client).sendMessage(argThat(request -> request.delaySeconds() == 0));
    }

    @Test
    void enqueueSerializesWorkItemToJson() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-b", "exec-2"))
            .await().indefinitely();

        verify(client).sendMessage(argThat(request -> {
            String body = request.messageBody();
            return body.contains("tenant-b") && body.contains("exec-2");
        }));
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueNowWithEventNull() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1"))
            .await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueNowContinuesWhenLoopbackFails() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(event.fireAsync(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("loopback failure")));
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1"))
            .await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
        verify(event).fireAsync(any());
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
}