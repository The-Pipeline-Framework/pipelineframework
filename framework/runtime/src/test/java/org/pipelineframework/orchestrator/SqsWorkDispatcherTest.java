package org.pipelineframework.orchestrator;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.time.Duration;
import jakarta.enterprise.event.Event;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @Test
    void enqueueNowRejectsNullItem() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        NullPointerException error = assertThrows(NullPointerException.class, () -> dispatcher.enqueueNow(null));

        assertTrue(error.getMessage().contains("item must not be null"));
        verifyNoInteractions(client, event);
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
    void priorityIsNegativeThousand() {
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

    @SuppressWarnings("unchecked")
    @Test
    void enqueueDelayedClampsDelayToMaximum900Seconds() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-1"), Duration.ofSeconds(1000))
            .await().indefinitely();

        verify(client).sendMessage(argThat((SendMessageRequest request) ->
            request.delaySeconds() == 900
        ));
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueDelayedHandlesNullDelayAsZero() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(event.fireAsync(any())).thenReturn(CompletableFuture.completedFuture(new ExecutionWorkItem("tenant-a", "exec-1")));
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-1"), null)
            .await().indefinitely();

        verify(client).sendMessage(argThat((SendMessageRequest request) ->
            request.delaySeconds() == 0
        ));
        verify(event).fireAsync(any());
    }

    @Test
    void enqueueNowThrowsWhenQueueUrlNotConfigured() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.empty(), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
            dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1")).await().indefinitely());

        assertTrue(error.getMessage().contains("queue-url must be configured"));
        verifyNoInteractions(client, event);
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueNowToleratesLoopbackFailure() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(event.fireAsync(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Event bus error")));
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1")).await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
        verify(event).fireAsync(new ExecutionWorkItem("tenant-a", "exec-1"));
    }

    @Test
    void startupValidationReportsBlankQueueUrl() {
        PipelineOrchestratorConfig config = mockConfig(Optional.of(""), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(null, config, null);

        var validationError = dispatcher.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("queue-url"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueDelayedSkipsLoopbackWhenDelayIsPositive() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-1"), Duration.ofSeconds(10))
            .await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
        verify(event, never()).fireAsync(any());
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueNowUsesZeroDelaySeconds() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(event.fireAsync(any())).thenReturn(CompletableFuture.completedFuture(new ExecutionWorkItem("tenant-a", "exec-1")));
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1")).await().indefinitely();

        verify(client).sendMessage(argThat((SendMessageRequest request) ->
            request.delaySeconds() == 0
        ));
    }
}
