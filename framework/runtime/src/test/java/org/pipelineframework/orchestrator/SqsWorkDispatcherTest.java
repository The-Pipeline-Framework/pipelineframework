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
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.us-east-1.amazonaws.com/123/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(null, config, null);

        var validationError = dispatcher.startupValidationError(config);

        assertTrue(validationError.isEmpty());
    }

    @Test
    void startupValidationFailsWithBlankQueueUrl() {
        PipelineOrchestratorConfig config = mockConfig(Optional.of(""), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(null, config, null);

        var validationError = dispatcher.startupValidationError(config);

        assertTrue(validationError.isPresent());
    }

    @Test
    void enqueueNowThrowsWhenQueueUrlNotConfigured() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.empty(), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> {
            dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1")).await().indefinitely();
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueNowSkipsLoopbackWhenDisabled() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-1")).await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
        verify(event, never()).fireAsync(any());
    }

    @SuppressWarnings("unchecked")
    @Test
    void enqueueNowToleratesLoopbackFailure() {
        SqsClient client = mock(SqsClient.class);
        Event<ExecutionWorkItem> event = mock(Event.class);
        when(event.fireAsync(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("loopback failed")));
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/work"), true);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, event);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-a", "exec-fail")).await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void enqueueDelayedClampsDelayToMaximum() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-max"), Duration.ofSeconds(1000))
            .await().indefinitely();

        verify(client).sendMessage(argThat(req -> req.delaySeconds() == 900));
    }

    @Test
    void enqueueDelayedHandlesNullDelay() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-null"), null)
            .await().indefinitely();

        verify(client).sendMessage(argThat(req -> req.delaySeconds() == 0));
    }

    @Test
    void enqueueDelayedHandlesNegativeDelay() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-neg"), Duration.ofSeconds(-10))
            .await().indefinitely();

        verify(client).sendMessage(argThat(req -> req.delaySeconds() == 0));
    }

    @Test
    void enqueueDelayedHandlesZeroDelay() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/work"), false);
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        dispatcher.enqueueDelayed(new ExecutionWorkItem("tenant-a", "exec-zero"), Duration.ZERO)
            .await().indefinitely();

        verify(client).sendMessage(argThat(req -> req.delaySeconds() == 0));
    }

    @Test
    void enqueueWithRegionOverride() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.SqsConfig sqs = mock(PipelineOrchestratorConfig.SqsConfig.class);
        when(config.queueUrl()).thenReturn(Optional.of("https://sqs.eu-west-1.amazonaws.com/123/work"));
        when(config.sqs()).thenReturn(sqs);
        when(sqs.localLoopback()).thenReturn(false);
        when(sqs.region()).thenReturn(Optional.of("eu-west-1"));
        when(sqs.endpointOverride()).thenReturn(Optional.empty());
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-eu", "exec-eu")).await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void enqueueWithEndpointOverride() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.SqsConfig sqs = mock(PipelineOrchestratorConfig.SqsConfig.class);
        when(config.queueUrl()).thenReturn(Optional.of("http://localhost:4566/000000000000/work"));
        when(config.sqs()).thenReturn(sqs);
        when(sqs.localLoopback()).thenReturn(false);
        when(sqs.region()).thenReturn(Optional.empty());
        when(sqs.endpointOverride()).thenReturn(Optional.of("http://localhost:4566"));
        SqsWorkDispatcher dispatcher = new SqsWorkDispatcher(client, config, null);

        dispatcher.enqueueNow(new ExecutionWorkItem("tenant-local", "exec-local")).await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
    }
}