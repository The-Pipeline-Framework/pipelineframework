package org.pipelineframework.orchestrator;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SqsDeadLetterPublisherTest {

    @Test
    void providerNameIsSqs() {
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher();
        assertEquals("sqs", publisher.providerName());
    }

    @Test
    void startupValidationRequiresDlqUrl() {
        PipelineOrchestratorConfig config = mockConfig(Optional.empty());
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(null, config);

        var validationError = publisher.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("dlq-url"));
    }

    @Test
    void publishSendsMessageToConfiguredDlq() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/dlq"));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);

        publisher.publish(new DeadLetterEnvelope(
                "tenant-a",
                "exec-1",
                "exec-1:1:0",
                "DOWNSTREAM_TIMEOUT",
                "timeout",
                System.currentTimeMillis()))
            .await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
    }

    private static PipelineOrchestratorConfig mockConfig(Optional<String> dlqUrl) {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.SqsConfig sqs = mock(PipelineOrchestratorConfig.SqsConfig.class);
        when(config.dlqUrl()).thenReturn(dlqUrl);
        when(config.sqs()).thenReturn(sqs);
        when(sqs.region()).thenReturn(Optional.empty());
        when(sqs.endpointOverride()).thenReturn(Optional.empty());
        return config;
    }

    @Test
    void priorityIsNegative() {
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher();
        assertEquals(-1000, publisher.priority());
    }

    @Test
    void startupValidationPassesWhenDlqUrlConfigured() {
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.us-east-1.amazonaws.com/123/dlq"));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(null, config);

        var validationError = publisher.startupValidationError(config);

        assertTrue(validationError.isEmpty());
    }

    @Test
    void startupValidationFailsWithBlankDlqUrl() {
        PipelineOrchestratorConfig config = mockConfig(Optional.of(""));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(null, config);

        var validationError = publisher.startupValidationError(config);

        assertTrue(validationError.isPresent());
    }

    @Test
    void publishThrowsWhenDlqUrlNotConfigured() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.empty());
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> {
            publisher.publish(new DeadLetterEnvelope(
                    "tenant-a",
                    "exec-1",
                    "key-1",
                    "ERROR",
                    "message",
                    System.currentTimeMillis()))
                .await().indefinitely();
        });
    }

    @Test
    void publishWithNullEnvelopeFields() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/dlq"));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);

        publisher.publish(new DeadLetterEnvelope(
                null,
                null,
                null,
                null,
                null,
                0))
            .await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void publishHandlesLongErrorMessages() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/dlq"));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);
        String longMessage = "x".repeat(10000);

        publisher.publish(new DeadLetterEnvelope(
                "tenant-a",
                "exec-long",
                "key",
                "TIMEOUT",
                longMessage,
                System.currentTimeMillis()))
            .await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void publishWithRegionOverride() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.SqsConfig sqs = mock(PipelineOrchestratorConfig.SqsConfig.class);
        when(config.dlqUrl()).thenReturn(Optional.of("https://sqs.us-west-2.amazonaws.com/123/dlq"));
        when(config.sqs()).thenReturn(sqs);
        when(sqs.region()).thenReturn(Optional.of("us-west-2"));
        when(sqs.endpointOverride()).thenReturn(Optional.empty());
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);

        publisher.publish(new DeadLetterEnvelope(
                "tenant-b",
                "exec-2",
                "key-2",
                "ERROR",
                "error",
                System.currentTimeMillis()))
            .await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void publishWithEndpointOverride() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.SqsConfig sqs = mock(PipelineOrchestratorConfig.SqsConfig.class);
        when(config.dlqUrl()).thenReturn(Optional.of("http://localhost:4566/000000000000/dlq"));
        when(config.sqs()).thenReturn(sqs);
        when(sqs.region()).thenReturn(Optional.empty());
        when(sqs.endpointOverride()).thenReturn(Optional.of("http://localhost:4566"));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);

        publisher.publish(new DeadLetterEnvelope(
                "tenant-c",
                "exec-3",
                "key-3",
                "ERROR",
                "localstack test",
                System.currentTimeMillis()))
            .await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
    }
}