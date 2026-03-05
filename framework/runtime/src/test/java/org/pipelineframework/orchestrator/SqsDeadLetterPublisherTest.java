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
    void startupValidationPassesWithValidDlqUrl() {
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/dlq"));
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
        assertTrue(validationError.get().contains("dlq-url"));
    }

    @Test
    void publishThrowsWhenDlqUrlNotConfigured() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.empty());
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);

        var envelope = new DeadLetterEnvelope(
            "tenant-a",
            "exec-1",
            "exec-1:1:0",
            "ERROR",
            "message",
            System.currentTimeMillis());

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> {
            publisher.publish(envelope).await().indefinitely();
        });
    }

    @Test
    void publishHandlesNullErrorCode() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/dlq"));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);

        var envelope = new DeadLetterEnvelope(
            "tenant-a",
            "exec-2",
            "exec-2:1:0",
            null,
            "error without code",
            System.currentTimeMillis());

        publisher.publish(envelope).await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void publishHandlesLargeErrorMessage() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/dlq"));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);

        String largeMessage = "x".repeat(10000);
        var envelope = new DeadLetterEnvelope(
            "tenant-a",
            "exec-3",
            "exec-3:1:0",
            "LARGE_ERROR",
            largeMessage,
            System.currentTimeMillis());

        publisher.publish(envelope).await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
    }
}