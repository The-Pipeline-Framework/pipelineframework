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
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/dlq"));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(null, config);

        var validationError = publisher.startupValidationError(config);

        assertTrue(validationError.isEmpty());
    }

    @Test
    void startupValidationRejectsBlankDlqUrl() {
        PipelineOrchestratorConfig config = mockConfig(Optional.of("   "));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(null, config);

        var validationError = publisher.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("dlq-url"));
    }

    @Test
    void startupValidationRejectsNullConfig() {
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher();

        var validationError = publisher.startupValidationError(null);

        assertTrue(validationError.isPresent());
    }

    @Test
    void publishIncludesAllEnvelopeFieldsInMessage() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/dlq"));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);
        long now = System.currentTimeMillis();

        publisher.publish(new DeadLetterEnvelope(
                "tenant-b",
                "exec-2",
                "exec-2:1:5",
                "FATAL_ERROR",
                "System failure",
                now))
            .await().indefinitely();

        verify(client).sendMessage(argThat(req ->
            req.queueUrl().equals("https://sqs.local/123/dlq") &&
            req.messageBody().contains("tenant-b") &&
            req.messageBody().contains("exec-2") &&
            req.messageBody().contains("FATAL_ERROR")
        ));
    }

    @Test
    void publishThrowsWhenDlqUrlNotConfigured() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.empty());
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);

        try {
            publisher.publish(new DeadLetterEnvelope(
                    "tenant-a", "exec-1", "key", "ERROR", "msg", System.currentTimeMillis()))
                .await().indefinitely();
            org.junit.jupiter.api.Assertions.fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("dlq-url"));
        }
    }

    @Test
    void publishHandlesNullErrorMessage() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/dlq"));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);

        publisher.publish(new DeadLetterEnvelope(
                "tenant-a",
                "exec-1",
                "exec-1:0:0",
                "ERROR",
                null,
                System.currentTimeMillis()))
            .await().indefinitely();

        verify(client).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void closeClientHandlesNullClient() {
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/dlq"));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(null, config);

        publisher.closeClient();
        // Should not throw
    }
}