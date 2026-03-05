package org.pipelineframework.orchestrator;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
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
    void startupValidationFailsWhenConfigIsNull() {
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher();

        var validationError = publisher.startupValidationError(null);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("dlq-url"));
    }

    @Test
    void startupValidationFailsWhenDlqUrlIsBlank() {
        PipelineOrchestratorConfig config = mockConfig(Optional.of(""));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(null, config);

        var validationError = publisher.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("dlq-url"));
    }

    @Test
    void publishThrowsExceptionWhenDlqUrlNotConfigured() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.empty());
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);

        Exception exception = null;
        try {
            publisher.publish(new DeadLetterEnvelope(
                    "tenant-a",
                    "exec-1",
                    "exec-1:1:0",
                    "ERROR",
                    "message",
                    System.currentTimeMillis()))
                .await().indefinitely();
        } catch (Exception e) {
            exception = e;
        }

        assertTrue(exception instanceof IllegalStateException);
        assertTrue(exception.getMessage().contains("dlq-url"));
        verify(client, never()).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void publishSerializesEnvelopeToJson() {
        SqsClient client = mock(SqsClient.class);
        PipelineOrchestratorConfig config = mockConfig(Optional.of("https://sqs.local/123/dlq"));
        SqsDeadLetterPublisher publisher = new SqsDeadLetterPublisher(client, config);

        publisher.publish(new DeadLetterEnvelope(
                "tenant-b",
                "exec-2",
                "exec-2:3:0",
                "MAX_RETRIES",
                "Failed after 3 attempts",
                1234567890000L))
            .await().indefinitely();

        verify(client).sendMessage(argThat((SendMessageRequest request) -> {
            String body = request.messageBody();
            return body.contains("tenant-b") &&
                   body.contains("exec-2") &&
                   body.contains("exec-2:3:0") &&
                   body.contains("MAX_RETRIES") &&
                   body.contains("Failed after 3 attempts");
        }));
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
}
