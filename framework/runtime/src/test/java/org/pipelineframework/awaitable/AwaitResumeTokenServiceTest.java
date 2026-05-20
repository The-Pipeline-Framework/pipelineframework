package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class AwaitResumeTokenServiceTest {

    private final AwaitResumeTokenService service = new AwaitResumeTokenService("secret-value-for-tests");

    @Test
    void signedTokenValidatesAgainstOriginalInteraction() {
        AwaitInteractionRecord record = record("tenant-1", "interaction-1", "corr-1", 20_000L);

        String token = service.sign(record, 10_000L);

        assertDoesNotThrow(() -> service.validate(token, record, 11_000L));
    }

    @Test
    void rejectsTamperedToken() {
        AwaitInteractionRecord record = record("tenant-1", "interaction-1", "corr-1", 20_000L);
        String token = service.sign(record, 10_000L);
        String tampered = token.substring(0, token.length() - 2) + "xx";

        assertThrows(IllegalArgumentException.class, () -> service.validate(tampered, record, 11_000L));
    }

    @Test
    void rejectsExpiredToken() {
        AwaitInteractionRecord record = record("tenant-1", "interaction-1", "corr-1", 20_000L);
        String token = service.sign(record, 10_000L);

        assertThrows(IllegalStateException.class, () -> service.validate(token, record, 20_001L));
    }

    @Test
    void rejectsWrongTenant() {
        AwaitInteractionRecord record = record("tenant-1", "interaction-1", "corr-1", 20_000L);
        AwaitInteractionRecord wrongTenant = record("tenant-2", "interaction-1", "corr-1", 20_000L);
        String token = service.sign(record, 10_000L);

        assertThrows(IllegalArgumentException.class, () -> service.validate(token, wrongTenant, 11_000L));
    }

    @Test
    void rejectsWrongInteractionId() {
        AwaitInteractionRecord record = record("tenant-1", "interaction-1", "corr-1", 20_000L);
        AwaitInteractionRecord wrongInteraction = record("tenant-1", "interaction-2", "corr-1", 20_000L);
        String token = service.sign(record, 10_000L);

        assertThrows(IllegalArgumentException.class, () -> service.validate(token, wrongInteraction, 11_000L));
    }

    @Test
    void rejectsWrongCorrelationId() {
        AwaitInteractionRecord record = record("tenant-1", "interaction-1", "corr-1", 20_000L);
        AwaitInteractionRecord wrongCorrelation = record("tenant-1", "interaction-1", "corr-2", 20_000L);
        String token = service.sign(record, 10_000L);

        assertThrows(IllegalArgumentException.class, () -> service.validate(token, wrongCorrelation, 11_000L));
    }

    @Test
    void rejectsWrongExecutionId() {
        AwaitInteractionRecord record = record("tenant-1", "interaction-1", "corr-1", "exec-1", "FraudCheck", 20_000L);
        AwaitInteractionRecord wrongExecution = record("tenant-1", "interaction-1", "corr-1", "exec-2", "FraudCheck", 20_000L);
        String token = service.sign(record, 10_000L);

        assertThrows(IllegalArgumentException.class, () -> service.validate(token, wrongExecution, 11_000L));
    }

    @Test
    void rejectsWrongStepId() {
        AwaitInteractionRecord record = record("tenant-1", "interaction-1", "corr-1", "exec-1", "FraudCheck", 20_000L);
        AwaitInteractionRecord wrongStep = record("tenant-1", "interaction-1", "corr-1", "exec-1", "ReviewStep", 20_000L);
        String token = service.sign(record, 10_000L);

        assertThrows(IllegalArgumentException.class, () -> service.validate(token, wrongStep, 11_000L));
    }

    @Test
    void rejectsBlankToken() {
        AwaitInteractionRecord record = record("tenant-1", "interaction-1", "corr-1", 20_000L);

        assertThrows(IllegalArgumentException.class, () -> service.validate(null, record, 11_000L));
        assertThrows(IllegalArgumentException.class, () -> service.validate("  ", record, 11_000L));
    }

    @Test
    void rejectsMalformedTokenParts() {
        AwaitInteractionRecord record = record("tenant-1", "interaction-1", "corr-1", 20_000L);

        assertThrows(IllegalArgumentException.class, () -> service.validate("not-a-token", record, 11_000L));
        assertThrows(IllegalArgumentException.class, () -> service.validate(".signature", record, 11_000L));
        assertThrows(IllegalArgumentException.class, () -> service.validate("payload.", record, 11_000L));
    }

    @Test
    void rejectsMissingSecretWhenSigning() {
        AwaitResumeTokenService serviceWithoutSecret = new AwaitResumeTokenService();

        assertThrows(IllegalStateException.class, () -> serviceWithoutSecret.sign(
            record("tenant-1", "interaction-1", "corr-1", 20_000L),
            10_000L));
    }

    private static AwaitInteractionRecord record(
        String tenantId,
        String interactionId,
        String correlationId,
        long deadlineEpochMs) {
        return record(tenantId, interactionId, correlationId, "exec-1", "FraudCheck", deadlineEpochMs);
    }

    private static AwaitInteractionRecord record(
        String tenantId,
        String interactionId,
        String correlationId,
        String executionId,
        String stepId,
        long deadlineEpochMs) {
        return new AwaitInteractionRecord(
            tenantId,
            executionId,
            stepId,
            1,
            "com.example.Decision",
            interactionId,
            correlationId,
            "cause-1",
            "idem-1",
            0L,
            AwaitInteractionStatus.DISPATCHED,
            java.util.Map.of("orderId", "o-1"),
            null,
            null,
            null,
            null,
            "webhook",
            java.util.Map.of(),
            deadlineEpochMs,
            1_000L,
            2_000L,
            99_999L);
    }
}
