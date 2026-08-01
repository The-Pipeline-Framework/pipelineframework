package org.pipelineframework.localcommandproof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.pipelineframework.LocalPipelineControlPlane;
import org.pipelineframework.command.CommandEffectRecord;
import org.pipelineframework.command.CommandEffectStatus;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

/**
 * Generated LOCAL command-path conformance coverage. This is deliberately not an application example.
 */
@QuarkusTest
class ObservedOperationCommandMonolithTest {

    private static final String TENANT_ID = "local-command-proof";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Inject
    LocalPipelineControlPlane controlPlane;

    @Inject
    InMemoryCommandEffectStore commandEffects;

    @Inject
    FakeSerializedOperationManager manager;

    @BeforeEach
    void resetManager() {
        manager.resetForTest();
    }

    @Test
    void generatedCommandConnectorRunsThroughLocalQueueAsyncAndRecordsTypedSuccess() {
        ObservedOperationCommand command = success("success");

        RunAsyncAcceptedDto accepted = submit(command, "success-submission");
        ObservedOperationResult result = awaitResult(accepted.executionId());
        ExecutionStatusDto status = awaitTerminalStatus(accepted.executionId());
        CommandEffectRecord effect = awaitEffect(command.operationId(), CommandEffectStatus.SUCCEEDED);

        assertEquals(command, manager.lastInput());
        assertEquals(Map.of("observationLabel", "local-command-proof", "maxConcurrency", 1), manager.lastConfig());
        assertThrows(UnsupportedOperationException.class, () -> manager.lastConfig().put("another", "value"));
        assertEquals(command.operationId(), result.operationId());
        assertEquals("OBSERVED:" + command.operationId(), result.confirmation());
        assertTrue(result.workerThreadName().startsWith("local-command-proof-worker"));
        assertFalse(result.workerThreadName().contains("vert.x-eventloop"));
        assertEquals(ExecutionStatus.SUCCEEDED, status.status());
        assertEquals(CommandEffectStatus.SUCCEEDED, effect.status());
        assertEquals(result, effect.output());
        assertEquals(1, manager.invocationCount());
    }

    @Test
    void successfulSameCommandIdReplaysRecordedOutputWithoutAnotherOperation() {
        ObservedOperationCommand command = success("replay");

        ObservedOperationResult first = awaitResult(submit(command, "replay-first").executionId());
        ObservedOperationResult replay = awaitResult(submit(command, "replay-second").executionId());

        assertEquals(first, replay);
        assertEquals(1, manager.invocationCount());
        assertEquals(CommandEffectStatus.SUCCEEDED, awaitEffect(command.operationId(), CommandEffectStatus.SUCCEEDED).status());
    }

    @Test
    void differentCommandIdsRemainIndependent() {
        ObservedOperationCommand firstCommand = success("independent-one");
        ObservedOperationCommand secondCommand = success("independent-two");

        ObservedOperationResult first = awaitResult(submit(firstCommand, "independent-first").executionId());
        ObservedOperationResult second = awaitResult(submit(secondCommand, "independent-second").executionId());

        assertEquals("independent-one", first.operationId());
        assertEquals("independent-two", second.operationId());
        assertNotNull(awaitEffect(firstCommand.operationId(), CommandEffectStatus.SUCCEEDED));
        assertNotNull(awaitEffect(secondCommand.operationId(), CommandEffectStatus.SUCCEEDED));
        assertEquals(2, manager.invocationCount());
    }

    @Test
    void nonRetryableCommandFailureTerminatesExecutionAndRecordsDlqEffect() {
        ObservedOperationCommand command = new ObservedOperationCommand(
            "non-retryable", ObservedOperationCommand.Behavior.NON_RETRYABLE_FAILURE);

        ExecutionStatusDto status = awaitTerminalStatus(submit(command, "non-retryable").executionId());

        assertEquals(ExecutionStatus.FAILED, status.status());
        assertEquals(CommandEffectStatus.DLQ, awaitEffect(command.operationId(), CommandEffectStatus.DLQ).status());
    }

    @Test
    @Disabled("Deferred issue: FAILED_RETRYABLE command effects cannot currently be redispatched with the same stable command ID.")
    void retryableCommandFailureTerminatesExecutionAndRecordsRetryableEffect() {
        ObservedOperationCommand command = new ObservedOperationCommand(
            "retryable", ObservedOperationCommand.Behavior.RETRYABLE_FAILURE);

        ExecutionStatusDto status = awaitTerminalStatus(submit(command, "retryable").executionId());

        assertEquals(ExecutionStatus.FAILED, status.status());
        assertEquals(
            CommandEffectStatus.FAILED_RETRYABLE,
            awaitEffect(command.operationId(), CommandEffectStatus.FAILED_RETRYABLE).status());
    }

    @Test
    void applicationManagerSerializesIndependentBlockingOperations() throws InterruptedException {
        ObservedOperationCommand firstCommand = blockingSuccess("serial-one");
        ObservedOperationCommand secondCommand = blockingSuccess("serial-two");
        manager.prepareBlockingOperation();

        Future<ObservedOperationResult> first = manager.executor().submit(
            () -> manager.executeBlocking(firstCommand, Map.of("maxConcurrency", 1)));
        assertTrue(manager.awaitBlockingOperationEntered(5, TimeUnit.SECONDS));
        Future<ObservedOperationResult> second = manager.executor().submit(
            () -> manager.executeBlocking(secondCommand, Map.of("maxConcurrency", 1)));
        manager.releaseBlockingOperation();

        assertEquals(firstCommand.operationId(), getFutureResult(first).operationId());
        assertEquals(secondCommand.operationId(), getFutureResult(second).operationId());

        assertEquals(1, manager.maxActiveOperations());
        assertEquals(2, manager.invocationCount());
    }

    private RunAsyncAcceptedDto submit(ObservedOperationCommand command, String submissionName) {
        return controlPlane.executePipelineAsync(
                command,
                TENANT_ID,
                submissionName + ":" + UUID.randomUUID(),
                false)
            .await().atMost(TIMEOUT);
    }

    private ObservedOperationResult awaitResult(String executionId) {
        long deadlineNanos = System.nanoTime() + TIMEOUT.toNanos();
        RuntimeException latest = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                return (ObservedOperationResult) controlPlane.getExecutionResult(
                    TENANT_ID, executionId, ObservedOperationResult.class, false)
                    .await().atMost(Duration.ofMillis(250));
            } catch (RuntimeException failure) {
                latest = failure;
                sleepBriefly();
            }
        }
        throw new AssertionError("Execution did not produce a result: " + executionId, latest);
    }

    private ExecutionStatusDto awaitTerminalStatus(String executionId) {
        long deadlineNanos = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            ExecutionStatusDto status = controlPlane.getExecutionStatus(TENANT_ID, executionId)
                .await().atMost(Duration.ofMillis(250));
            if (status.status() != null && status.status().terminal()) {
                return status;
            }
            sleepBriefly();
        }
        throw new AssertionError("Execution did not reach a terminal status: " + executionId);
    }

    private CommandEffectRecord awaitEffect(String operationId, CommandEffectStatus expectedStatus) {
        String commandId = "observed-operation:" + operationId;
        long deadlineNanos = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            CommandEffectRecord record = commandEffects.find(TENANT_ID, commandId)
                .await().atMost(Duration.ofMillis(250))
                .orElse(null);
            if (record != null && record.status() == expectedStatus) {
                return record;
            }
            sleepBriefly();
        }
        throw new AssertionError("Command effect did not reach " + expectedStatus + ": " + commandId);
    }

    private static ObservedOperationCommand success(String operationId) {
        return new ObservedOperationCommand(operationId, ObservedOperationCommand.Behavior.SUCCESS);
    }

    private static ObservedOperationCommand blockingSuccess(String operationId) {
        return new ObservedOperationCommand(operationId, ObservedOperationCommand.Behavior.BLOCKING_SUCCESS);
    }

    private static void sleepBriefly() {
        try {
            Thread.sleep(25);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while awaiting local execution", interrupted);
        }
    }

    private static ObservedOperationResult getFutureResult(Future<ObservedOperationResult> operation) {
        try {
            return operation.get(5, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new AssertionError("Serialized manager operation did not complete", failure);
        }
    }
}
