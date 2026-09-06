package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionRedriveIntent;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.TransitionWorkerCommand;

class CommandReexecutionBoundaryTest {
    @AfterEach
    void clearScope() {
        CommandReexecutionScope.clear();
    }

    @Test
    void onlyExactLogicalEffectConsumesRetryAdmission() {
        TransitionWorkerCommand command = retryCommand("failed-effect", "transition-1");

        List<?> result = CommandReexecutionBoundary.invokeTransitionWorker(command, () -> {
            assertTrue(CommandReexecutionScope.claimAttempt("other-effect", "occurrence-1").isEmpty());
            assertTrue(CommandReexecutionScope.claimAttempt("failed-effect", "occurrence-1").isPresent());
            assertTrue(CommandReexecutionScope.claimAttempt("failed-effect", "occurrence-1").isEmpty());
            return Multi.createFrom().item("retried");
        }).collect().asList().await().indefinitely();

        assertEquals(List.of("retried"), result);
    }

    @Test
    void successfulReplayFailsWhenExactEffectWasNotReached() {
        TransitionWorkerCommand command = retryCommand("failed-effect", "transition-2");

        RuntimeException failure = assertThrows(RuntimeException.class, () -> CommandReexecutionBoundary.invokeTransitionWorker(
            command,
            () -> Multi.createFrom().item("unchanged")).collect().asList().await().indefinitely());

        assertTrue(hasMessage(failure, "did not encounter logical effect failed-effect"));
    }

    @Test
    void previousScopeIsRestoredAfterSuccessAndFailure() {
        CommandReexecutionScope.installRetry("outer-effect", "outer-transition");

        CommandReexecutionBoundary.invokeTransitionWorker(replayCommand("success"), () -> {
            assertTrue(CommandReexecutionScope.claimAttempt("outer-effect", "outer-occurrence").isEmpty());
            return Multi.createFrom().item("ok");
        }).collect().asList().await().indefinitely();
        assertTrue(CommandReexecutionScope.claimAttempt("outer-effect", "outer-occurrence").isPresent());

        CommandReexecutionScope.installRetry("outer-effect-2", "outer-transition-2");
        assertThrows(IllegalStateException.class, () -> CommandReexecutionBoundary.invokeTransitionWorker(
            replayCommand("failure"), () ->
            Multi.createFrom().failure(new IllegalStateException("boom")))
            .collect().asList().await().indefinitely());
        assertTrue(CommandReexecutionScope.claimAttempt("outer-effect-2", "outer-occurrence-2").isPresent());
    }

    @Test
    void previousScopeIsRestoredAfterCancellation() throws InterruptedException {
        CommandReexecutionScope.installRetry("outer-effect", "outer-transition");
        CountDownLatch cancelled = new CountDownLatch(1);
        Multi<String> never = Multi.createFrom().emitter(emitter -> emitter.onTermination(cancelled::countDown));

        AssertSubscriber<Object> subscriber = CommandReexecutionBoundary
            .invokeTransitionWorker(replayCommand("cancel"), () -> never)
            .subscribe().withSubscriber(AssertSubscriber.create(1));
        assertFalse(CommandReexecutionScope.claimAttempt("outer-effect", "outer-occurrence").isPresent());
        subscriber.cancel();

        assertTrue(cancelled.await(2, TimeUnit.SECONDS));
        assertTrue(CommandReexecutionScope.claimAttempt("outer-effect", "outer-occurrence").isPresent());
    }

    private static TransitionWorkerCommand retryCommand(String commandId, String transitionKey) {
        return new TransitionWorkerCommand(
            "tenant", "execution", 0, -1, 1, ExecutionResultShape.SINGLE, 1, transitionKey, "input",
            ExecutionRedriveIntent.RETRY_FAILED_COMMAND, 0, Optional.of(commandId), Optional.empty());
    }

    private static TransitionWorkerCommand replayCommand(String transitionKey) {
        return new TransitionWorkerCommand(
            "tenant", "execution", 0, -1, 1, ExecutionResultShape.SINGLE, 1, transitionKey, "input",
            ExecutionRedriveIntent.REPLAY, -1, Optional.empty(), Optional.empty());
    }

    private static boolean hasMessage(Throwable failure, String expected) {
        Throwable current = failure;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(expected)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

}
