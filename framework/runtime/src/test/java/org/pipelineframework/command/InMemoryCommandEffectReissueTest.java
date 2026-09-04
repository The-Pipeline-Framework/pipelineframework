package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.execution.PipelineExecutionContext;

class InMemoryCommandEffectReissueTest {

    @Test
    void reissueModelRequiresPurposeOccurrenceAndAuditReason() {
        assertThrows(IllegalArgumentException.class, () -> CommandAttemptAdmission.reissue(" "));
        assertThrows(IllegalArgumentException.class, () -> new CommandEffectAttemptRecord(
            "attempt-1", " ", 1, "execution-1", CommandAttemptPurpose.INITIAL,
            CommandEffectStatus.PENDING, Optional.empty(), null, null, Optional.empty(),
            Optional.empty(), 1L, 1L));
        assertThrows(IllegalArgumentException.class, () -> new CommandEffectAttemptRecord(
            "attempt-1", "occurrence-1", 1, "execution-1", CommandAttemptPurpose.REISSUE,
            CommandEffectStatus.PENDING, Optional.empty(), null, null, Optional.empty(),
            Optional.empty(), 1L, 1L));
    }

    @Test
    void reissueRequiresSucceededAndConcurrentClaimsHaveOneWinner() {
        InMemoryCommandEffectStore store = new InMemoryCommandEffectStore();
        CommandRequest<Input> initial = request("command-1", "command-1", "attempt-1", "execution-1");
        store.createPending(initial, 1L).await().atMost(Duration.ofSeconds(5));

        assertThrows(IllegalStateException.class, () -> store.createAttempt(
                request("command-1", "occurrence-invalid", "attempt-invalid", "execution-invalid"),
                CommandAttemptAdmission.reissue("approved"),
                2L)
            .await().atMost(Duration.ofSeconds(5)));

        store.markDispatching("tenant-a", "command-1", "attempt-1", 2L)
            .await().atMost(Duration.ofSeconds(5));
        store.markSucceeded("tenant-a", "command-1", "attempt-1", new Output("first"), 3L)
            .await().atMost(Duration.ofSeconds(5));

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            List<CompletableFuture<Boolean>> claims = List.of(
                claim(executor, store, request("command-1", "occurrence-a", "attempt-a", "execution-2")),
                claim(executor, store, request("command-1", "occurrence-b", "attempt-b", "execution-3")));
            List<Boolean> results = claims.stream().map(CompletableFuture::join).toList();

            assertEquals(1, results.stream().filter(Boolean::booleanValue).count());
        }

        CommandEffectRecord retained = store.find("tenant-a", "command-1")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow();
        assertEquals(CommandEffectStatus.PENDING, retained.status());
        assertEquals(2, retained.attempts().size());
        assertEquals(CommandAttemptPurpose.REISSUE, retained.currentAttempt().purpose());
        assertEquals("approved", retained.currentAttempt().reason().orElseThrow());
    }

    private static CompletableFuture<Boolean> claim(
        ExecutorService executor,
        InMemoryCommandEffectStore store,
        CommandRequest<Input> request
    ) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                store.createAttempt(request, CommandAttemptAdmission.reissue("approved"), 4L)
                    .await().atMost(Duration.ofSeconds(5));
                return true;
            } catch (RuntimeException expectedConflict) {
                return false;
            }
        }, executor);
    }

    private static CommandRequest<Input> request(
        String commandId,
        String occurrenceId,
        String attemptId,
        String executionId
    ) {
        CommandDescriptor descriptor = new CommandDescriptor(
            "Write", "write", Input.class.getName(), Output.class.getName(), "generator",
            CommandDuplicatePolicy.RETURN_RECORDED, Map.of());
        return new CommandRequest<>(
            descriptor,
            commandId,
            occurrenceId,
            attemptId,
            new Input("value"),
            new PipelineExecutionContext("tenant-a", executionId, 0),
            Map.of());
    }

    record Input(String value) {
    }

    record Output(String value) {
    }
}
