package org.pipelineframework;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExecutionInputPolicyTest {

    private ExecutionInputPolicy policy;

    @Mock
    private PipelineOrchestratorConfig orchestratorConfig;

    @BeforeEach
    void setUp() {
        policy = new ExecutionInputPolicy();
        policy.orchestratorConfig = orchestratorConfig;
    }

    @Test
    void rejectsMissingClientKeyWhenRequired() {
        when(orchestratorConfig.idempotencyPolicy()).thenReturn(OrchestratorIdempotencyPolicy.CLIENT_KEY_REQUIRED);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> policy.resolveExecutionKey("tenant", "payload", null));

        assertTrue(ex.getMessage().contains("Idempotency-Key header is required"));
    }

    @Test
    void prefersProvidedClientKeyWhenOptionalPolicy() {
        when(orchestratorConfig.idempotencyPolicy()).thenReturn(OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY);

        String key = policy.resolveExecutionKey("tenant", "payload", " my-key ");

        assertEquals("my-key", key);
    }

    @Test
    void derivesDeterministicServerKeyWhenNoClientKey() {
        when(orchestratorConfig.idempotencyPolicy()).thenReturn(OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY);

        String keyA = policy.resolveExecutionKey("tenant", java.util.Map.of("a", 1), null);
        String keyB = policy.resolveExecutionKey("tenant", java.util.Map.of("a", 1), null);

        assertEquals(keyA, keyB);
        assertNotNull(keyA);
    }

    @Test
    void validateInputShapeAcceptsReactiveTypesOnly() {
        assertNotNull(policy.validateInputShape("plain"));
        assertEquals(null, policy.validateInputShape(Uni.createFrom().item("x")));
        assertEquals(null, policy.validateInputShape(Multi.createFrom().items("x", "y")));
    }

    @Test
    void resolvesMultiSnapshotAndReplaysAsMulti() {
        ExecutionInputSnapshot snapshot = policy.resolveExecutionInputPayload(Multi.createFrom().items("x", "y"))
            .await().indefinitely();
        assertEquals(ExecutionInputShape.MULTI, snapshot.shape());

        Object replay = policy.toReplayInput(snapshot);
        java.util.List<?> replayed = ((Multi<?>) replay).collect().asList().await().indefinitely();
        assertEquals(java.util.List.of("x", "y"), replayed);
    }
}
