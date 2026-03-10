package org.pipelineframework;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.telemetry.PipelineTelemetry;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExecutionHooksTest {

    private ExecutionHooks hooks;

    @Mock
    private PipelineTelemetry telemetry;

    @BeforeEach
    void setUp() {
        hooks = new ExecutionHooks();
        hooks.telemetry = telemetry;
        when(telemetry.retryAmplificationGuardEnabled()).thenReturn(false);
    }

    @Test
    void attachUniHooksReturnsWrappedUniWhenGuardDisabled() {
        Uni<String> wrapped = hooks.attachUniHooks(Uni.createFrom().item("ok"), new StopWatch());
        assertNotNull(wrapped);
    }

    @Test
    void attachMultiHooksReturnsWrappedMultiWhenGuardDisabled() {
        Multi<String> wrapped = hooks.attachMultiHooks(Multi.createFrom().items("a", "b"), new StopWatch());
        assertNotNull(wrapped);
    }
}
