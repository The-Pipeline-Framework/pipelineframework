package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class AwaitLiveCompletionRegistryTest {

    @Test
    void closeOnlyRemovesTheOwningLiveSession() {
        AwaitLiveCompletionRegistry registry = new AwaitLiveCompletionRegistry();
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "await",
            String.class.getName(),
            String.class.getName(),
            "ONE_TO_ONE",
            Duration.ofSeconds(30),
            "interactionId",
            "kafka",
            Map.of(),
            List.of());
        LiveAwaitSession<String> owner = registry.open(descriptor, "tenant-1", "unit-1");
        LiveAwaitSession<String> unrelated = new LiveAwaitSession<>("unit-1", String.class, () -> {
        });

        registry.close("tenant-1", "unit-1", unrelated);

        assertThrows(
            IllegalStateException.class,
            () -> registry.open(descriptor, "tenant-1", "unit-1"));
        registry.close("tenant-1", "unit-1", owner);
        assertDoesNotThrow(() -> registry.open(descriptor, "tenant-1", "unit-1"));
    }
}
