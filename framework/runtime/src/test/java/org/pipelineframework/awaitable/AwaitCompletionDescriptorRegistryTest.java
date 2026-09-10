package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AwaitCompletionDescriptorRegistryTest {
    @Test
    void registersCompilerGeneratedDescriptorForDurableLookup() {
        AwaitCompletionDescriptorRegistry registry = new AwaitCompletionDescriptorRegistry();
        AwaitCompletionDescriptor descriptor = descriptor("approval", "Pending", "Decision");

        assertSame(descriptor, registry.register(descriptor));
        assertSame(descriptor, registry.descriptorByStepIdNow("approval"));
        assertSame(descriptor, registry.descriptorByStepId("approval").await().indefinitely());
    }

    @Test
    void rejectsUnknownIdentityInsteadOfReconstructingYaml() {
        AwaitCompletionDescriptorRegistry registry = new AwaitCompletionDescriptorRegistry();

        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> registry.descriptorByStepIdNow("missing"));

        assertEquals("No compiler-generated deferred-completion descriptor is registered for step 'missing'",
            failure.getMessage());
    }

    @Test
    void rejectsConflictingGeneratedDescriptors() {
        AwaitCompletionDescriptorRegistry registry = new AwaitCompletionDescriptorRegistry();
        registry.register(descriptor("approval", "Pending", "Decision"));

        assertThrows(IllegalStateException.class,
            () -> registry.register(descriptor("approval", "OtherPending", "Decision")));
    }

    private AwaitCompletionDescriptor descriptor(String stepId, String input, String output) {
        return new AwaitCompletionDescriptor(stepId, input, output, Duration.ofMinutes(5),
            "interactionId", "interaction-api", Map.of(), List.of());
    }
}
