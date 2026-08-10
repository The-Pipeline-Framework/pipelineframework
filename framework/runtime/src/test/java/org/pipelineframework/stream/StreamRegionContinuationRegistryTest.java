package org.pipelineframework.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;

class StreamRegionContinuationRegistryTest {

    private static final ResumableSourceDescriptor DESCRIPTOR =
        new ResumableSourceDescriptor("deterministic", "payments", "fixture-v1");

    @Test
    void rejectsDuplicateDescriptorDeterministically() {
        StreamRegionContinuation first = mock(StreamRegionContinuation.class);
        StreamRegionContinuation second = mock(StreamRegionContinuation.class);
        when(first.descriptor()).thenReturn(DESCRIPTOR);
        when(second.descriptor()).thenReturn(DESCRIPTOR);
        @SuppressWarnings("unchecked")
        Instance<StreamRegionContinuation> continuations = mock(Instance.class);
        when(continuations.iterator()).thenReturn(List.of(first, second).iterator());

        StreamRegionContinuationRegistry registry = new StreamRegionContinuationRegistry();
        registry.continuations = continuations;

        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> registry.find(DESCRIPTOR));
        assertEquals("Multiple generated stream continuations match " + DESCRIPTOR, failure.getMessage());
    }
}
