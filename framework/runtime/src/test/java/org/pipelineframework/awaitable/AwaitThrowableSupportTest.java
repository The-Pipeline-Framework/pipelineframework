package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.smallrye.mutiny.CompositeException;
import org.junit.jupiter.api.Test;

class AwaitThrowableSupportTest {

    @Test
    void findsAwaitSuspensionInsideCompositeException() {
        AwaitSuspendedException suspended =
            new AwaitSuspendedException("tenant", "execution", "unit", 2);
        CompositeException failure = new CompositeException(
            new IllegalStateException("stream cancelled"),
            suspended);

        assertTrue(AwaitThrowableSupport.containsAwaitSuspension(failure));
        assertSame(suspended, AwaitThrowableSupport.extractAwaitSuspension(failure));
    }
}
