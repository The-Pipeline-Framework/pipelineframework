package org.pipelineframework.branching;

import java.lang.ref.WeakReference;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class BranchExecutionTrackerTest {

    @Test
    void doesNotRetainProcessedItemsForTheLifetimeOfTheRun() {
        BranchExecutionTracker tracker = new BranchExecutionTracker();
        WeakReference<Object> processed = recordTemporaryItem(tracker);

        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            while (processed.get() != null) {
                System.gc();
                Thread.sleep(10);
            }
        });
        assertNull(processed.get());
    }

    private static WeakReference<Object> recordTemporaryItem(BranchExecutionTracker tracker) {
        Object item = new Object();
        tracker.recordSkipped(item);
        return new WeakReference<>(item);
    }
}
