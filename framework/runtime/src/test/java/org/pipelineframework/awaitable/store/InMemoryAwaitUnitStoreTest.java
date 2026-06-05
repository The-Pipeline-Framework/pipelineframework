package org.pipelineframework.awaitable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitUnitCreateCommand;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;

class InMemoryAwaitUnitStoreTest {

    @Test
    void duplicateItemCompletionDoesNotIncrementCompletedItemCount() {
        InMemoryAwaitUnitStore store = new InMemoryAwaitUnitStore();
        store.createOrGet(createCommand()).await().indefinitely();

        var first = store.recordItemCompleted("tenant", "unit-1", "interaction-1", 11_000L).await().indefinitely().orElseThrow();
        var duplicate = store.recordItemCompleted("tenant", "unit-1", "interaction-1", 12_000L).await().indefinitely().orElseThrow();
        var dispatchComplete = store.markDispatchComplete("tenant", "unit-1", 1, 13_000L).await().indefinitely().orElseThrow();

        assertEquals(1, first.completedItemCount());
        assertEquals(1, duplicate.completedItemCount());
        assertEquals(1, dispatchComplete.completedItemCount());
        assertEquals(AwaitUnitStatus.COMPLETED, dispatchComplete.status());
    }

    @Test
    void distinctItemCompletionsIncrementCompletedItemCount() {
        InMemoryAwaitUnitStore store = new InMemoryAwaitUnitStore();
        store.createOrGet(createCommand()).await().indefinitely();

        store.recordItemCompleted("tenant", "unit-1", "interaction-1", 11_000L).await().indefinitely();
        var second = store.recordItemCompleted("tenant", "unit-1", "interaction-2", 12_000L).await().indefinitely().orElseThrow();
        var dispatchComplete = store.markDispatchComplete("tenant", "unit-1", 2, 13_000L).await().indefinitely().orElseThrow();

        assertEquals(2, second.completedItemCount());
        assertEquals(2, dispatchComplete.completedItemCount());
        assertEquals(AwaitUnitStatus.COMPLETED, dispatchComplete.status());
    }

    @Test
    void importedCompletedItemKeysPreserveDuplicateProtection() {
        InMemoryAwaitUnitStore store = new InMemoryAwaitUnitStore();
        store.importRecord(new AwaitUnitRecord(
            "tenant",
            "unit-1",
            "execution-1",
            "AwaitPaymentProvider",
            1,
            "ONE_TO_ONE",
            1L,
            AwaitUnitStatus.WAITING_EXTERNAL,
            null,
            2,
            1,
            java.util.Set.of("item:0"),
            true,
            10_000L,
            10_000L,
            9_999_999_999L)).await().indefinitely();

        var duplicate = store.recordItemCompleted("tenant", "unit-1", "item:0", 11_000L).await().indefinitely().orElseThrow();
        var second = store.recordItemCompleted("tenant", "unit-1", "item:1", 12_000L).await().indefinitely().orElseThrow();

        assertEquals(1, duplicate.completedItemCount());
        assertEquals(2, second.completedItemCount());
        assertEquals(AwaitUnitStatus.COMPLETED, second.status());
    }

    private static AwaitUnitCreateCommand createCommand() {
        return new AwaitUnitCreateCommand(
            "tenant",
            "unit-1",
            "execution-1",
            "AwaitPaymentProvider",
            1,
            "ONE_TO_ONE",
            10_000L,
            9_999_999_999L);
    }
}
