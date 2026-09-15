package org.pipelineframework.awaitable;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitUnitRecordTest {

    @Test
    void copiesCompletionKeysAndExposesContinuationFacts() {
        Set<String> keys = new HashSet<>(Set.of("item-0", AwaitUnitRecord.continuationCompletionKey(2)));
        AwaitUnitRecord record = new AwaitUnitRecord("tenant", "unit", "execution", "step", 0, "ONE_TO_MANY",
            1L, AwaitUnitStatus.WAITING_EXTERNAL, "interaction", 3, 1, keys, false, 10L, 11L, 100L);

        keys.add("later-mutation");
        assertEquals(Set.of("item-0", "continuation:2"), record.completedItemKeys());
        assertEquals(1, record.completedItemCount());
        assertEquals(1, record.completedContinuationItemCount());
        assertTrue(record.hasContinuationCompletionFacts());
        assertTrue(record.hasContinuationCompletionFact(2));
        assertFalse(record.hasContinuationCompletionFact(1));
        assertThrows(UnsupportedOperationException.class, () -> record.completedItemKeys().add("extra"));
        assertThrows(IllegalArgumentException.class, () -> AwaitUnitRecord.continuationCompletionKey(-1));
    }

    @Test
    void validatesCompletionCountsAndClassifiesTerminalStatuses() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitUnitRecord("tenant", "unit", "execution", "step",
            0, "ONE_TO_MANY", 1L, AwaitUnitStatus.WAITING_EXTERNAL, "interaction", 1, 1,
            Set.of("item-0", "item-1"), false, 10L, 11L, 100L));

        assertFalse(AwaitUnitStatus.WAITING_EXTERNAL.terminal());
        for (AwaitUnitStatus status : Set.of(AwaitUnitStatus.COMPLETED, AwaitUnitStatus.FAILED,
            AwaitUnitStatus.TIMED_OUT, AwaitUnitStatus.CANCELLED, AwaitUnitStatus.EXPIRED)) {
            assertTrue(status.terminal(), status.name());
        }
    }
}
