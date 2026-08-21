package org.pipelineframework.branching;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

/** Per-run, per-item provenance used to keep synthetic observers attached to executed parents. */
public final class BranchExecutionTracker {
    private final Map<Object, Boolean> lastStepSkipped =
        Collections.synchronizedMap(new IdentityHashMap<>());

    public void recordSkipped(Object item) {
        if (item != null) {
            lastStepSkipped.put(item, Boolean.TRUE);
        }
    }

    public void recordExecuted(Object output) {
        if (output != null) {
            lastStepSkipped.put(output, Boolean.FALSE);
        }
    }

    public boolean wasLastStepSkipped(Object item) {
        return item != null && Boolean.TRUE.equals(lastStepSkipped.get(item));
    }
}
