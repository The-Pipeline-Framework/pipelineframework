package org.pipelineframework.branching;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

/** Per-run, per-item provenance used to keep synthetic observers attached to executed parents. */
public final class BranchExecutionTracker {
    private final ReferenceQueue<Object> collectedItems = new ReferenceQueue<>();
    private final Map<IdentityWeakReference, Boolean> lastStepSkipped = new HashMap<>();

    public synchronized void recordSkipped(Object item) {
        expungeCollectedItems();
        if (item != null) {
            lastStepSkipped.put(new IdentityWeakReference(item, collectedItems), Boolean.TRUE);
        }
    }

    public synchronized void recordExecuted(Object output) {
        expungeCollectedItems();
        if (output != null) {
            lastStepSkipped.put(new IdentityWeakReference(output, collectedItems), Boolean.FALSE);
        }
    }

    public synchronized boolean wasLastStepSkipped(Object item) {
        expungeCollectedItems();
        return item != null
            && Boolean.TRUE.equals(lastStepSkipped.get(new IdentityWeakReference(item)));
    }

    private void expungeCollectedItems() {
        IdentityWeakReference collected;
        while ((collected = (IdentityWeakReference) collectedItems.poll()) != null) {
            lastStepSkipped.remove(collected);
        }
    }

    private static final class IdentityWeakReference extends WeakReference<Object> {
        private final int identityHash;

        private IdentityWeakReference(Object referent) {
            super(referent);
            this.identityHash = System.identityHashCode(referent);
        }

        private IdentityWeakReference(Object referent, ReferenceQueue<Object> queue) {
            super(referent, queue);
            this.identityHash = System.identityHashCode(referent);
        }

        @Override
        public int hashCode() {
            return identityHash;
        }

        @Override
        public boolean equals(Object candidate) {
            if (this == candidate) {
                return true;
            }
            if (!(candidate instanceof IdentityWeakReference other)) {
                return false;
            }
            Object referent = get();
            return referent != null && referent == other.get();
        }
    }
}
