package org.pipelineframework.awaitable;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Shared helpers for locating await control-flow signals through wrapper exceptions.
 */
public final class AwaitThrowableSupport {
    private AwaitThrowableSupport() {
    }

    public static boolean containsAwaitSuspension(Throwable failure) {
        return findThrowable(failure, AwaitSuspendedException.class) != null;
    }

    public static AwaitSuspendedException extractAwaitSuspension(Throwable failure) {
        AwaitSuspendedException suspended = findThrowable(failure, AwaitSuspendedException.class);
        if (suspended == null) {
            throw new IllegalStateException("Await suspension failure not found in cause chain", failure);
        }
        return suspended;
    }

    public static <T extends Throwable> T findThrowable(Throwable failure, Class<T> targetType) {
        if (failure == null || targetType == null) {
            return null;
        }
        ArrayDeque<Throwable> queue = new ArrayDeque<>();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        queue.add(failure);
        while (!queue.isEmpty()) {
            Throwable current = queue.removeFirst();
            if (!seen.add(current)) {
                continue;
            }
            if (targetType.isInstance(current)) {
                return targetType.cast(current);
            }
            Throwable cause = current.getCause();
            if (cause != null && cause != current) {
                queue.addLast(cause);
            }
            for (Throwable suppressed : current.getSuppressed()) {
                if (suppressed != null && suppressed != current) {
                    queue.addLast(suppressed);
                }
            }
        }
        return null;
    }
}
