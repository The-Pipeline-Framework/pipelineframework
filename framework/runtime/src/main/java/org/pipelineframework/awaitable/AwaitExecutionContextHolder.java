package org.pipelineframework.awaitable;

/**
 * Thread-local holder for internal await execution context.
 */
public final class AwaitExecutionContextHolder {
    private static final ThreadLocal<AwaitExecutionContext> CURRENT = new ThreadLocal<>();

    private AwaitExecutionContextHolder() {
    }

    public static AwaitExecutionContext get() {
        return CURRENT.get();
    }

    public static void set(AwaitExecutionContext context) {
        CURRENT.set(context);
    }

    public static void clear() {
        CURRENT.remove();
    }
}
