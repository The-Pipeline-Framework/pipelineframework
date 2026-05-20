package org.pipelineframework.awaitable;

/**
 * Thread-local holder for internal await execution context.
 *
 * <p>Callers that invoke {@link #set(AwaitExecutionContext)} must restore the previous value or call
 * {@link #clear()} in a {@code finally} block when the execution scope ends. This holder is intentionally
 * narrow: it is only safe while framework-managed await execution remains on the same thread.</p>
 *
 * <p>Because this is backed by {@link ThreadLocal}, pooled threads can leak stale context if cleanup is missed,
 * and reactive thread-hopping or virtual-thread handoff can lose context unless the framework explicitly
 * propagates it. Code that cannot guarantee same-thread execution should use a managed context-propagation
 * mechanism instead of reading {@link #get()} directly.</p>
 *
 * <p>TODO: replace this with a framework-managed scope abstraction, for example Java {@code ScopedValue}
 * where suitable or Quarkus/SmallRye context propagation, so cleanup is enforced centrally.</p>
 *
 * <ul>
 * <li>{@link #get()} returns the current thread's context, or {@code null} when none is installed.</li>
 * <li>{@link #set(AwaitExecutionContext)} installs context for the current thread only.</li>
 * <li>{@link #clear()} removes the current thread's context and should be called at scope end.</li>
 * </ul>
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
