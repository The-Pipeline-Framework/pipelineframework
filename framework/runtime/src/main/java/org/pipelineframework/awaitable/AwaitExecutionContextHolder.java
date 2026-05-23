package org.pipelineframework.awaitable;

import io.vertx.core.Context;
import io.vertx.core.Vertx;

/**
 * Holder for internal await execution context using Vert.x local context when available and
 * falling back to thread-local storage otherwise.
 *
 * <p>Callers that invoke {@link #set(AwaitExecutionContext)} must restore the previous value or call
 * {@link #clear()} in a {@code finally} block when the execution scope ends. This holder is intentionally
 * narrow: it is only safe while framework-managed await execution remains on the same Vert.x context or,
 * when no Vert.x context exists, on the same thread.</p>
 *
 * <p>Because this falls back to {@link ThreadLocal}, pooled threads can leak stale context if cleanup is missed,
 * and handoff across unrelated reactive or virtual-thread contexts can still lose context unless the framework
 * explicitly propagates it. Code that cannot guarantee same-context execution should use a managed
 * context-propagation mechanism instead of reading {@link #get()} directly.</p>
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
    private static final String CONTEXT_KEY = AwaitExecutionContextHolder.class.getName() + ".context";
    private static final ThreadLocal<AwaitExecutionContext> CURRENT = new ThreadLocal<>();

    private AwaitExecutionContextHolder() {
    }

    public static AwaitExecutionContext get() {
        Context context = Vertx.currentContext();
        if (context != null) {
            try {
                Object stored = context.getLocal(CONTEXT_KEY);
                if (stored instanceof AwaitExecutionContext awaitExecutionContext) {
                    return awaitExecutionContext;
                }
            } catch (UnsupportedOperationException ignored) {
                // Root contexts forbid locals; fall back to thread-local storage.
            }
        }
        return CURRENT.get();
    }

    public static void set(AwaitExecutionContext context) {
        Context vertxContext = Vertx.currentContext();
        if (vertxContext != null) {
            try {
                vertxContext.putLocal(CONTEXT_KEY, context);
                return;
            } catch (UnsupportedOperationException ignored) {
                // Root contexts forbid locals; fall back to thread-local storage.
            }
        }
        CURRENT.set(context);
    }

    public static void clear() {
        Context context = Vertx.currentContext();
        if (context != null) {
            try {
                context.removeLocal(CONTEXT_KEY);
            } catch (UnsupportedOperationException ignored) {
                // Root contexts forbid locals; fall back to thread-local storage.
            }
        }
        CURRENT.remove();
    }
}
