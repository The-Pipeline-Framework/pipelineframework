package org.pipelineframework.execution;

import org.pipelineframework.runtime.core.RuntimeAdapters;

/**
 * Holder for framework-managed execution context using the runtime adapter-backed context carrier.
 */
public final class PipelineExecutionContextHolder {
    private static final String CONTEXT_KEY = PipelineExecutionContextHolder.class.getName() + ".context";

    private PipelineExecutionContextHolder() {
    }

    public static PipelineExecutionContext get() {
        Object value = RuntimeAdapters.executionContext(CONTEXT_KEY, Object.class);
        if (value instanceof PipelineExecutionContext executionContext) {
            return executionContext;
        }
        return null;
    }

    public static void set(PipelineExecutionContext context) {
        if (context == null) {
            RuntimeAdapters.clearExecutionContext(CONTEXT_KEY);
            return;
        }
        RuntimeAdapters.setExecutionContext(CONTEXT_KEY, context);
    }

    public static void clear() {
        RuntimeAdapters.clearExecutionContext(CONTEXT_KEY);
    }
}
