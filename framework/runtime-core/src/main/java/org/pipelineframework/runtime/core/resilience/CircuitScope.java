package org.pipelineframework.runtime.core.resilience;

/**
 * Visibility of circuit state and the protection guarantee it provides.
 */
public enum CircuitScope {
    /** State is visible only to calls made by one runtime process. */
    LOCAL_PROCESS,

    /** State is intended to be shared by every runtime process protecting the dependency. */
    SHARED_DEPENDENCY
}
