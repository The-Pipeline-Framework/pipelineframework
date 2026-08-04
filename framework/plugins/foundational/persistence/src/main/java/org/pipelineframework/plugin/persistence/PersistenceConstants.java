package org.pipelineframework.plugin.persistence;

/**
 * Shared persistence plugin constants.
 */
public final class PersistenceConstants {
    public static final String SESSION_ON_DEMAND_KEY = "hibernate.reactive.panache.sessionOnDemand";
    public static final String VTHREAD_PROVIDER_CLASS =
        "org.pipelineframework.plugin.persistence.provider.VThreadPersistenceProvider";
    public static final String REACTIVE_PROVIDER_CLASS =
        "org.pipelineframework.plugin.persistence.provider.ReactivePanachePersistenceProvider";
    public static final String VTHREAD_PROVIDER_SIMPLE = "VThreadPersistenceProvider";
    public static final String REACTIVE_PROVIDER_SIMPLE = "ReactivePanachePersistenceProvider";

    /**
     * Prevents instantiation of this utility class.
     */
    private PersistenceConstants() {
    }
}