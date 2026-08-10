package org.pipelineframework.orchestrator;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Shared monitor for the selectable in-memory control-plane stores.
 *
 * <p>The stores remain separate projections, but stream-page materialisation and a linked item
 * continuation's APPLIED-plus-credit transition must not become visible as two local writes.</p>
 */
@ApplicationScoped
public class InMemoryControlPlaneTransactionLock {

    private final Object monitor = new Object();

    public Object monitor() {
        return monitor;
    }
}
