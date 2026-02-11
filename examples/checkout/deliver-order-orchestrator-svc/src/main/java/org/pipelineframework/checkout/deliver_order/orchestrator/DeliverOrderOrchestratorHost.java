package org.pipelineframework.checkout.deliver_order.orchestrator;

import org.pipelineframework.annotation.PipelineOrchestrator;

/**
 * Orchestrator marker for checkout deliver-order pipeline generation.
 */
@PipelineOrchestrator(generateCli = false)
public final class DeliverOrderOrchestratorHost {
    /**
     * Prevents instantiation of this orchestrator host class.
     *
     * This private constructor enforces that the class is used only as a marker for pipeline generation
     * and should not be instantiated.
     */
    private DeliverOrderOrchestratorHost() {
    }
}