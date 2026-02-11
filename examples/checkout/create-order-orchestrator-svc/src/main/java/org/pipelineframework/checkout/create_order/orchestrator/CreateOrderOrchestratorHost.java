package org.pipelineframework.checkout.create_order.orchestrator;

import org.pipelineframework.annotation.PipelineOrchestrator;

/**
 * Orchestrator marker for checkout create-order pipeline generation.
 */
@PipelineOrchestrator(generateCli = false)
public final class CreateOrderOrchestratorHost {
    /**
     * Prevents instantiation of this marker host class.
     *
     * The private no-argument constructor enforces non-instantiability.
     */
    private CreateOrderOrchestratorHost() {
    }
}