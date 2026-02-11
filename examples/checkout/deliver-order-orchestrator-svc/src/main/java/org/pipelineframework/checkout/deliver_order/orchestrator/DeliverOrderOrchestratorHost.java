package org.pipelineframework.checkout.deliver_order.orchestrator;

import org.pipelineframework.annotation.PipelineOrchestrator;

/**
 * Orchestrator marker for checkout deliver-order pipeline generation.
 */
@PipelineOrchestrator(generateCli = false)
public final class DeliverOrderOrchestratorHost {
    private DeliverOrderOrchestratorHost() {
    }
}
