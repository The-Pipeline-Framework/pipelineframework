package org.pipelineframework.restaurantapproval.orchestrator.service;

import java.util.Map;
import org.junit.jupiter.api.Test;

class RestaurantApprovalRestWorkerSplitIT extends RestaurantApprovalSplitWorkerITSupport {

    @Test
    void coordinatorUsesSeparateRestWorkerForAcceptedAndDeclinedAwaitFlows() throws Exception {
        int workerPort = startApp("rest-worker", Map.of(
            "pipeline.orchestrator.worker.rest.server-enabled", "true",
            "pipeline.orchestrator.worker.rest.shared-secret", WORKER_SECRET,
            "pipeline.orchestrator.strict-startup", "false"));

        int coordinatorPort = startApp("rest-coordinator", Map.of(
            "pipeline.orchestrator.worker.rest.base-url", "http://localhost:" + workerPort,
            "pipeline.orchestrator.worker.rest.shared-secret", WORKER_SECRET,
            "pipeline.orchestrator.worker.rest.request-timeout", "PT2M",
            "pipeline.orchestrator.strict-startup", "false"));

        assertAcceptedAndDeclinedFlows(coordinatorPort);
    }
}
