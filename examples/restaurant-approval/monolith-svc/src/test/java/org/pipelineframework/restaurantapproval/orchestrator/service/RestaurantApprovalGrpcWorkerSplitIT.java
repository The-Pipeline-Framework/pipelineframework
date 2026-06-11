package org.pipelineframework.restaurantapproval.orchestrator.service;

import java.util.Map;
import org.junit.jupiter.api.Test;

class RestaurantApprovalGrpcWorkerSplitIT extends RestaurantApprovalSplitWorkerITSupport {

    @Test
    void coordinatorUsesSeparateGrpcWorkerForAcceptedAndDeclinedAwaitFlows() throws Exception {
        int workerPort = startApp("grpc-worker", Map.of(
            "pipeline.orchestrator.worker.grpc.server-enabled", "true",
            "pipeline.orchestrator.worker.grpc.shared-secret", WORKER_SECRET,
            "pipeline.orchestrator.strict-startup", "false"));

        int coordinatorPort = startApp("grpc-coordinator", Map.of(
            "pipeline.orchestrator.worker.grpc.endpoint", "localhost:" + workerPort,
            "pipeline.orchestrator.worker.grpc.plaintext", "true",
            "pipeline.orchestrator.worker.grpc.shared-secret", WORKER_SECRET,
            "pipeline.orchestrator.strict-startup", "false"));

        assertAcceptedAndDeclinedFlows(coordinatorPort);
    }
}
