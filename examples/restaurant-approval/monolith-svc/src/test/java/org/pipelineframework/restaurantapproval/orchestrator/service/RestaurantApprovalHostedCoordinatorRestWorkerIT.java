package org.pipelineframework.restaurantapproval.orchestrator.service;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RestaurantApprovalHostedCoordinatorRestWorkerIT extends RestaurantApprovalSplitWorkerITSupport {

    @TempDir
    Path bundleStoreRoot;

    @Test
    void hostedCoordinatorUsesSeparateRestWorkerForAcceptedAndDeclinedAwaitFlows() throws Exception {
        int workerPort = startApp("hosted-rest-worker", Map.of(
            "pipeline.orchestrator.worker.rest.server-enabled", "true",
            "pipeline.orchestrator.worker.rest.shared-secret", WORKER_SECRET,
            "pipeline.orchestrator.strict-startup", "false"));

        int coordinatorPort = startApp("hosted-rest-coordinator", Map.of(
            "pipeline.orchestrator.control-plane.enabled", "true",
            "pipeline.orchestrator.control-plane.admin-token", CONTROL_PLANE_ADMIN_TOKEN,
            "pipeline.orchestrator.admin.enabled", "true",
            "pipeline.orchestrator.admin.admin-token", CONTROL_PLANE_ADMIN_TOKEN,
            "pipeline.orchestrator.bundles.registry.provider", "file",
            "pipeline.orchestrator.bundles.storage.root", bundleStoreRoot.toString(),
            "pipeline.orchestrator.worker.rest.base-url", "http://localhost:" + workerPort,
            "pipeline.orchestrator.worker.rest.shared-secret", WORKER_SECRET,
            "pipeline.orchestrator.strict-startup", "false"));

        registerAndActivateHostedBundle(coordinatorPort);
        assertHostedAcceptedAndDeclinedFlows(coordinatorPort);
    }

    @Test
    void hostedCoordinatorRejectsSubmitWhenRestWorkerHostsDifferentBundle() throws Exception {
        int workerPort = startApp("hosted-rest-worker-mismatch", Map.of(
            "pipeline.orchestrator.bundle-version-id", "sha256:mismatched-worker-bundle",
            "pipeline.orchestrator.worker.rest.server-enabled", "true",
            "pipeline.orchestrator.worker.rest.shared-secret", WORKER_SECRET,
            "pipeline.orchestrator.strict-startup", "false"));

        int coordinatorPort = startApp("hosted-rest-coordinator-mismatch", Map.of(
            "pipeline.orchestrator.control-plane.enabled", "true",
            "pipeline.orchestrator.control-plane.admin-token", CONTROL_PLANE_ADMIN_TOKEN,
            "pipeline.orchestrator.admin.enabled", "true",
            "pipeline.orchestrator.admin.admin-token", CONTROL_PLANE_ADMIN_TOKEN,
            "pipeline.orchestrator.bundles.registry.provider", "file",
            "pipeline.orchestrator.bundles.storage.root", bundleStoreRoot.toString(),
            "pipeline.orchestrator.worker.rest.base-url", "http://localhost:" + workerPort,
            "pipeline.orchestrator.worker.rest.shared-secret", WORKER_SECRET,
            "pipeline.orchestrator.strict-startup", "false"));

        registerAndActivateHostedBundle(coordinatorPort);
        assertHostedSubmitStatus(coordinatorPort, 503);
    }
}
