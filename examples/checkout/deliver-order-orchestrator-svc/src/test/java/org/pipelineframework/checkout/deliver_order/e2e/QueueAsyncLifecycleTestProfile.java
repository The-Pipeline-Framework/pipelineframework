package org.pipelineframework.checkout.deliver_order.e2e;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class QueueAsyncLifecycleTestProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        String configuredPolicy = System.getProperty(
            "pipeline.orchestrator.idempotency-policy",
            "CLIENT_KEY_REQUIRED");
        return Map.of(
            "pipeline.orchestrator.mode", "QUEUE_ASYNC",
            "pipeline.orchestrator.state-provider", "memory",
            "pipeline.orchestrator.dispatcher-provider", "event",
            "pipeline.orchestrator.dlq-provider", "log",
            "pipeline.orchestrator.strict-startup", "true",
            "pipeline.orchestrator.idempotency-policy", configuredPolicy,
            "quarkus.otel.sdk.disabled", "true");
    }
}
