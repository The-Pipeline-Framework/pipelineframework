package org.pipelineframework.checkout.create_order.e2e;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class RealGrpcBridgeTestProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
            "quarkus.arc.exclude-types",
            "org.pipelineframework.checkout.create_order.e2e.LocalDeliverCaptureIngestClient",
            "quarkus.grpc.server.use-separate-server",
            "true",
            "quarkus.grpc.server.port",
            "9001",
            "quarkus.grpc.clients.deliver-order-orchestrator.host",
            "localhost",
            "quarkus.grpc.clients.deliver-order-orchestrator.port",
            "9001",
            "quarkus.grpc.clients.deliver-order-orchestrator.deadline",
            "10s",
            "quarkus.grpc.clients.deliver-order-orchestrator.plain-text",
            "true");
    }
}
