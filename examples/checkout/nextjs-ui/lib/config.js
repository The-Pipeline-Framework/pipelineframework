export function getUiConfig() {
  const baseUrl = process.env.TPF_BASE_URL || "http://127.0.0.1:8080";
  const runtimeBaseUrl = process.env.TPF_RUNTIME_BASE_URL || "http://127.0.0.1:9000";
  let grpcHost = process.env.TPF_GRPC_HOST || "";
  let grpcPort = process.env.TPF_GRPC_PORT || "";

  try {
    const parsedBaseUrl = new URL(baseUrl);
    if (!grpcHost) {
      grpcHost = parsedBaseUrl.hostname || "127.0.0.1";
    }
    if (!grpcPort) {
      grpcPort = parsedBaseUrl.protocol === "https:" ? "8443" : "18080";
    }
  } catch (_error) {
    grpcHost = grpcHost || "127.0.0.1";
    grpcPort = grpcPort || "18080";
  }

  return {
    baseUrl,
    runtimeBaseUrl,
    grpcHost,
    grpcPort,
    protoRoot: process.env.TPF_GRPC_PROTO_DIR || "",
    grpcDeadlineMs: process.env.TPF_GRPC_DEADLINE_MS || "5000",
    tenantId: process.env.TPF_TENANT_ID || "default",
    awaitStepId: process.env.TPF_AWAIT_STEP_ID || ""
  };
}

export function getAwaitOrchestratorTargets(config = getUiConfig()) {
  const checkoutHost = config.grpcHost || "127.0.0.1";
  const checkoutPort = config.grpcPort || "18080";

  return [
    {
      id: "checkout-orchestrator-svc",
      packageName: "org.pipelineframework.tpfgo.checkout",
      host: checkoutHost,
      grpcPort: checkoutPort,
      grpcDeadlineMs: config.grpcDeadlineMs,
      protoRoot: config.protoRoot || "",
      moduleDir: "checkout-orchestrator-svc"
    },
    {
      id: "consumer-validation-orchestrator-svc",
      packageName: "org.pipelineframework.tpfgo.consumer.validation",
      host: checkoutHost,
      grpcPort: process.env.TPF_CONSUMER_AWAIT_GRPC_PORT || "18081",
      grpcDeadlineMs: config.grpcDeadlineMs,
      moduleDir: "consumer-validation-orchestrator-svc"
    },
    {
      id: "restaurant-acceptance-orchestrator-svc",
      packageName: "org.pipelineframework.tpfgo.restaurant.acceptance",
      host: checkoutHost,
      grpcPort: process.env.TPF_RESTAURANT_AWAIT_GRPC_PORT || "18082",
      grpcDeadlineMs: config.grpcDeadlineMs,
      moduleDir: "restaurant-acceptance-orchestrator-svc"
    }
  ];
}
