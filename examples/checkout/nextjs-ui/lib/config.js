export function getUiConfig() {
  const baseUrl = process.env.TPF_BASE_URL || "http://127.0.0.1:8080";
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
    grpcHost,
    grpcPort,
    protoRoot: process.env.TPF_GRPC_PROTO_DIR || "",
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
      protoRoot: config.protoRoot || "",
      moduleDir: "checkout-orchestrator-svc"
    },
    {
      id: "consumer-validation-orchestrator-svc",
      packageName: "org.pipelineframework.tpfgo.consumer.validation",
      host: checkoutHost,
      grpcPort: process.env.TPF_CONSUMER_AWAIT_GRPC_PORT || "18081",
      moduleDir: "consumer-validation-orchestrator-svc"
    },
    {
      id: "restaurant-acceptance-orchestrator-svc",
      packageName: "org.pipelineframework.tpfgo.restaurant.acceptance",
      host: checkoutHost,
      grpcPort: process.env.TPF_RESTAURANT_AWAIT_GRPC_PORT || "18082",
      moduleDir: "restaurant-acceptance-orchestrator-svc"
    }
  ];
}
