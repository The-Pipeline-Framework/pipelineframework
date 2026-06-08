import fs from "node:fs";
import path from "node:path";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

import { getAwaitOrchestratorTargets, getUiConfig } from "./config.js";
import {
  buildCheckoutCompletionPayload,
  buildCheckoutOrder,
  normalizePendingInteraction
} from "./checkout-ui.js";

const orchestratorServiceCache = new Map();

function toNumber(value, fallback = 0) {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "bigint") {
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
  }

  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  if (typeof value?.toNumber === "function") {
    const num = value.toNumber();
    return Number.isFinite(num) ? num : fallback;
  }

  return fallback;
}

function parseAddressFromHostPort(host, grpcPort) {
  const parsedHost = String(host || "127.0.0.1").trim() || "127.0.0.1";
  const parsedPort = String(grpcPort || "18080").trim();
  return `${parsedHost}:${parsedPort}`;
}

function grpcProtoPath(target, name) {
  const protoRoot = String(target.protoRoot || "").trim();
  const fallbackRoot = path.resolve(
    process.cwd(),
    "..",
    target.moduleDir || "checkout-orchestrator-svc",
    "target",
    "generated-sources",
    "proto"
  );
  const root = protoRoot ? path.resolve(protoRoot) : fallbackRoot;
  const protoPath = path.join(root, name);
  if (!fs.existsSync(protoPath)) {
    throw new Error(
      `TPF gRPC proto not found at ${protoPath}. Build checkout modules first or set TPF_GRPC_PROTO_DIR`
    );
  }
  return protoPath;
}

function getServiceNamespace(packageObject, packageName) {
  return packageName
    .split(".")
    .reduce((scope, segment) => (scope ? scope[segment] : undefined), packageObject);
}

function getTargetClient(target) {
  const key = `${parseAddressFromHostPort(target.grpcHost, target.grpcPort)}|${target.moduleDir || "checkout-orchestrator-svc"}|${target.packageName}`;
  if (orchestratorServiceCache.has(key)) {
    return orchestratorServiceCache.get(key);
  }

  const orchestratorProto = grpcProtoPath(target, "orchestrator.proto");
  const pipelineTypesProto = grpcProtoPath(target, "pipeline-types.proto");

  const packageDefinition = protoLoader.loadSync([orchestratorProto, pipelineTypesProto], {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: [path.dirname(orchestratorProto)]
  });

  const packageObject = grpc.loadPackageDefinition(packageDefinition);
  const packageName = String(target.packageName || "org.pipelineframework.tpfgo.checkout").trim();
  const packageNamespace = getServiceNamespace(packageObject, packageName);
  const serviceCtor = packageNamespace?.OrchestratorService;
  if (!serviceCtor) {
    throw new Error(`Unable to load OrchestratorService for ${packageName}`);
  }

  const client = new serviceCtor(
    parseAddressFromHostPort(target.grpcHost, target.grpcPort),
    grpc.credentials.createInsecure()
  );
  orchestratorServiceCache.set(key, client);
  return client;
}

function runGrpcUnaryToTarget(target, methodName, request) {
  const client = getTargetClient(target);
  if (typeof client?.[methodName] !== "function") {
    throw new Error(`TPF gRPC client does not expose ${methodName} on OrchestratorService`);
  }

  const endpoint = parseAddressFromHostPort(target.grpcHost, target.grpcPort);
  return new Promise((resolve, reject) => {
    client[methodName](request, (error, response) => {
      if (error) {
        reject(
          new Error(
            `TPF gRPC request failed (${methodName}) for ${endpoint}: ${error.message || "unknown error"}`
          )
        );
      } else {
        resolve(response);
      }
    });
  });
}

function runGrpcUnary(methodName, request) {
  const [defaultTarget] = getAwaitOrchestratorTargets();
  return runGrpcUnaryToTarget(defaultTarget, methodName, request);
}

function toExecutionStatus(response) {
  return {
    executionId: String(response?.execution_id || ""),
    status: String(response?.status || "UNKNOWN"),
    stepIndex: toNumber(response?.current_step_index, 0),
    attempt: toNumber(response?.attempt, 0),
    version: toNumber(response?.version, 0),
    nextDueEpochMs: toNumber(response?.next_due_epoch_ms, 0),
    updatedAtEpochMs: toNumber(response?.updated_at_epoch_ms, 0),
    errorCode: String(response?.error_code || ""),
    errorMessage: String(response?.error_message || "")
  };
}

function toExecutionResult(response) {
  return {
    items: response?.items ?? []
  };
}

function toExecutionId(interaction) {
  return String(interaction?.execution_id || "");
}

function normalizeInteraction(interaction, targetId) {
  const fallbackPayload = {
    requestId: interaction?.correlation_id ? String(interaction.correlation_id) : "",
    orderId: "",
    customerId: "",
    restaurantId: "",
    items: [],
    totalAmount: "",
    currency: ""
  };

  const normalized = normalizePendingInteraction({
    ...interaction,
    requestPayload: interaction?.requestPayload ?? fallbackPayload,
    executionId: toExecutionId(interaction)
  });

  return {
    ...normalized,
    executionId: toExecutionId(interaction),
    targetId: String(targetId || "")
  };
}

function getAwaitTargets() {
  return getAwaitOrchestratorTargets();
}

function getTarget(targetId) {
  const normalizedTargetId = String(targetId || "").trim();
  return (
    getAwaitTargets().find((target) => String(target.id || "").trim() === normalizedTargetId) ||
    getAwaitTargets().find((target) => String(target.packageName || "").trim() === normalizedTargetId) ||
    getAwaitTargets()[0]
  );
}

export async function submitOrder(form) {
  const config = getUiConfig();
  const request = buildCheckoutOrder(form);

  const response = await runGrpcUnary("RunAsync", {
    input: {
      requestId: String(request.requestId || ""),
      customerId: String(request.customerId || ""),
      restaurantId: String(request.restaurantId || ""),
      items: (request.items || []).map((item) => ({
        sku: String(item.sku || ""),
        quantity: toNumber(item.quantity, 1)
      })),
      totalAmount: String(request.totalAmount || ""),
      currency: String(request.currency || "")
    },
    tenant_id: String(config.tenantId || "default"),
    idempotency_key: `order-${request.requestId}`
  });

  return {
    executionId: String(response?.execution_id || ""),
    duplicate: Boolean(response?.duplicate),
    statusUrl: String(response?.status_url || ""),
    acceptedAtEpochMs: toNumber(response?.accepted_at_epoch_ms, 0)
  };
}

export async function fetchRunStatus(executionId) {
  const config = getUiConfig();
  const response = await runGrpcUnary("GetExecutionStatus", {
    tenant_id: String(config.tenantId || "default"),
    execution_id: String(executionId)
  });
  return toExecutionStatus(response);
}

export async function fetchRunResult(executionId) {
  const config = getUiConfig();
  const response = await runGrpcUnary("GetExecutionResult", {
    tenant_id: String(config.tenantId || "default"),
    execution_id: String(executionId)
  });
  return toExecutionResult(response);
}

export function buildListPendingRequest() {
  const config = getUiConfig();
  const request = {
    tenant_id: String(config.tenantId || "default"),
    limit: 100
  };
  const awaitedStepId = String(config.awaitStepId || "").trim();
  if (awaitedStepId) {
    request.step_id = awaitedStepId;
  }
  return request;
}

async function queryPendingInteractions(targets, request) {
  const settled = await Promise.allSettled(
    targets.map((target) => runGrpcUnaryToTarget(target, "ListPendingAwait", request))
  );

  const interactions = [];
  const failedTargets = [];
  settled.forEach((result, index) => {
    const target = targets[index];
    if (result.status === "fulfilled") {
      const items = result.value?.interactions || [];
      for (const interaction of items) {
        interactions.push({
          ...normalizeInteraction(interaction || {}, target.id),
          status: String(interaction?.status || ""),
          deadlineEpochMs: toNumber(interaction?.deadline_epoch_ms, 0)
        });
      }
    } else {
      failedTargets.push(target.id);
    }
  });

  return { interactions, failedTargets };
}

export async function fetchPendingInteractions() {
  const config = getUiConfig();
  const targets = getAwaitTargets();
  const request = buildListPendingRequest();
  const primaryResult = await queryPendingInteractions(targets, request);

  const awaitedStepId = String(config.awaitStepId || "").trim();
  if (
    awaitedStepId &&
    primaryResult.interactions.length === 0 &&
    primaryResult.failedTargets.length === 0
  ) {
    const fallbackRequest = { ...request };
    delete fallbackRequest.step_id;
    const fallbackResult = await queryPendingInteractions(targets, fallbackRequest);
    return fallbackResult.interactions;
  }

  if (primaryResult.interactions.length > 0 || primaryResult.failedTargets.length === 0) {
    return primaryResult.interactions;
  }

  throw new Error(`Unable to reach interaction endpoints for: ${primaryResult.failedTargets.join(", ")}.`);
}

export async function completeInteraction({ interactionId, payload, targetId }) {
  const completionPayload = buildCheckoutCompletionPayload(payload);
  const target = getTarget(targetId);
  const config = getUiConfig();
  const response = await runGrpcUnaryToTarget(target, "CompleteAwait", {
    tenant_id: String(config.tenantId || "default"),
    interaction_id: String(interactionId || ""),
    idempotency_key: `complete-${interactionId}-manual`,
    actor: "tpfgo-checkout-ui",
    response_json: JSON.stringify(completionPayload)
  });

  return {
    interactionId: String(response?.interaction_id || ""),
    executionId: String(response?.execution_id || ""),
    stepId: String(response?.step_id || ""),
    status: String(response?.status || ""),
    duplicate: Boolean(response?.duplicate),
    targetId: String(target.id || "")
  };
}

export async function fetchExecutionStatus(executionId) {
  return fetchRunStatus(executionId);
}

export async function fetchExecutionResult(executionId) {
  return fetchRunResult(executionId);
}

export function getAvailableAwaitTargets() {
  return getAwaitTargets();
}
