import fs from "node:fs";
import path from "node:path";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

import { getAwaitOrchestratorTargets, getUiConfig } from "./config.js";
import {
  buildCheckoutCompletionPayload,
  buildCheckoutOrder,
  isTerminalAwaitStatus,
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

function fieldValue(container, ...keys) {
  for (const key of keys) {
    const value = container?.[key];
    if (value !== undefined && value !== null) {
      return value;
    }
  }
  return undefined;
}

function toText(value) {
  return value === null || value === undefined ? "" : String(value);
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
  const host = String(target.grpcHost || target.host || "127.0.0.1").trim();
  const key = `${parseAddressFromHostPort(host, target.grpcPort)}|${target.moduleDir || "checkout-orchestrator-svc"}|${target.packageName}`;
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
    parseAddressFromHostPort(host, target.grpcPort),
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

  const endpoint = parseAddressFromHostPort(target.grpcHost || target.host || "127.0.0.1", target.grpcPort);
  return new Promise((resolve, reject) => {
    client[methodName](request, (error, response) => {
      if (error) {
        reject(error);
      } else {
        resolve(response);
      }
    });
  });
}

function isExecutionNotFoundError(error) {
  const message = String(error?.message || "").toLowerCase();
  return (
    error?.code === 5 ||
    message.includes("execution not found") ||
    message.includes("not found") ||
    message.includes("jakarta.ws.rs.notfoundexception")
  );
}

function getOrderedTargets(preferredTargetId) {
  const normalized = String(preferredTargetId || "").trim();
  if (!normalized) {
    return getAwaitOrchestratorTargets();
  }

  const targets = getAwaitOrchestratorTargets();
  const preferred = getTarget(normalized);
  if (!preferred) {
    return targets;
  }
  const ordered = [];
  const targetId = String(preferred.id || "").trim();
  for (const target of targets) {
    if (String(target.id || "").trim() === targetId) {
      ordered.unshift(target);
    } else {
      ordered.push(target);
    }
  }
  return ordered;
}

function wrapGrpcError(error, methodName, endpoint) {
  const details = String(error?.message || "unknown error");
  const errorCode = error && typeof error === "object" && "code" in error ? ` (${error.code})` : "";
  return new Error(`TPF gRPC request failed (${methodName}) for ${endpoint}: ${details}${errorCode}`);
}

async function runGrpcUnary(methodName, request) {
  const [defaultTarget] = getAwaitOrchestratorTargets();
  return runGrpcUnaryToTarget(defaultTarget, methodName, request);
}

async function runGrpcUnaryAcrossTargets(
  methodName,
  request,
  preferredTargetId = "",
  allowExecutionNotFoundFallback = false
) {
  const targets = getOrderedTargets(preferredTargetId);
  const errors = [];
  for (const target of targets) {
    try {
      return {
        response: await runGrpcUnaryToTarget(target, methodName, request),
        target
      };
    } catch (error) {
      const endpoint = parseAddressFromHostPort(target.grpcHost || target.host || "127.0.0.1", target.grpcPort);
      const wrapped = wrapGrpcError(error, methodName, endpoint);
      if (!allowExecutionNotFoundFallback || !isExecutionNotFoundError(error)) {
        errors.push(wrapped);
        continue;
      }

      if (targets.length === 1) {
        throw wrapped;
      }

      errors.push(wrapped);
    }
  }

  const endpointErrors = errors.length ? errors.map((error) => error.message).join(" | ") : "";
  throw new Error(endpointErrors || `TPF gRPC request failed (${methodName}) for all configured execution services`);
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

function parseRequestPayload(interaction, fallbackPayload) {
  const requestPayload = fieldValue(interaction, "requestPayload", "request_payload");
  if (requestPayload !== null && requestPayload !== undefined) {
    if (typeof requestPayload === "string") {
      try {
        return JSON.parse(requestPayload);
      } catch (_error) {
        return fallbackPayload;
      }
    }
    return requestPayload;
  }

  const rawPayload = fieldValue(interaction, "requestPayloadJson", "request_payload_json") || "";
  if (typeof rawPayload !== "string" || rawPayload.length === 0) {
    return fallbackPayload;
  }

  try {
    return JSON.parse(rawPayload);
  } catch (_error) {
    return fallbackPayload;
  }
}

export function normalizeGrpcAwaitInteraction(interaction, targetId) {
  const requestId = toText(fieldValue(
    interaction,
    "requestId",
    "request_id",
    "correlationId",
    "correlation_id"
  ));
  const fallbackPayload = {
    requestId,
    orderId: requestId,
    customerId: "",
    restaurantId: "",
    items: [],
    totalAmount: "",
    currency: ""
  };
  const requestPayload = parseRequestPayload(interaction, fallbackPayload);
  const normalizedInteraction = {
    interactionId: toText(fieldValue(interaction, "interactionId", "interaction_id")),
    correlationId: toText(fieldValue(interaction, "correlationId", "correlation_id")),
    executionId: toText(fieldValue(interaction, "executionId", "execution_id")),
    stepId: toText(fieldValue(interaction, "stepId", "step_id")),
    stepIndex: toNumber(fieldValue(interaction, "stepIndex", "step_index"), 0),
    outputType: toText(fieldValue(interaction, "outputType", "output_type")),
    status: toText(fieldValue(interaction, "status")),
    transportType: toText(fieldValue(interaction, "transportType", "transport_type")),
    deadlineEpochMs: toNumber(fieldValue(interaction, "deadlineEpochMs", "deadline_epoch_ms"), 0),
    createdAtEpochMs: toNumber(fieldValue(interaction, "createdAtEpochMs", "created_at_epoch_ms"), 0),
    updatedAtEpochMs: toNumber(fieldValue(interaction, "updatedAtEpochMs", "updated_at_epoch_ms"), 0),
    requestPayload
  };

  const normalized = normalizePendingInteraction({
    ...normalizedInteraction
  });

  return {
    ...normalized,
    correlationId: normalizedInteraction.correlationId,
    stepIndex: normalizedInteraction.stepIndex,
    createdAtEpochMs: normalizedInteraction.createdAtEpochMs,
    updatedAtEpochMs: normalizedInteraction.updatedAtEpochMs,
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

export async function fetchRunStatus(executionId, preferredTargetId = "") {
  const config = getUiConfig();
  const result = await runGrpcUnaryAcrossTargets(
    "GetExecutionStatus",
    {
    tenant_id: String(config.tenantId || "default"),
    execution_id: String(executionId)
    },
    preferredTargetId,
    true
  );
  const status = toExecutionStatus(result.response);
  status.targetId = String(result.target?.id || "");
  return status;
}

export async function fetchRunResult(executionId, preferredTargetId = "") {
  const config = getUiConfig();
  const result = await runGrpcUnaryAcrossTargets(
    "GetExecutionResult",
    {
    tenant_id: String(config.tenantId || "default"),
    execution_id: String(executionId)
    },
    preferredTargetId,
    true
  );
  return toExecutionResult(result.response);
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
        const normalized = normalizeGrpcAwaitInteraction(interaction || {}, target.id);
        if (!isTerminalAwaitStatus(normalized.status)) {
          interactions.push(normalized);
        }
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
  const config = getUiConfig();
  const preferredTarget = getTarget(targetId);
  const completionRequest = {
    tenant_id: String(config.tenantId || "default"),
    interaction_id: String(interactionId || ""),
    idempotency_key: `complete-${interactionId}-manual`,
    actor: "tpfgo-checkout-ui",
    response_json: JSON.stringify(completionPayload)
  };

  const result = await runGrpcUnaryAcrossTargets(
    "CompleteAwait",
    completionRequest,
    preferredTarget?.id || "",
    true
  );
  const response = result.response;

  return {
    interactionId: String(response?.interaction_id || ""),
    executionId: String(response?.execution_id || ""),
    stepId: String(response?.step_id || ""),
    status: String(response?.status || ""),
    duplicate: Boolean(response?.duplicate),
    targetId: String(result.target?.id || "")
  };
}

export async function fetchExecutionStatus(executionId, targetId = "") {
  return fetchRunStatus(executionId, targetId);
}

export async function fetchExecutionResult(executionId, targetId = "") {
  return fetchRunResult(executionId, targetId);
}

export function getAvailableAwaitTargets() {
  return getAwaitTargets();
}
