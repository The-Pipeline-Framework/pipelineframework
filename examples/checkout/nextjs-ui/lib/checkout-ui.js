import {
  nextReviewCheckpointAfterOutputType,
  stageForInteraction,
  stageForOutputType,
  stageForServiceId
} from "./checkout-flow.js";

function normalizeId(rawValue, fallback) {
  const trimmed = String(rawValue || "").trim();
  return trimmed || fallback;
}

function getFieldValue(container, ...keys) {
  for (const key of keys) {
    const value = container?.[key];
    if (value !== undefined && value !== null) {
      return value;
    }
  }
  return undefined;
}

export function outputTypeName(interaction) {
  const outputType = String(interaction?.outputType || "").trim();
  const segments = outputType.split(/[.$/]+/u).filter(Boolean);
  return segments.at(-1) || outputType;
}

function parseItemLine(rawLine, defaultIndex) {
  const line = String(rawLine || "").trim();
  if (!line) {
    return null;
  }

  const withQuantity = line.match(/^(.*?)[\s×x:]+(\d+)\s*$/u);
  const [, skuPart, quantityPart] = withQuantity || [];
  const sku = String(skuPart || line).trim() || `item-${defaultIndex + 1}`;
  const quantity = Math.max(1, Number.parseInt(quantityPart ?? "1", 10) || 1);

  return {
    sku,
    quantity
  };
}

export function buildCheckoutOrder(form) {
  const itemLines = String(form.items || "")
    .split(/[\n,]+/)
    .map((item) => String(item || "").trim())
    .flatMap((item) => item ? item.split(";").map((part) => String(part).trim()).filter(Boolean) : [])
    .filter(Boolean);

  const parsedItems = itemLines.map((itemLine, index) => parseItemLine(itemLine, index));
  const items = parsedItems.filter(Boolean);

  const fallbackCustomerId = crypto.randomUUID();
  const fallbackRestaurantId = crypto.randomUUID();

  return {
    requestId: crypto.randomUUID(),
    customerId: normalizeId(form.customerId, fallbackCustomerId),
    restaurantId: normalizeId(form.restaurantId, fallbackRestaurantId),
    items,
    totalAmount: String(form.totalAmount || "0"),
    currency: String(form.currency || "USD")
  };
}

export function normalizePendingInteraction(interaction) {
  const requestPayload = interaction?.requestPayload ?? {};
  const requestPayloadRequestId = String(getFieldValue(requestPayload, "requestId", "request_id") || interaction?.requestId || "");
  const requestPayloadOrderId = getFieldValue(requestPayload, "orderId", "order_id");
  const requestPayloadCustomerId = getFieldValue(requestPayload, "customerId", "customer_id");
  const requestPayloadRestaurantId = getFieldValue(requestPayload, "restaurantId", "restaurant_id");
  const requestPayloadTotalAmount = getFieldValue(requestPayload, "totalAmount", "total_amount");
  const requestPayloadCurrency = getFieldValue(requestPayload, "currency", "currencyCode", "currency_code");
  const itemList = getFieldValue(requestPayload, "items", "itemList");
  const deadlineEpochMs = Number(interaction?.deadlineEpochMs ?? 0);
  const itemCount = Array.isArray(itemList) ? itemList.length : 0;

  return {
    interactionId: String(interaction?.interactionId ?? ""),
    executionId: String(interaction?.executionId ?? ""),
    stepId: String(interaction?.stepId ?? ""),
    status: String(interaction?.status ?? ""),
    transportType: String(interaction?.transportType ?? ""),
    deadlineEpochMs: Number.isFinite(deadlineEpochMs) ? deadlineEpochMs : 0,
    orderId: String(requestPayloadOrderId ?? requestPayloadRequestId),
    requestId: requestPayloadRequestId,
    customerId: String(requestPayloadCustomerId ?? ""),
    requestPayload: requestPayload,
    outputType: String(interaction?.outputType ?? ""),
    totalAmount: String(requestPayloadTotalAmount ?? ""),
    currency: String(requestPayloadCurrency ?? ""),
    itemCount,
    restaurantId: String(requestPayloadRestaurantId ?? ""),
    payloadPreview: Array.isArray(itemList)
      ? itemList
          .slice(0, 3)
          .map((item) => `${item.sku} x ${item.quantity}`)
          .join(", ")
      : ""
  };
}

export function inferInteractionTargetId(interaction) {
  const explicitTarget = String(interaction?.targetId || "").trim();
  if (explicitTarget) {
    return explicitTarget;
  }
  return stageForInteraction(interaction)?.serviceId || "checkout-orchestrator-svc";
}

export function interactionActionLabel(interaction) {
  return stageForInteraction(interaction)?.actionLabel || "Complete interaction";
}

export function reviewStatusLabel(status, transportType = "") {
  const normalizedStatus = normalizeAwaitStatus(status);
  const normalizedTransportType = String(transportType || "").toLowerCase();
  if (normalizedStatus === "DISPATCHED" && normalizedTransportType === "interaction-api") {
    return "Ready for review";
  }
  if (normalizedStatus === "DISPATCHING") {
    return "Preparing inbox item";
  }
  if (normalizedStatus === "WAITING") {
    return "Waiting for completion";
  }
  return describeAwaitStatus(status);
}

export function stageIdForInteraction(interaction) {
  return stageForInteraction(interaction)?.id || "";
}

export function stageIdForOutputType(outputType) {
  return stageForOutputType(outputType)?.id || "";
}

export function serviceLabelForTarget(targetId) {
  return stageForServiceId(targetId)?.title || String(targetId || "Checkout service");
}

export function nextCheckpointLabelAfterOutputType(outputType) {
  return nextReviewCheckpointAfterOutputType(outputType)?.title || "Downstream automatic flow";
}

export function isTerminalAwaitStatus(status) {
  const normalized = normalizeAwaitStatus(status);
  return normalized === "COMPLETED"
    || normalized === "FAILED"
    || normalized === "TIMED_OUT"
    || normalized === "CANCELLED"
    || normalized === "EXPIRED";
}

const AWAIT_STATUS_LABELS = new Map([
  ["WAITING", "Awaiting review"],
  ["DISPATCHING", "Dispatching await payload"],
  ["DISPATCHED", "Await dispatched to transport"],
  ["FAILED", "Await failed"],
  ["TIMED_OUT", "Await timed out"],
  ["CANCELLED", "Await canceled"],
  ["EXPIRED", "Await expired"],
  ["", "Await status unknown"]
]);

const AWAIT_STATUS_GUIDANCE = new Map([
  [
    "WAITING",
    "This step is paused for approval and can be resumed from the review desk."
  ],
  [
    "DISPATCHING",
    "TPF has received the pending interaction and is dispatching it to the await transport."
  ],
  [
    "DISPATCHED",
    "Await payload was dispatched; refresh shortly. It is usually ready for completion once transport confirmation is complete."
  ],
  ["TIMED_OUT", "This await timed out before completion."],
  ["FAILED", "Await transport marked this interaction failed."],
  ["CANCELLED", "Await interaction was canceled before completion."],
  ["EXPIRED", "Await interaction record is no longer active."]
]);

export function normalizeAwaitStatus(status) {
  return String(status || "").trim().toUpperCase();
}

export function describeAwaitStatus(status) {
  const normalized = normalizeAwaitStatus(status);
  return AWAIT_STATUS_LABELS.get(normalized) || `Await status ${normalized || "unknown"}`;
}

export function shouldAllowAwaitCompletion(status, transportType = "") {
  const normalizedStatus = normalizeAwaitStatus(status);
  const normalizedTransportType = String(transportType || "").toLowerCase();
  return normalizedStatus === "WAITING"
    || (normalizedTransportType === "interaction-api" && normalizedStatus === "DISPATCHED");
}

export function awaitStatusGuidance(status) {
  const normalized = normalizeAwaitStatus(status);
  return AWAIT_STATUS_GUIDANCE.get(normalized) || "Await is in progress and may become user-actionable.";
}

export function buildCheckoutCompletionPayload(rawPayloadJson) {
  try {
    return rawPayloadJson ? JSON.parse(String(rawPayloadJson)) : null;
  } catch (error) {
    throw new Error(`Interaction completion payload must be valid JSON: ${error.message}`);
  }
}
