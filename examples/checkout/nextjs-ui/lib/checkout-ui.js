function normalizeId(rawValue, fallback) {
  const trimmed = String(rawValue || "").trim();
  return trimmed || fallback;
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
  const deadlineEpochMs = Number(interaction?.deadlineEpochMs ?? 0);
  const itemCount = Array.isArray(requestPayload.items) ? requestPayload.items.length : 0;

  return {
    interactionId: String(interaction?.interactionId ?? ""),
    executionId: String(interaction?.executionId ?? ""),
    stepId: String(interaction?.stepId ?? ""),
    status: String(interaction?.status ?? ""),
    transportType: String(interaction?.transportType ?? ""),
    deadlineEpochMs: Number.isFinite(deadlineEpochMs) ? deadlineEpochMs : 0,
    orderId: String(requestPayload.orderId ?? ""),
    requestId: String(requestPayload.requestId ?? ""),
    customerId: String(requestPayload.customerId ?? ""),
    requestPayload: requestPayload,
    outputType: String(interaction?.outputType ?? ""),
    totalAmount: String(requestPayload.totalAmount ?? ""),
    currency: String(requestPayload.currency ?? ""),
    itemCount,
    restaurantId: String(requestPayload.restaurantId ?? ""),
    payloadPreview: Array.isArray(requestPayload.items)
      ? requestPayload.items
          .slice(0, 3)
          .map((item) => `${item.sku} x ${item.quantity}`)
          .join(", ")
      : ""
  };
}

const AWAIT_STATUS_LABELS = new Map([
  ["WAITING", "Awaiting human input"],
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
    "This step is paused on a human decision point and can be resumed from the interaction inbox."
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

export function shouldAllowAwaitCompletion(status) {
  return normalizeAwaitStatus(status) === "WAITING";
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
