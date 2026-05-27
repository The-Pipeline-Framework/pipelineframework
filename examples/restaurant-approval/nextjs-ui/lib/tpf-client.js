import { getUiConfig } from "./config.js";
import {
  buildAcceptedPayload,
  buildDeclinedPayload,
  buildOrderRequest,
  normalizePendingInteraction
} from "./restaurant-approval.js";

async function tpfFetch(path, init = {}) {
  const config = getUiConfig();
  const url = new URL(path, config.baseUrl);
  const headers = new Headers(init.headers || {});
  headers.set("x-tenant-id", config.tenantId);
  if (!headers.has("content-type") && init.body) {
    headers.set("content-type", "application/json");
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 10000);

  try {
    const response = await fetch(url, {
      ...init,
      headers,
      cache: "no-store",
      signal: controller.signal
    });
    clearTimeout(timeoutId);

    if (!response.ok) {
      const detail = await response.text();
      throw new Error(`TPF request failed (${response.status}) for ${path}: ${detail}`);
    }
    return response;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === "AbortError") {
      throw new Error(`TPF request timeout (10s) for ${path}`);
    }
    throw error;
  }
}

export async function submitOrder(form) {
  const request = buildOrderRequest(form);
  const response = await tpfFetch("/pipeline/run-async", {
    method: "POST",
    headers: {
      "Idempotency-Key": `order-${request.requestId}`
    },
    body: JSON.stringify(request)
  });
  return response.json();
}

export async function fetchPendingInteractions() {
  const config = getUiConfig();
  const response = await tpfFetch(
    `/pipeline/interactions/pending?stepId=${encodeURIComponent(config.awaitStepId)}`
  );
  const items = await response.json();
  return items.map(normalizePendingInteraction);
}

export async function completeAcceptedInteraction({ interactionId, orderId, note }) {
  const response = await tpfFetch("/pipeline/interactions/complete", {
    method: "POST",
    body: JSON.stringify({
      interactionId,
      idempotencyKey: `complete-${interactionId}-accepted`,
      actor: "restaurant-approval-nextjs-ui",
      responsePayload: buildAcceptedPayload(orderId, note)
    })
  });
  return response.json();
}

export async function completeDeclinedInteraction({
  interactionId,
  orderId,
  note,
  declineReason
}) {
  const response = await tpfFetch("/pipeline/interactions/complete", {
    method: "POST",
    body: JSON.stringify({
      interactionId,
      idempotencyKey: `complete-${interactionId}-declined`,
      actor: "restaurant-approval-nextjs-ui",
      responsePayload: buildDeclinedPayload(orderId, note, declineReason)
    })
  });
  return response.json();
}

export async function fetchExecutionStatus(executionId) {
  const response = await tpfFetch(`/pipeline/executions/${encodeURIComponent(executionId)}`);
  return response.json();
}

export async function fetchExecutionResult(executionId) {
  const response = await tpfFetch(`/pipeline/executions/${encodeURIComponent(executionId)}/result`);
  return response.json();
}
