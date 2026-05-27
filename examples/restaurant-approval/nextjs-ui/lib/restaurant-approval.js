export function buildAcceptedPayload(orderId, note) {
  return {
    accepted: {
      orderId,
      decidedAt: new Date().toISOString(),
      note
    }
  };
}

export function buildDeclinedPayload(orderId, note, declineReason) {
  return {
    declined: {
      orderId,
      decidedAt: new Date().toISOString(),
      note,
      declineReason
    }
  };
}

export function normalizePendingInteraction(interaction) {
  const requestPayload = interaction?.requestPayload ?? {};
  const deadlineEpochMs = Number(interaction?.deadlineEpochMs ?? 0);
  return {
    interactionId: String(interaction?.interactionId ?? ""),
    executionId: String(interaction?.executionId ?? ""),
    stepId: String(interaction?.stepId ?? ""),
    status: String(interaction?.status ?? ""),
    transportType: String(interaction?.transportType ?? ""),
    deadlineEpochMs: Number.isFinite(deadlineEpochMs) ? deadlineEpochMs : 0,
    orderId: String(requestPayload.orderId ?? ""),
    customerName: String(requestPayload.customerName ?? ""),
    restaurantName: String(requestPayload.restaurantName ?? ""),
    items: String(requestPayload.items ?? ""),
    totalAmount: String(requestPayload.totalAmount ?? ""),
    currency: String(requestPayload.currency ?? "")
  };
}

export function buildOrderRequest(form) {
  return {
    requestId: crypto.randomUUID(),
    customerName: form.customerName,
    restaurantName: form.restaurantName,
    items: form.items,
    totalAmount: form.totalAmount,
    currency: form.currency
  };
}
