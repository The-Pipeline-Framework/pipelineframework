import test from "node:test";
import assert from "node:assert/strict";
import {
  buildAcceptedPayload,
  buildDeclinedPayload,
  normalizePendingInteraction
} from "../lib/restaurant-approval.js";

test("normalizePendingInteraction keeps execution and request details", () => {
  const normalized = normalizePendingInteraction({
    interactionId: "interaction-1",
    executionId: "execution-1",
    stepId: "ProcessAwaitRestaurantDecisionService",
    status: "WAITING",
    transportType: "interaction-api",
    deadlineEpochMs: 12345,
    requestPayload: {
      orderId: "order-1",
      customerName: "Ada Lovelace",
      restaurantName: "Cafe TPF",
      items: "Pizza",
      totalAmount: "27.50",
      currency: "EUR"
    }
  });

  assert.equal(normalized.executionId, "execution-1");
  assert.equal(normalized.orderId, "order-1");
  assert.equal(normalized.restaurantName, "Cafe TPF");
});

test("buildAcceptedPayload wraps the accepted union variant", () => {
  const payload = buildAcceptedPayload("order-1", "Approved by Cafe TPF");
  assert.equal(payload.accepted.orderId, "order-1");
  assert.equal(payload.accepted.note, "Approved by Cafe TPF");
  assert.match(payload.accepted.decidedAt, /^\d{4}-\d{2}-\d{2}T/);
});

test("buildDeclinedPayload wraps the declined union variant", () => {
  const payload = buildDeclinedPayload("order-2", "Need more prep time", "Kitchen overload");
  assert.equal(payload.declined.orderId, "order-2");
  assert.equal(payload.declined.note, "Need more prep time");
  assert.equal(payload.declined.declineReason, "Kitchen overload");
  assert.match(payload.declined.decidedAt, /^\d{4}-\d{2}-\d{2}T/);
});
