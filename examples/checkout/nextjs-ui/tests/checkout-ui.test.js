import test from "node:test";
import assert from "node:assert/strict";
import {
  buildCheckoutCompletionPayload,
  buildCheckoutOrder,
  normalizeAwaitStatus,
  describeAwaitStatus,
  inferInteractionTargetId,
  interactionActionLabel,
  nextCheckpointLabelAfterOutputType,
  reviewStatusLabel,
  shouldAllowAwaitCompletion,
  shortIdentifier,
  stageIdForOutputType,
  awaitStatusGuidance,
  normalizePendingInteraction
} from "../lib/checkout-ui.js";
import { buildListPendingRequest, normalizeGrpcAwaitInteraction, normalizeJourneyTraceEvent } from "../lib/tpf-client.js";
import {
  AUTOMATIC_STAGES,
  CHECKOUT_FLOW_STAGES,
  REVIEW_CHECKPOINTS,
  checkpointProgress,
  handoffSummaryForStage,
  nextStageAfter,
  nextReviewCheckpointAfterOutputType,
  stageForPublication,
  stageForOutputType
} from "../lib/checkout-flow.js";

test("buildCheckoutOrder uses deterministic required fields", () => {
  const request = buildCheckoutOrder({
    customerId: "11111111-1111-1111-1111-111111111111",
    restaurantId: "22222222-2222-2222-2222-222222222222",
    items: "Margherita x 2, Pasta, Soup x 3",
    totalAmount: "41.50",
    currency: "USD"
  });

  assert.equal(request.customerId, "11111111-1111-1111-1111-111111111111");
  assert.equal(request.restaurantId, "22222222-2222-2222-2222-222222222222");
  assert.equal(request.items.length, 3);
  assert.deepEqual(request.items[0], { sku: "Margherita", quantity: 2 });
  assert.equal(request.items[1].sku, "Pasta");
  assert.equal(request.items[1].quantity, 1);
  assert.equal(request.totalAmount, "41.50");
});

test("buildCheckoutOrder normalizes zero and negative quantities", () => {
  const zeroQty = buildCheckoutOrder({
    customerId: "11111111-1111-1111-1111-111111111111",
    restaurantId: "22222222-2222-2222-2222-222222222222",
    items: "Margherita x 0",
    totalAmount: "12.00",
    currency: "USD"
  });
  const negativeQty = buildCheckoutOrder({
    customerId: "11111111-1111-1111-1111-111111111111",
    restaurantId: "22222222-2222-2222-2222-222222222222",
    items: "Pasta x -1",
    totalAmount: "12.00",
    currency: "USD"
  });
  assert.equal(zeroQty.items[0].quantity, 1);
  assert.equal(negativeQty.items[0].sku, "Pasta x -1");
  assert.equal(negativeQty.items[0].quantity, 1);
});

test("buildCheckoutOrder handles non-numeric and malformed separators with defaults", () => {
  const nonNumeric = buildCheckoutOrder({
    customerId: "11111111-1111-1111-1111-111111111111",
    restaurantId: "22222222-2222-2222-2222-222222222222",
    items: "Soup x abc",
    totalAmount: "12.00",
    currency: "USD"
  });
  const whitespaceName = buildCheckoutOrder({
    customerId: "11111111-1111-1111-1111-111111111111",
    restaurantId: "22222222-2222-2222-2222-222222222222",
    items: "  x 2",
    totalAmount: "12.00",
    currency: "USD"
  });
  const malformedSep1 = buildCheckoutOrder({
    customerId: "11111111-1111-1111-1111-111111111111",
    restaurantId: "22222222-2222-2222-2222-222222222222",
    items: "item xx 2",
    totalAmount: "12.00",
    currency: "USD"
  });
  const malformedSep2 = buildCheckoutOrder({
    customerId: "11111111-1111-1111-1111-111111111111",
    restaurantId: "22222222-2222-2222-2222-222222222222",
    items: "itemx2",
    totalAmount: "12.00",
    currency: "USD"
  });
  const emptyName = buildCheckoutOrder({
    customerId: "11111111-1111-1111-1111-111111111111",
    restaurantId: "22222222-2222-2222-2222-222222222222",
    items: "",
    totalAmount: "12.00",
    currency: "USD"
  });

  assert.equal(nonNumeric.items.length, 1);
  assert.equal(nonNumeric.items[0].sku, "Soup x abc");
  assert.equal(nonNumeric.items[0].quantity, 1);
  assert.equal(whitespaceName.items[0].sku, "x 2");
  assert.equal(whitespaceName.items[0].quantity, 2);
  assert.equal(malformedSep1.items[0].sku, "item");
  assert.equal(malformedSep1.items[0].quantity, 2);
  assert.equal(malformedSep2.items[0].sku, "item");
  assert.equal(malformedSep2.items[0].quantity, 2);
  assert.equal(emptyName.items.length, 0);
});

test("normalizePendingInteraction handles request payloads", () => {
  const normalized = normalizePendingInteraction({
    interactionId: "interaction-1",
    executionId: "execution-1",
    status: "WAITING",
    requestPayload: {
      orderId: "order-1",
      customerId: "11111111-1111-1111-1111-111111111111",
      restaurantId: "22222222-2222-2222-2222-222222222222",
      totalAmount: "12.00",
      currency: "USD",
      items: [{ sku: "Pizza", quantity: 1 }]
    },
    transportType: "interaction-api"
  });

  assert.equal(normalized.interactionId, "interaction-1");
  assert.equal(normalized.orderId, "order-1");
  assert.equal(normalized.customerId, "11111111-1111-1111-1111-111111111111");
  assert.equal(normalized.restaurantId, "22222222-2222-2222-2222-222222222222");
  assert.equal(normalized.itemCount, 1);
});

test("normalizePendingInteraction tolerates alternate payload keys and missing orderId", () => {
  const normalized = normalizePendingInteraction({
    interactionId: "interaction-2",
    executionId: "execution-2",
    status: "WAITING",
    requestPayload: {
      order_id: "order-2",
      request_id: "request-2",
      customer_id: "33333333-3333-3333-3333-333333333333",
      restaurant_id: "44444444-4444-4444-4444-444444444444",
      total_amount: "17.50",
      currency: "EUR",
      itemList: [{ sku: "Pasta", quantity: 3 }]
    },
    transportType: "interaction-api"
  });

  assert.equal(normalized.orderId, "order-2");
  assert.equal(normalized.requestId, "request-2");
  assert.equal(normalized.itemCount, 1);
});

test("normalizeGrpcAwaitInteraction maps snake_case grpc fields", () => {
  const normalized = normalizeGrpcAwaitInteraction(
    {
      interaction_id: "interaction-3",
      correlation_id: "correlation-3",
      execution_id: "execution-3",
      step_id: "Await Order Approval",
      step_index: 1,
      output_type: "org.pipelineframework.tpfgo.common.domain.OrderApproved",
      status: "DISPATCHED",
      transport_type: "interaction-api",
      deadline_epoch_ms: "1800000000000",
      created_at_epoch_ms: "1700000000000",
      updated_at_epoch_ms: "1700000000100",
      request_payload_json: JSON.stringify({
        orderId: "order-3",
        requestId: "request-3",
        customerId: "55555555-5555-5555-5555-555555555555",
        restaurantId: "66666666-6666-6666-6666-666666666666",
        totalAmount: "29.75",
        currency: "USD",
        items: [{ sku: "Margherita Pizza", quantity: 1 }]
      })
    },
    "consumer-validation-orchestrator-svc"
  );

  assert.equal(normalized.interactionId, "interaction-3");
  assert.equal(normalized.executionId, "execution-3");
  assert.equal(normalized.stepId, "Await Order Approval");
  assert.equal(normalized.stepIndex, 1);
  assert.equal(normalized.outputType, "org.pipelineframework.tpfgo.common.domain.OrderApproved");
  assert.equal(normalized.transportType, "interaction-api");
  assert.equal(normalized.deadlineEpochMs, 1800000000000);
  assert.equal(normalized.itemCount, 1);
  assert.equal(inferInteractionTargetId(normalized), "consumer-validation-orchestrator-svc");
  assert.equal(shouldAllowAwaitCompletion(normalized.status, normalized.transportType), true);
});

test("interaction helpers classify checkout await stages", () => {
  const consumer = {
    outputType: "org.pipelineframework.tpfgo.common.domain.OrderApproved",
    transportType: "interaction-api",
    status: "DISPATCHED"
  };
  const restaurant = {
    outputType: "org.pipelineframework.tpfgo.common.domain.OrderAcceptedByRestaurant",
    transportType: "interaction-api",
    status: "DISPATCHED"
  };

  assert.equal(inferInteractionTargetId(consumer), "consumer-validation-orchestrator-svc");
  assert.equal(inferInteractionTargetId(restaurant), "restaurant-acceptance-orchestrator-svc");
  assert.equal(interactionActionLabel(consumer), "Complete consumer approval");
  assert.equal(interactionActionLabel(restaurant), "Complete restaurant acceptance");
  assert.equal(stageIdForOutputType(consumer.outputType), "consumer-approval");
  assert.equal(stageIdForOutputType(restaurant.outputType), "restaurant-acceptance");
});

test("checkout flow metadata contains ordered approval checkpoints", () => {
  assert.equal(CHECKOUT_FLOW_STAGES.length, 8);
  assert.equal(AUTOMATIC_STAGES.length, 6);
  assert.deepEqual(REVIEW_CHECKPOINTS.map((stage) => stage.id), [
    "consumer-approval",
    "restaurant-acceptance"
  ]);
  assert.equal(stageForOutputType("com.example.OrderApproved").serviceId, "consumer-validation-orchestrator-svc");
  assert.equal(
    nextReviewCheckpointAfterOutputType("com.example.OrderApproved").id,
    "restaurant-acceptance"
  );
  assert.equal(nextReviewCheckpointAfterOutputType("com.example.OrderAcceptedByRestaurant"), null);
  assert.equal(nextCheckpointLabelAfterOutputType("com.example.OrderAcceptedByRestaurant"), "Downstream automatic flow");
  assert.equal(checkpointProgress("").next.id, "consumer-approval");
  assert.equal(checkpointProgress("consumer-approval").next.id, "restaurant-acceptance");
});

test("checkout flow metadata teaches automatic handoffs without runtime tracing", () => {
  for (const stage of CHECKOUT_FLOW_STAGES) {
    assert.ok(stage.plainTitle, `${stage.id} has plain title`);
    assert.ok(stage.sketch, `${stage.id} has sketch label`);
    assert.ok(stage.inputSummary, `${stage.id} has input summary`);
    assert.ok(stage.outputSummary, `${stage.id} has output summary`);
    assert.ok(stage.handoffSummary, `${stage.id} has handoff summary`);
  }

  assert.equal(nextStageAfter("checkout").id, "consumer-approval");
  assert.equal(nextStageAfter("terminal"), null);
  assert.equal(
    handoffSummaryForStage("checkout"),
    "Checkout sends tpfgo.checkout.order-pending.v1 to Consumer."
  );
  assert.equal(
    handoffSummaryForStage("restaurant-acceptance"),
    "Restaurant sends tpfgo.restaurant.order-accepted.v1 to Kitchen."
  );
});

test("checkpoint publications map to journey stages", () => {
  assert.equal(stageForPublication("tpfgo.checkout.order-pending.v1").id, "checkout");
  assert.equal(stageForPublication("tpfgo.kitchen.order-ready.v1").id, "kitchen-preparation");
  assert.equal(stageForPublication("tpfgo.dispatch.delivery-assigned.v1").id, "dispatch");
  assert.equal(stageForPublication("tpfgo.delivery.order-delivered.v1").id, "delivery");
  assert.equal(stageForPublication("tpfgo.payment.capture-result.v1").id, "payment");
  assert.equal(stageForPublication("tpfgo.compensation.terminal-state.v1").id, "terminal");
  assert.equal(stageForPublication("unknown.publication"), null);
});

test("normalizeJourneyTraceEvent maps raw trace fields", () => {
  const normalized = normalizeJourneyTraceEvent({
    publication: "tpfgo.kitchen.order-ready.v1",
    observed_at_epoch_ms: "1800000000000",
    request_id: "request-1",
    order_id: "order-1",
    payload: {
      orderId: "order-1",
      requestId: "request-1",
      items: [{ sku: "Pizza", quantity: 2 }]
    }
  });

  assert.equal(normalized.publication, "tpfgo.kitchen.order-ready.v1");
  assert.equal(normalized.stageId, "kitchen-preparation");
  assert.equal(normalized.observedAtEpochMs, 1800000000000);
  assert.equal(normalized.requestId, "request-1");
  assert.equal(normalized.orderId, "order-1");
  assert.equal(normalized.payload.items.length, 1);
});

test("shortIdentifier keeps UUIDs readable", () => {
  assert.equal(shortIdentifier(""), "pending");
  assert.equal(shortIdentifier("abc123"), "abc123");
  assert.equal(shortIdentifier("7d1fb0e2-1f67-4f74-95db-bfd2c6f84f5f"), "7d1fb0e2...4f5f");
});

test("buildCheckoutCompletionPayload enforces JSON", () => {
  const parsed = buildCheckoutCompletionPayload('{"accepted":true}');
  assert.deepEqual(parsed, { accepted: true });

  assert.throws(() => buildCheckoutCompletionPayload("{not-json}"));
});

test("buildListPendingRequest omits empty step filter", () => {
  const previousTenantId = process.env.TPF_TENANT_ID;
  const previousAwaitStepId = process.env.TPF_AWAIT_STEP_ID;
  try {
    process.env.TPF_TENANT_ID = "default";
    delete process.env.TPF_AWAIT_STEP_ID;

    let request = buildListPendingRequest();
    assert.equal(request.tenant_id, "default");
    assert.equal(request.limit, 100);
    assert.equal("step_id" in request, false);

    process.env.TPF_AWAIT_STEP_ID = " ";
    request = buildListPendingRequest();
    assert.equal("step_id" in request, false);

    process.env.TPF_AWAIT_STEP_ID = "checkpoint-123";
    request = buildListPendingRequest();
    assert.equal(request.step_id, "checkpoint-123");
  } finally {
    if (previousTenantId === undefined) {
      delete process.env.TPF_TENANT_ID;
    } else {
      process.env.TPF_TENANT_ID = previousTenantId;
    }
    if (previousAwaitStepId === undefined) {
      delete process.env.TPF_AWAIT_STEP_ID;
    } else {
      process.env.TPF_AWAIT_STEP_ID = previousAwaitStepId;
    }
  }
});

test("await status helpers classify actionable state", () => {
  assert.equal(normalizeAwaitStatus(" waiting "), "WAITING");
  assert.equal(describeAwaitStatus("WAITING"), "Awaiting review");
  assert.equal(shouldAllowAwaitCompletion("WAITING"), true);
  assert.equal(shouldAllowAwaitCompletion("DISPATCHED"), false);
  assert.equal(shouldAllowAwaitCompletion("DISPATCHED", "interaction-api"), true);
  assert.equal(shouldAllowAwaitCompletion("DISPATCHED", "kafka"), false);
  assert.equal(shouldAllowAwaitCompletion("DISPATCHED", "webhook"), false);
  assert.equal(shouldAllowAwaitCompletion("DISPATCHING", "interaction-api"), false);
  assert.equal(reviewStatusLabel("DISPATCHED", "interaction-api"), "Ready for review");
  assert.equal(reviewStatusLabel("DISPATCHING", "interaction-api"), "Preparing inbox item");
  assert.equal(reviewStatusLabel("WAITING", "interaction-api"), "Waiting for completion");
  assert.equal(awaitStatusGuidance("DISPATCHED"), "Await payload was dispatched; refresh shortly. It is usually ready for completion once transport confirmation is complete.");
});
