import test from "node:test";
import assert from "node:assert/strict";
import {
  buildCheckoutCompletionPayload,
  buildCheckoutOrder,
  normalizeAwaitStatus,
  describeAwaitStatus,
  shouldAllowAwaitCompletion,
  awaitStatusGuidance,
  normalizePendingInteraction
} from "../lib/checkout-ui.js";
import { buildListPendingRequest } from "../lib/tpf-client.js";

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

test("buildCheckoutCompletionPayload enforces JSON", () => {
  const parsed = buildCheckoutCompletionPayload('{"accepted":true}');
  assert.deepEqual(parsed, { accepted: true });

  assert.throws(() => buildCheckoutCompletionPayload("{not-json}"));
});

test("buildListPendingRequest omits empty step filter", () => {
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
});

test("await status helpers classify actionable state", () => {
  assert.equal(normalizeAwaitStatus(" waiting "), "WAITING");
  assert.equal(describeAwaitStatus("WAITING"), "Awaiting human input");
  assert.equal(shouldAllowAwaitCompletion("WAITING"), true);
  assert.equal(shouldAllowAwaitCompletion("DISPATCHED"), false);
  assert.equal(awaitStatusGuidance("DISPATCHED"), "Await payload was dispatched; refresh shortly. It is usually ready for completion once transport confirmation is complete.");
});
