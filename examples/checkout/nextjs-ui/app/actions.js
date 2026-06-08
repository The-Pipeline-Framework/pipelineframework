"use server";

import { revalidatePath } from "next/cache";
import { redirect } from "next/navigation";
import {
  completeInteraction,
  submitOrder
} from "../lib/tpf-client.js";

function getRequiredField(formData, fieldName) {
  const value = String(formData.get(fieldName) || "").trim();
  if (!value) {
    throw new Error(`${fieldName} must not be empty`);
  }
  return value;
}

export async function submitCheckoutOrder(formData) {
  const customerId = getRequiredField(formData, "customerId");
  const restaurantId = getRequiredField(formData, "restaurantId");
  const items = String(formData.get("items") || "");
  const totalAmount = getRequiredField(formData, "totalAmount");
  const currency = getRequiredField(formData, "currency");

  const accepted = await submitOrder({
    customerId,
    restaurantId,
    items,
    totalAmount,
    currency
  });
  const executionId = String(accepted?.executionId || "").trim();
  if (!executionId) {
    throw new Error("Checkout request accepted but no execution id was returned from TPF.");
  }
  redirect(`/executions/${encodeURIComponent(executionId)}`);
}

export async function completeCheckoutInteraction(formData) {
  const executionId = getRequiredField(formData, "executionId");
  const interactionId = getRequiredField(formData, "interactionId");
  const targetId = String(formData.get("targetId") || "").trim();
  const payload = String(formData.get("payload") || "").trim();
  await completeInteraction({ interactionId, payload, targetId });
  revalidatePath("/interactions");
  redirect(`/executions/${encodeURIComponent(executionId)}`);
}
