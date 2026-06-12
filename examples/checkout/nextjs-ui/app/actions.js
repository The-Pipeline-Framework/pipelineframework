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
  const items = getRequiredField(formData, "items");
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
  revalidatePath("/interactions");
  redirect(`/interactions?started=${encodeURIComponent(executionId)}`);
}

export async function completeCheckoutInteraction(formData) {
  const interactionId = getRequiredField(formData, "interactionId");
  const targetId = String(formData.get("targetId") || "").trim();
  const stageId = String(formData.get("stageId") || "").trim();
  const payload = String(formData.get("payload") || "").trim();
  let destination;
  try {
    const completed = await completeInteraction({ interactionId, payload, targetId });
    const completedInteractionId = String(completed?.interactionId || interactionId);
    const completedStage = stageId ? `&completedStage=${encodeURIComponent(stageId)}` : "";
    const target = targetId ? `&target=${encodeURIComponent(targetId)}` : "";
    destination = `/interactions?completed=${encodeURIComponent(completedInteractionId)}${completedStage}${target}`;
  } catch (error) {
    const message = String(error?.message || "Interaction completion failed.").slice(0, 600);
    destination = `/interactions?error=${encodeURIComponent(message)}`;
  }
  revalidatePath("/interactions");
  redirect(destination);
}
