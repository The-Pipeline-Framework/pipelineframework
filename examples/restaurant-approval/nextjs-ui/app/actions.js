"use server";

import { revalidatePath } from "next/cache";
import { redirect } from "next/navigation";
import {
  completeAcceptedInteraction,
  completeDeclinedInteraction,
  submitOrder
} from "../lib/tpf-client.js";

export async function submitRestaurantOrder(formData) {
  const customerName = String(formData.get("customerName") || "");
  const restaurantName = String(formData.get("restaurantName") || "");
  const items = String(formData.get("items") || "");
  const totalAmount = String(formData.get("totalAmount") || "");
  const currency = String(formData.get("currency") || "EUR");

  if (!customerName.trim() || !restaurantName.trim() || !items.trim() || !totalAmount.trim()) {
    throw new Error("Required fields (customerName, restaurantName, items, totalAmount) must not be empty");
  }

  const accepted = await submitOrder({
    customerName,
    restaurantName,
    items,
    totalAmount,
    currency
  });
  redirect(`/executions/${accepted.executionId}`);
}

export async function approveRestaurantOrder(formData) {
  const executionId = String(formData.get("executionId") || "");
  const interactionId = String(formData.get("interactionId") || "");
  const orderId = String(formData.get("orderId") || "");
  const note = String(formData.get("note") || "Approved by the restaurant");

  if (!executionId.trim() || !interactionId.trim() || !orderId.trim()) {
    throw new Error("Required fields (executionId, interactionId, orderId) must not be empty");
  }

  await completeAcceptedInteraction({
    interactionId,
    orderId,
    note
  });
  revalidatePath("/interactions");
  redirect(`/executions/${executionId}`);
}

export async function declineRestaurantOrder(formData) {
  const executionId = String(formData.get("executionId") || "");
  const interactionId = String(formData.get("interactionId") || "");
  const orderId = String(formData.get("orderId") || "");
  const note = String(formData.get("note") || "Unable to accept the order");
  const declineReason = String(formData.get("declineReason") || "No reason provided");

  if (!executionId.trim() || !interactionId.trim() || !orderId.trim()) {
    throw new Error("Required fields (executionId, interactionId, orderId) must not be empty");
  }

  await completeDeclinedInteraction({
    interactionId,
    orderId,
    note,
    declineReason
  });
  revalidatePath("/interactions");
  redirect(`/executions/${executionId}`);
}
