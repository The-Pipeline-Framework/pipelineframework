"use server";

import { revalidatePath } from "next/cache";
import { redirect } from "next/navigation";
import {
  completeAcceptedInteraction,
  completeDeclinedInteraction,
  submitOrder
} from "../lib/tpf-client.js";

export async function submitRestaurantOrder(formData) {
  const accepted = await submitOrder({
    customerName: String(formData.get("customerName") || ""),
    restaurantName: String(formData.get("restaurantName") || ""),
    items: String(formData.get("items") || ""),
    totalAmount: String(formData.get("totalAmount") || ""),
    currency: String(formData.get("currency") || "EUR")
  });
  redirect(`/executions/${accepted.executionId}`);
}

export async function approveRestaurantOrder(formData) {
  const executionId = String(formData.get("executionId") || "");
  await completeAcceptedInteraction({
    interactionId: String(formData.get("interactionId") || ""),
    orderId: String(formData.get("orderId") || ""),
    note: String(formData.get("note") || "Approved by the restaurant")
  });
  revalidatePath("/interactions");
  redirect(`/executions/${executionId}`);
}

export async function declineRestaurantOrder(formData) {
  const executionId = String(formData.get("executionId") || "");
  await completeDeclinedInteraction({
    interactionId: String(formData.get("interactionId") || ""),
    orderId: String(formData.get("orderId") || ""),
    note: String(formData.get("note") || "Unable to accept the order"),
    declineReason: String(formData.get("declineReason") || "No reason provided")
  });
  revalidatePath("/interactions");
  redirect(`/executions/${executionId}`);
}
