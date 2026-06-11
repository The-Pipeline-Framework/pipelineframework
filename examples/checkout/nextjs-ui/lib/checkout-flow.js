export const CHECKOUT_FLOW_STAGES = [
  {
    id: "checkout",
    order: 1,
    serviceId: "checkout-orchestrator-svc",
    title: "Checkout intake",
    shortTitle: "Checkout",
    role: "Entrypoint",
    mode: "automatic",
    responsibility: "Accepts the checkout request and starts the checkpoint handoff chain.",
    receives: "PlaceOrderRequest from the generated gRPC RunAsync endpoint.",
    emits: "OrderPending checkpoint for consumer validation.",
    endpoint: "OrchestratorService::RunAsync",
    publication: "tpfgo.checkout.order-pending.v1"
  },
  {
    id: "consumer-approval",
    order: 2,
    serviceId: "consumer-validation-orchestrator-svc",
    title: "Consumer approval",
    shortTitle: "Consumer",
    role: "Approval step",
    mode: "human",
    responsibility: "Validates the pending order and pauses for approval before restaurant handoff.",
    receives: "OrderPending checkpoint from checkout.",
    emits: "OrderApproved checkpoint for restaurant acceptance.",
    endpoint: "ListPendingAwait / CompleteAwait",
    publication: "tpfgo.consumer.order-approved.v1",
    outputType: "OrderApproved",
    actionLabel: "Complete consumer approval"
  },
  {
    id: "restaurant-acceptance",
    order: 3,
    serviceId: "restaurant-acceptance-orchestrator-svc",
    title: "Restaurant acceptance",
    shortTitle: "Restaurant",
    role: "Approval step",
    mode: "human",
    responsibility: "Confirms the restaurant can accept the order before kitchen preparation starts.",
    receives: "OrderApproved checkpoint from consumer validation.",
    emits: "OrderAcceptedByRestaurant checkpoint for kitchen preparation.",
    endpoint: "ListPendingAwait / CompleteAwait",
    publication: "tpfgo.restaurant.order-accepted.v1",
    outputType: "OrderAcceptedByRestaurant",
    actionLabel: "Complete restaurant acceptance"
  },
  {
    id: "kitchen-preparation",
    order: 4,
    serviceId: "kitchen-preparation-orchestrator-svc",
    title: "Kitchen preparation",
    shortTitle: "Kitchen",
    role: "Operations planner",
    mode: "automatic",
    responsibility: "Expands accepted orders into kitchen work and reduces completion into a ready order.",
    receives: "OrderAcceptedByRestaurant checkpoint.",
    emits: "OrderReady checkpoint for dispatch.",
    endpoint: "Checkpoint subscription",
    publication: "tpfgo.kitchen.order-ready.v1"
  },
  {
    id: "dispatch",
    order: 5,
    serviceId: "dispatch-orchestrator-svc",
    title: "Dispatch assignment",
    shortTitle: "Dispatch",
    role: "Logistics boundary",
    mode: "automatic",
    responsibility: "Assigns delivery logistics after the order is ready.",
    receives: "OrderReady checkpoint from kitchen preparation.",
    emits: "DeliveryAssigned checkpoint for delivery execution.",
    endpoint: "Checkpoint subscription",
    publication: "tpfgo.dispatch.delivery-assigned.v1"
  },
  {
    id: "delivery",
    order: 6,
    serviceId: "delivery-execution-orchestrator-svc",
    title: "Delivery execution",
    shortTitle: "Delivery",
    role: "Fulfillment boundary",
    mode: "automatic",
    responsibility: "Materializes the delivery completion state.",
    receives: "DeliveryAssigned checkpoint from dispatch.",
    emits: "OrderDelivered checkpoint for payment capture.",
    endpoint: "Checkpoint subscription",
    publication: "tpfgo.delivery.order-delivered.v1"
  },
  {
    id: "payment",
    order: 7,
    serviceId: "payment-capture-orchestrator-svc",
    title: "Payment capture",
    shortTitle: "Payment",
    role: "Payment terminal path",
    mode: "automatic",
    responsibility: "Captures payment and emits the closed payment outcome.",
    receives: "OrderDelivered checkpoint from delivery execution.",
    emits: "PaymentOutcome checkpoint for terminal adjudication.",
    endpoint: "Checkpoint subscription",
    publication: "tpfgo.payment.capture-result.v1"
  },
  {
    id: "terminal",
    order: 8,
    serviceId: "compensation-failure-orchestrator-svc",
    title: "Terminal adjudication",
    shortTitle: "Terminal",
    role: "Final state owner",
    mode: "automatic",
    responsibility: "Maps the payment outcome into the final business state.",
    receives: "PaymentOutcome checkpoint from payment capture.",
    emits: "Terminal order state.",
    endpoint: "Checkpoint subscription",
    publication: "tpfgo.compensation.terminal-state.v1"
  }
];

export const REVIEW_CHECKPOINTS = CHECKOUT_FLOW_STAGES.filter((stage) => stage.mode === "human");

export function stageById(stageId) {
  const normalized = String(stageId || "").trim();
  return CHECKOUT_FLOW_STAGES.find((stage) => stage.id === normalized) || null;
}

export function stageForOutputType(outputType) {
  const normalized = String(outputType || "").toLowerCase();
  return CHECKOUT_FLOW_STAGES.find((stage) =>
    stage.outputType && normalized.endsWith(stage.outputType.toLowerCase())) || null;
}

export function stageForServiceId(serviceId) {
  const normalized = String(serviceId || "").trim();
  return CHECKOUT_FLOW_STAGES.find((stage) => stage.serviceId === normalized) || null;
}

export function stageForInteraction(interaction) {
  return stageForOutputType(interaction?.outputType)
    || stageForServiceId(interaction?.targetId)
    || null;
}

export function nextReviewCheckpointAfterStage(stageId) {
  const completed = stageById(stageId);
  if (!completed) {
    return REVIEW_CHECKPOINTS[0] || null;
  }
  return REVIEW_CHECKPOINTS.find((stage) => stage.order > completed.order) || null;
}

export function nextReviewCheckpointAfterOutputType(outputType) {
  const completed = stageForOutputType(outputType);
  return nextReviewCheckpointAfterStage(completed?.id);
}

export function checkpointProgress(completedStageId) {
  const completed = stageById(completedStageId);
  if (!completed) {
    return { completed: null, next: HUMAN_CHECKPOINTS[0] || null };
  }
  return {
    completed,
    next: nextReviewCheckpointAfterStage(completed.id)
  };
}
