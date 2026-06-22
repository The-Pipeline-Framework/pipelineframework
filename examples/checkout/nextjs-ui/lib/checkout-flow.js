export const CHECKOUT_FLOW_STAGES = [
  {
    id: "checkout",
    order: 1,
    serviceId: "checkout-orchestrator-svc",
    title: "Checkout intake",
    shortTitle: "Checkout",
    plainTitle: "Order starts here",
    role: "Entrypoint",
    mode: "automatic",
    icon: "ticket",
    sketch: "Order ticket",
    responsibility: "Accepts the checkout request and starts the checkpoint handoff chain.",
    receives: "PlaceOrderRequest from the generated gRPC RunAsync endpoint.",
    emits: "OrderPending checkpoint for consumer validation.",
    inputSummary: "Customer, restaurant, items, amount.",
    outputSummary: "OrderPending checkpoint with the order details.",
    handoffSummary: "Sends the order ticket to consumer validation.",
    endpoint: "OrchestratorService::RunAsync",
    publication: "tpfgo.checkout.order-pending.v1"
  },
  {
    id: "consumer-approval",
    order: 2,
    serviceId: "consumer-validation-orchestrator-svc",
    title: "Consumer approval",
    shortTitle: "Consumer",
    plainTitle: "Check the customer",
    role: "Approval step",
    mode: "human",
    icon: "approval",
    sketch: "Approval stamp",
    responsibility: "Validates the pending order and pauses for approval before restaurant handoff.",
    receives: "OrderPending checkpoint from checkout.",
    emits: "OrderApproved checkpoint for restaurant acceptance.",
    inputSummary: "The pending order and customer context.",
    outputSummary: "OrderApproved checkpoint after review.",
    handoffSummary: "A person approves the order, then TPF passes it to the restaurant service.",
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
    plainTitle: "Ask the restaurant",
    role: "Approval step",
    mode: "human",
    icon: "restaurant",
    sketch: "Restaurant pass",
    responsibility: "Confirms the restaurant can accept the order before kitchen preparation starts.",
    receives: "OrderApproved checkpoint from consumer validation.",
    emits: "OrderAcceptedByRestaurant checkpoint for kitchen preparation.",
    inputSummary: "Approved order and restaurant request.",
    outputSummary: "OrderAcceptedByRestaurant checkpoint.",
    handoffSummary: "The restaurant accepts, then the kitchen can start preparing.",
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
    plainTitle: "Kitchen makes it",
    role: "Operations planner",
    mode: "automatic",
    icon: "kitchen",
    sketch: "Kitchen board",
    responsibility: "Expands accepted orders into kitchen work and reduces completion into a ready order.",
    receives: "OrderAcceptedByRestaurant checkpoint.",
    emits: "OrderReady checkpoint for dispatch.",
    inputSummary: "Accepted restaurant order.",
    outputSummary: "OrderReady checkpoint.",
    handoffSummary: "Turns the accepted order into kitchen work, then says it is ready.",
    endpoint: "Checkpoint subscription",
    publication: "tpfgo.kitchen.order-ready.v1"
  },
  {
    id: "dispatch",
    order: 5,
    serviceId: "dispatch-orchestrator-svc",
    title: "Dispatch assignment",
    shortTitle: "Dispatch",
    plainTitle: "Find a courier",
    role: "Logistics boundary",
    mode: "automatic",
    icon: "route",
    sketch: "Route card",
    responsibility: "Assigns delivery logistics after the order is ready.",
    receives: "OrderReady checkpoint from kitchen preparation.",
    emits: "DeliveryAssigned checkpoint for delivery execution.",
    inputSummary: "Ready order from the kitchen.",
    outputSummary: "DeliveryAssigned checkpoint.",
    handoffSummary: "Chooses the delivery path and hands it to delivery execution.",
    endpoint: "Checkpoint subscription",
    publication: "tpfgo.dispatch.delivery-assigned.v1"
  },
  {
    id: "delivery",
    order: 6,
    serviceId: "delivery-execution-orchestrator-svc",
    title: "Delivery execution",
    shortTitle: "Delivery",
    plainTitle: "Deliver the order",
    role: "Fulfillment boundary",
    mode: "automatic",
    icon: "delivery",
    sketch: "Delivery slip",
    responsibility: "Materializes the delivery completion state.",
    receives: "DeliveryAssigned checkpoint from dispatch.",
    emits: "OrderDelivered checkpoint for payment capture.",
    inputSummary: "Assigned delivery details.",
    outputSummary: "OrderDelivered checkpoint.",
    handoffSummary: "Marks the order delivered so payment can be captured.",
    endpoint: "Checkpoint subscription",
    publication: "tpfgo.delivery.order-delivered.v1"
  },
  {
    id: "payment",
    order: 7,
    serviceId: "payment-capture-orchestrator-svc",
    title: "Payment capture",
    shortTitle: "Payment",
    plainTitle: "Take payment",
    role: "Payment terminal path",
    mode: "automatic",
    icon: "payment",
    sketch: "Receipt",
    responsibility: "Captures payment and emits the closed payment outcome.",
    receives: "OrderDelivered checkpoint from delivery execution.",
    emits: "PaymentOutcome checkpoint for terminal adjudication.",
    inputSummary: "Delivered order and payment amount.",
    outputSummary: "PaymentOutcome checkpoint.",
    handoffSummary: "Captures payment and passes the result to final state handling.",
    endpoint: "Checkpoint subscription",
    publication: "tpfgo.payment.capture-result.v1"
  },
  {
    id: "terminal",
    order: 8,
    serviceId: "compensation-failure-orchestrator-svc",
    title: "Terminal adjudication",
    shortTitle: "Terminal",
    plainTitle: "Finish the story",
    role: "Final state owner",
    mode: "automatic",
    icon: "finish",
    sketch: "Finish flag",
    responsibility: "Maps the payment outcome into the final business state.",
    receives: "PaymentOutcome checkpoint from payment capture.",
    emits: "Terminal order state.",
    inputSummary: "Payment outcome.",
    outputSummary: "Final order state.",
    handoffSummary: "Closes the order journey.",
    endpoint: "Checkpoint subscription",
    publication: "tpfgo.compensation.terminal-state.v1"
  }
];

export const REVIEW_CHECKPOINTS = CHECKOUT_FLOW_STAGES.filter((stage) => stage.mode === "human");
export const AUTOMATIC_STAGES = CHECKOUT_FLOW_STAGES.filter((stage) => stage.mode === "automatic");

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

export function stageForPublication(publication) {
  const normalized = String(publication || "").trim();
  return CHECKOUT_FLOW_STAGES.find((stage) => stage.publication === normalized) || null;
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
    return { completed: null, next: REVIEW_CHECKPOINTS[0] || null };
  }
  return {
    completed,
    next: nextReviewCheckpointAfterStage(completed.id)
  };
}

export function nextStageAfter(stageId) {
  const stage = stageById(stageId);
  if (!stage) {
    return null;
  }
  return CHECKOUT_FLOW_STAGES.find((candidate) => candidate.order === stage.order + 1) || null;
}

export function handoffSummaryForStage(stageId) {
  const stage = stageById(stageId);
  const nextStage = nextStageAfter(stageId);
  if (!stage) {
    return "";
  }
  if (!nextStage) {
    return stage.handoffSummary || "This is the final step.";
  }
  return `${stage.shortTitle} sends ${stage.publication} to ${nextStage.shortTitle}.`;
}
