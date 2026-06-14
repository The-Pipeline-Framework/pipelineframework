import Link from "next/link";
import { ArrowRight, CheckCircle2, ClipboardCheck } from "lucide-react";

import { completeCheckoutInteraction } from "../actions.js";
import {
  inferInteractionTargetId,
  interactionActionLabel,
  reviewStatusLabel,
  shouldAllowAwaitCompletion,
  shortIdentifier,
  stageIdForInteraction
} from "../../lib/checkout-ui.js";
import { handoffSummaryForStage, nextReviewCheckpointAfterStage, stageForInteraction } from "../../lib/checkout-flow.js";

function defaultPayloadForInteraction(interaction) {
  const requestPayload = interaction.requestPayload;
  if (requestPayload && typeof requestPayload === "object" && Object.keys(requestPayload).length > 0) {
    const payload = { ...requestPayload };
    const outputType = String(interaction.outputType || "");
    if (outputType.endsWith("OrderAcceptedByRestaurant")) {
      payload.acceptedAt = payload.acceptedAt || "1970-01-01T00:00:00Z";
      payload.kitchenTicketId = payload.kitchenTicketId || "00000000-0000-0000-0000-000000000000";
    } else if (outputType.endsWith("OrderApproved")) {
      payload.approvedAt = payload.approvedAt || "1970-01-01T00:00:00Z";
      payload.riskBand = payload.riskBand || "standard";
    }
    return JSON.stringify(payload, null, 2);
  }

  const outputType = String(interaction.outputType || "");
  if (outputType.endsWith("OrderAcceptedByRestaurant")) {
    return JSON.stringify(
      {
        orderId: interaction.orderId || interaction.requestId,
        requestId: interaction.requestId || "",
        customerId: interaction.customerId || "",
        restaurantId: interaction.restaurantId || "",
        totalAmount: interaction.totalAmount || "0",
        currency: interaction.currency || "USD",
        acceptedAt: "1970-01-01T00:00:00Z",
        kitchenTicketId: "00000000-0000-0000-0000-000000000000"
      },
      null,
      2
    );
  }

  return JSON.stringify(
    {
      orderId: interaction.orderId || interaction.requestId,
      requestId: interaction.requestId || "",
      customerId: interaction.customerId || "",
      restaurantId: interaction.restaurantId || "",
      totalAmount: interaction.totalAmount || "0",
      currency: interaction.currency || "USD",
      approvedAt: "1970-01-01T00:00:00Z",
      riskBand: "standard"
    },
    null,
    2
  );
}

export default function InteractionDecisionPanel({ interaction }) {
  const stage = stageForInteraction(interaction);
  const stageId = stageIdForInteraction(interaction);
  const nextStage = nextReviewCheckpointAfterStage(stageId);
  const targetId = inferInteractionTargetId(interaction);
  const transportType = String(interaction.transportType || "").trim();
  const canComplete = shouldAllowAwaitCompletion(interaction.status, transportType);
  const displayOrderId = interaction.orderId || interaction.requestId;

  return (
    <article className="decision-panel">
      <div className="decision-main">
        <div className="panel-heading">
          <ClipboardCheck aria-hidden="true" size={20} />
          <div>
            <p className="eyebrow">{stage?.role || "Await checkpoint"}</p>
            <h2>{stage?.title || "Checkout decision"}</h2>
          </div>
        </div>
        <p className="lead">{stage?.responsibility || "Resume the paused checkout flow."}</p>
        <div className="order-summary">
          <strong>Order {shortIdentifier(displayOrderId)}</strong>
          <span>{interaction.payloadPreview || `${interaction.itemCount || 0} item(s)`}</span>
        </div>
        <dl className="metric-grid">
          <div>
            <dt>Order</dt>
            <dd>{shortIdentifier(displayOrderId, "n/a")}</dd>
          </div>
          <div>
            <dt>Amount</dt>
            <dd>{interaction.totalAmount || "n/a"} {interaction.currency || ""}</dd>
          </div>
          <div>
            <dt>Status</dt>
            <dd>{reviewStatusLabel(interaction.status, transportType)}</dd>
          </div>
          <div>
            <dt>Next</dt>
            <dd>{nextStage?.title || "Automatic downstream flow"}</dd>
          </div>
        </dl>
        {stage ? (
          <div className="payload-story">
            <div>
              <span>Comes in</span>
              <strong>{stage.inputSummary}</strong>
            </div>
            <div>
              <span>Goes out</span>
              <strong>{stage.outputSummary}</strong>
            </div>
          </div>
        ) : null}
        <div className="handoff-card">
          <span>{stage?.title || targetId}</span>
          <ArrowRight aria-hidden="true" size={16} />
          <strong>{nextStage?.title || "Automatic downstream flow"}</strong>
        </div>
        {stage ? <p className="field-help">{handoffSummaryForStage(stage.id)}</p> : null}
        <details className="identity-details">
          <summary>Technical identifiers</summary>
          <dl className="definition-list">
            <div>
              <dt>Interaction</dt>
              <dd>{interaction.interactionId || "n/a"}</dd>
            </div>
            <div>
              <dt>Execution</dt>
              <dd>{interaction.executionId || "n/a"}</dd>
            </div>
            <div>
              <dt>Service</dt>
              <dd>{targetId}</dd>
            </div>
          </dl>
        </details>
      </div>

      <form action={completeCheckoutInteraction} className="decision-action">
        <input type="hidden" name="interactionId" value={interaction.interactionId} />
        <input type="hidden" name="executionId" value={interaction.executionId} />
        <input type="hidden" name="targetId" value={targetId} />
        <input type="hidden" name="stageId" value={stageId} />
        <input type="hidden" name="requestId" value={interaction.requestId || ""} />
        <input type="hidden" name="orderId" value={interaction.orderId || ""} />
        <input type="hidden" name="outputType" value={interaction.outputType || ""} />
        <details className="payload-details">
          <summary>Review response payload</summary>
          <textarea
            id={`payload-${interaction.interactionId}`}
            name="payload"
            defaultValue={defaultPayloadForInteraction(interaction)}
            required
          />
        </details>
        <button className="button primary wide" type="submit" disabled={!canComplete} aria-disabled={!canComplete}>
          <CheckCircle2 aria-hidden="true" size={16} />
          {interactionActionLabel(interaction)}
        </button>
        {!canComplete ? (
          <p className="field-help">This item is still entering the approval desk. Refresh shortly.</p>
        ) : null}
        {interaction.executionId ? (
          <Link className="text-link" href={`/executions/${interaction.executionId}`}>
            View execution status
          </Link>
        ) : null}
      </form>
    </article>
  );
}
