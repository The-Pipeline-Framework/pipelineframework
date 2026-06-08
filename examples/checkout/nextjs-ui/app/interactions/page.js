import Link from "next/link";
import { completeCheckoutInteraction } from "../actions.js";
import { fetchPendingInteractions } from "../../lib/tpf-client.js";
import { getUiConfig } from "../../lib/config.js";
import {
  awaitStatusGuidance,
  describeAwaitStatus,
  shouldAllowAwaitCompletion,
  normalizeAwaitStatus
} from "../../lib/checkout-ui.js";

export const dynamic = "force-dynamic";

function formatDeadline(epochMs) {
  if (epochMs === null || epochMs === undefined || Number.isNaN(epochMs)) {
    return "n/a";
  }
  const date = new Date(epochMs);
  if (Number.isNaN(date.getTime())) {
    return "n/a";
  }
  return date.toISOString();
}

const statusOrder = ["WAITING", "DISPATCHING", "DISPATCHED", "FAILED", "CANCELLED", "TIMED_OUT", "EXPIRED"];

const statusPhase = new Map([
  ["WAITING", "Human action pause"],
  ["DISPATCHING", "Transport handoff"],
  ["DISPATCHED", "Transport handoff"],
  ["FAILED", "Terminal transport result"],
  ["TIMED_OUT", "Terminal timeout"],
  ["CANCELLED", "Terminal cancellation"],
  ["EXPIRED", "Terminal expiration"]
]);

function statusPhaseLabel(status) {
  return statusPhase.get(normalizeAwaitStatus(status)) || "Terminal or terminalizing";
}

function actionTextForStatus(status) {
  return shouldAllowAwaitCompletion(status) ? "Complete interaction" : "Tracking interaction";
}

function interactionTitle(interaction) {
  return (
    interaction.restaurantId ||
    interaction.orderId ||
    interaction.requestId ||
    interaction.interactionId ||
    "Checkout interaction"
  );
}

function interactionStatusClass(status) {
  if (status === "FAILED" || status === "TIMED_OUT" || status === "EXPIRED" || status === "CANCELLED") {
    return "status failed";
  }
  return "status waiting";
}

function defaultPayloadForInteraction(interaction) {
  const requestPayload = interaction.requestPayload;
  if (requestPayload && typeof requestPayload === "object" && Object.keys(requestPayload).length > 0) {
    return JSON.stringify(requestPayload, null, 2);
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
        currency: interaction.currency || "",
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
      totalAmount: interaction.totalAmount || "",
      currency: interaction.currency || "",
      approvedAt: "1970-01-01T00:00:00Z",
      riskBand: "standard"
    },
    null,
    2
  );
}

export default async function PendingInteractionsPage() {
  let interactions = [];
  let errorMessage = null;
  const uiConfig = getUiConfig();
  const awaitedStepId = String(uiConfig.awaitStepId || "").trim();
  try {
    interactions = await fetchPendingInteractions();
  } catch (error) {
    errorMessage = error?.message || "Unable to load pending interactions.";
  }

  return (
    <main className="shell">
      <section className="hero">
        <p className="eyebrow">Pending decisions</p>
        <h1>Human decision handoff</h1>
        <p>
          This inbox shows durable await entries surfaced through the generated TPF interaction gRPC APIs.
          Non-terminal await statuses can be in human-action states or in transport handoff states.
        </p>
        <div className="actions">
          <Link className="link-chip" href="/">
            Start another order
          </Link>
        </div>
      </section>

      <section className="section">
        {errorMessage ? (
          <div className="card">
            <h2>TPF API unavailable</h2>
            <p className="muted">Unable to reach the interaction endpoint right now.</p>
            <p className="muted">
              <code>{errorMessage}</code>
            </p>
            <p className="muted">
              Ensure the checkout services are running and <code>TPF_BASE_URL</code> points to them.
            </p>
          </div>
        ) : interactions.length === 0 ? (
          <div className="card empty">
            <h2>No active await entries</h2>
            <p className="muted">The inbox only shows durable non-terminal await interactions.</p>
            {awaitedStepId ? (
              <p className="muted">
                Filtering is currently constrained to <code>TPF_AWAIT_STEP_ID={awaitedStepId}</code>.
                Clear this value for all steps or wait for a matching record to be created.
              </p>
            ) : null}
            <p className="muted">Submit an order from the home page, then refresh this inbox.</p>
          </div>
        ) : (
          <div className="grid">
            {interactions
              .slice()
              .sort((left, right) => {
                const leftStatus = normalizeAwaitStatus(left.status);
                const rightStatus = normalizeAwaitStatus(right.status);
                const leftRank = statusOrder.indexOf(leftStatus);
                const rightRank = statusOrder.indexOf(rightStatus);
                return (leftRank === -1 ? statusOrder.length : leftRank) - (rightRank === -1 ? statusOrder.length : rightRank);
              })
              .map((interaction) => {
                const status = normalizeAwaitStatus(interaction.status);
                const canComplete = shouldAllowAwaitCompletion(status);
                return (
                  <article
                    className="card"
                    key={`${interaction.targetId || "checkout"}-${interaction.interactionId}`}
                  >
                    <div className="stack">
                      <span className="pill">{interaction.transportType}</span>
                      <h2>{interactionTitle(interaction)}</h2>
                      <p className={interactionStatusClass(status)}>
                        {describeAwaitStatus(status)}
                      </p>
                      <p className="muted">{statusPhaseLabel(status)}</p>
                      <p className="muted">{awaitStatusGuidance(status)}</p>
                      <dl className="meta">
                        <div>
                          <dt>Order / request</dt>
                          <dd>{interaction.orderId || interaction.requestId || "n/a"}</dd>
                        </div>
                        <div>
                          <dt>Execution</dt>
                          <dd>
                            {interaction.executionId ? (
                              <Link href={`/executions/${interaction.executionId}`}>{interaction.executionId}</Link>
                            ) : (
                              <span>n/a</span>
                            )}
                          </dd>
                        </div>
                        <div>
                          <dt>Await status</dt>
                          <dd>{status || "UNKNOWN"}</dd>
                        </div>
                        <div>
                          <dt>Amount</dt>
                          <dd>
                            {interaction.totalAmount || "n/a"} {interaction.currency || ""}
                          </dd>
                        </div>
                        <div>
                          <dt>Items</dt>
                          <dd>{interaction.payloadPreview || `${interaction.itemCount || 0} item(s)`}</dd>
                        </div>
                        <div>
                          <dt>Deadline</dt>
                          <dd>{formatDeadline(interaction.deadlineEpochMs)}</dd>
                        </div>
                        <div>
                          <dt>Step</dt>
                          <dd>{interaction.stepId || "manual boundary"}</dd>
                        </div>
                        <div>
                          <dt>Service</dt>
                          <dd>{interaction.targetId || "checkout-orchestrator-svc"}</dd>
                        </div>
                      </dl>
                    </div>

                    <form action={completeCheckoutInteraction} className="card stack" style={{ marginTop: "1rem" }}>
                      <h3>Resume interaction</h3>
                      <input type="hidden" name="interactionId" value={interaction.interactionId} />
                      <input type="hidden" name="executionId" value={interaction.executionId} />
                      <input type="hidden" name="targetId" value={interaction.targetId || "checkout-orchestrator-svc"} />
                      <div className="field">
                        <label htmlFor={`payload-${interaction.interactionId}`}>Response payload (JSON)</label>
                        <textarea
                          id={`payload-${interaction.interactionId}`}
                          name="payload"
                          defaultValue={defaultPayloadForInteraction(interaction)}
                          required
                        />
                      </div>
                      <button
                        className="button"
                        type="submit"
                        disabled={!canComplete}
                        aria-disabled={!canComplete}
                      >
                        {actionTextForStatus(status)}
                      </button>
                      {!canComplete ? (
                        <p className="muted">Completion is available once status becomes WAITING.</p>
                      ) : null}
                    </form>
                  </article>
                );
              })}
          </div>
        )}
      </section>
    </main>
  );
}
