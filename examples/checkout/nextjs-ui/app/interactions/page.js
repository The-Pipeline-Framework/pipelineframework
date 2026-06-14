import Link from "next/link";
import { Inbox, RotateCw } from "lucide-react";

import { fetchJourneyTrace, fetchPendingInteractions } from "../../lib/tpf-client.js";
import { getUiConfig } from "../../lib/config.js";
import { checkpointProgress } from "../../lib/checkout-flow.js";
import AutoRefresh from "../components/AutoRefresh.js";
import InteractionDecisionPanel from "../components/InteractionDecisionPanel.js";
import JourneyTracker from "../components/JourneyTracker.js";
import StatusNotice from "../components/StatusNotice.js";
import { shortIdentifier } from "../../lib/checkout-ui.js";

export const dynamic = "force-dynamic";

export default async function PendingInteractionsPage({ searchParams }) {
  const resolvedSearchParams = await Promise.resolve(searchParams || {});
  const completedInteractionId = String(resolvedSearchParams.completed || "").trim();
  const completedStageId = String(resolvedSearchParams.completedStage || "").trim();
  const completionError = String(resolvedSearchParams.error || "").trim();
  const startedExecutionId = String(resolvedSearchParams.started || "").trim();
  const completedExecutionId = String(resolvedSearchParams.execution || "").trim();
  const requestId = String(resolvedSearchParams.requestId || "").trim();
  const orderId = String(resolvedSearchParams.orderId || "").trim();
  const progress = checkpointProgress(completedStageId);
  let interactions = [];
  let traceEvents = [];
  let errorMessage = null;
  let traceErrorMessage = null;
  const uiConfig = getUiConfig();
  const awaitedStepId = String(uiConfig.awaitStepId || "").trim();
  const isTrackingOrder = Boolean(startedExecutionId || completedInteractionId || requestId || orderId);
  try {
    interactions = await fetchPendingInteractions();
  } catch (error) {
    errorMessage = error?.message || "Unable to load pending interactions.";
  }
  try {
    traceEvents = await fetchJourneyTrace({ requestId, orderId });
  } catch (error) {
    traceErrorMessage = error?.message || "Unable to load checkout journey trace.";
  }

  return (
    <main className="app-shell">
      {startedExecutionId || completedInteractionId || requestId || orderId ? (
        <AutoRefresh
          attempts={90}
          intervalMs={900}
          refreshKey={`${startedExecutionId}:${completedInteractionId}:${completedStageId}:${requestId}:${orderId}:${traceEvents.length}`}
        />
      ) : null}
      <header className="topbar">
        <div>
          <p className="eyebrow">Approval desk</p>
          <h1>Checkout approvals</h1>
        </div>
        <nav className="topbar-actions" aria-label="Interaction actions">
          <Link className="button ghost" href="/">
            Start another order
          </Link>
          <Link className="button ghost" href="/interactions">
            <RotateCw aria-hidden="true" size={16} />
            Refresh
          </Link>
        </nav>
      </header>

      <section className="desk-grid">
        <aside className="panel">
          <JourneyTracker
            completedExecutionId={completedExecutionId}
            completedInteractionId={completedInteractionId}
            completedStageId={completedStageId}
            interactions={interactions}
            orderId={orderId}
            requestId={requestId}
            startedExecutionId={startedExecutionId}
            traceErrorMessage={traceErrorMessage}
            traceEvents={traceEvents}
          />
        </aside>

        <section className="desk-main">
          {completedInteractionId ? (
            <StatusNotice tone="success" title="Checkpoint resumed">
              <p>
                {progress.completed?.title || "The interaction"} was completed.{" "}
                {progress.next
                  ? `${progress.next.title} should appear here when the service handoff reaches the next await boundary.`
                  : "No approval steps remain; downstream services continue automatically."}
              </p>
              <p className="muted">
                Refreshing briefly while the next service catches up. Interaction <code>{shortIdentifier(completedInteractionId)}</code>
              </p>
            </StatusNotice>
          ) : null}

          {startedExecutionId ? (
            <StatusNotice tone="success" title="Checkout started">
              <p>
                The order is moving through checkout intake and consumer validation. The first approval task will appear here shortly.
              </p>
              <p className="muted">
                Tracking run <code>{shortIdentifier(startedExecutionId)}</code>; this page refreshes briefly while the backend reaches the first checkpoint.
              </p>
            </StatusNotice>
          ) : null}

          {completionError ? (
            <StatusNotice tone="error" title="Completion failed">
              <p><code>{completionError}</code></p>
            </StatusNotice>
          ) : null}

          {traceErrorMessage ? (
            <StatusNotice tone="warning" title="Journey trace unavailable">
              <p>The approval flow can continue, but automatic downstream checkpoints cannot be shown yet.</p>
              <p><code>{traceErrorMessage}</code></p>
            </StatusNotice>
          ) : null}

          {errorMessage ? (
            <StatusNotice tone="error" title="TPF API unavailable">
              <p>Unable to reach the interaction endpoints.</p>
              <p><code>{errorMessage}</code></p>
            </StatusNotice>
          ) : interactions.length === 0 ? (
            <section className="empty-state">
              <Inbox aria-hidden="true" size={28} />
              <h2>{isTrackingOrder ? "Waiting for the next approval" : "No active approval"}</h2>
              {isTrackingOrder ? (
                <p>
                  The order is already moving. This page is refreshing while the current service reaches an approval boundary or finishes the automatic checkpoint chain.
                  {awaitedStepId ? ` Filtering is constrained to TPF_AWAIT_STEP_ID=${awaitedStepId}.` : ""}
                </p>
              ) : (
                <p>
                  There is no checkout waiting for review right now.
                  {awaitedStepId ? ` Filtering is constrained to TPF_AWAIT_STEP_ID=${awaitedStepId}.` : ""}
                </p>
              )}
              <div className="actions">
                <Link className="button ghost" href="/interactions">
                  Refresh desk
                </Link>
              </div>
            </section>
          ) : (
            <div className="decision-list">
              {interactions.map((interaction) => (
                <InteractionDecisionPanel
                  interaction={interaction}
                  key={`${interaction.targetId || "checkout"}-${interaction.interactionId}`}
                />
              ))}
            </div>
          )}
        </section>
      </section>
    </main>
  );
}
