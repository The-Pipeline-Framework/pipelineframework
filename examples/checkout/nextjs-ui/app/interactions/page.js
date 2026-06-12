import Link from "next/link";
import { Inbox, RotateCw } from "lucide-react";

import { fetchPendingInteractions } from "../../lib/tpf-client.js";
import { getUiConfig } from "../../lib/config.js";
import { checkpointProgress } from "../../lib/checkout-flow.js";
import { stageIdForInteraction } from "../../lib/checkout-ui.js";
import { ApprovalCheckpointRail } from "../components/FlowRail.js";
import AutoRefresh from "../components/AutoRefresh.js";
import InteractionDecisionPanel from "../components/InteractionDecisionPanel.js";
import JourneyTracker from "../components/JourneyTracker.js";
import StatusNotice from "../components/StatusNotice.js";
import { shortIdentifier } from "../../lib/checkout-ui.js";

export const dynamic = "force-dynamic";

function activeStageId(interactions, completedStageId) {
  const firstInteractionStageId = interactions.map(stageIdForInteraction).find(Boolean);
  if (firstInteractionStageId) {
    return firstInteractionStageId;
  }
  return checkpointProgress(completedStageId).next?.id || "";
}

export default async function PendingInteractionsPage({ searchParams }) {
  const resolvedSearchParams = await Promise.resolve(searchParams || {});
  const completedInteractionId = String(resolvedSearchParams.completed || "").trim();
  const completedStageId = String(resolvedSearchParams.completedStage || "").trim();
  const completionError = String(resolvedSearchParams.error || "").trim();
  const startedExecutionId = String(resolvedSearchParams.started || "").trim();
  const completedExecutionId = String(resolvedSearchParams.execution || "").trim();
  const progress = checkpointProgress(completedStageId);
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
    <main className="app-shell">
      {startedExecutionId || completedInteractionId ? <AutoRefresh attempts={40} intervalMs={1500} /> : null}
      <header className="topbar">
        <div>
          <p className="eyebrow">Approval desk</p>
          <h1>Checkout approvals</h1>
        </div>
        <nav className="topbar-actions" aria-label="Interaction actions">
          <Link className="button ghost" href="/">
            Start order
          </Link>
          <Link className="button ghost" href="/interactions">
            <RotateCw aria-hidden="true" size={16} />
            Refresh
          </Link>
        </nav>
      </header>

      <section className="desk-grid">
        <aside className="panel">
          <div className="panel-heading">
            <Inbox aria-hidden="true" size={20} />
            <div>
              <p className="eyebrow">Approval steps</p>
              <h2>Approval path</h2>
            </div>
          </div>
          <ApprovalCheckpointRail
            activeStageId={activeStageId(interactions, completedStageId)}
            activeLabel={interactions.length > 0 ? "Current approval task" : "Expected next approval"}
          />
          <JourneyTracker
            completedExecutionId={completedExecutionId}
            completedInteractionId={completedInteractionId}
            completedStageId={completedStageId}
            interactions={interactions}
            startedExecutionId={startedExecutionId}
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
            <StatusNotice tone="info" title="Checkout started">
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

          {errorMessage ? (
            <StatusNotice tone="error" title="TPF API unavailable">
              <p>Unable to reach the interaction endpoints.</p>
              <p><code>{errorMessage}</code></p>
            </StatusNotice>
          ) : interactions.length === 0 ? (
            <section className="empty-state">
              <Inbox aria-hidden="true" size={28} />
              <h2>No active approval</h2>
              <p>
                Submit an order and this desk will update when consumer validation reaches the first approval step.
                {awaitedStepId ? ` Filtering is constrained to TPF_AWAIT_STEP_ID=${awaitedStepId}.` : ""}
              </p>
              <div className="actions">
                <Link className="button primary" href="/">
                  Start a checkout
                </Link>
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
