import Link from "next/link";
import { fetchExecutionResult, fetchExecutionStatus } from "../../../lib/tpf-client.js";

function statusClass(status) {
  if (status === "SUCCEEDED") {
    return "status done";
  }
  if (status === "FAILED" || status === "DLQ") {
    return "status failed";
  }
  return "status waiting";
}

function statusGuidance(status) {
  if (status === "WAITING_EXTERNAL") {
    return "Execution is waiting for an external resume interaction. Check the inbox for a pending checkpoint interaction.";
  }
  if (status === "WAITING") {
    return "Execution status is waiting for an external decision at the active await step. The inbox shows its non-terminal await states (WAITING/DISPATCHING/DISPATCHED).";
  }
  if (status === "WAIT_RETRY") {
    return "Execution failed temporarily and will retry automatically according to orchestration settings.";
  }
  if (status === "QUEUED" || status === "RUNNING") {
    return "Execution is in queue/processing and is being worked by backend workers. In this phase, the inbox is expected to be empty.";
  }
  return "Execution has not reached a terminal success state yet. Refresh this page for the latest status.";
}

export default async function ExecutionPage({ params }) {
  const { executionId } = await params;
  let status = null;
  let result = null;
  let errorMessage = null;

  try {
    status = await fetchExecutionStatus(executionId);
    result = status.status === "SUCCEEDED" ? await fetchExecutionResult(executionId) : null;
  } catch (error) {
    errorMessage = error?.message || "Unable to load execution data.";
  }

  return (
    <main className="shell">
      <section className="hero">
        <p className="eyebrow">Execution status</p>
        <h1>Track the checkout execution</h1>
        <p>
          This page reads the generated runtime gRPC status APIs. If the order is waiting externally,
          complete the interaction from the inbox and refresh. If it is queued or running, the flow is still
          processing and no inbox entry is expected yet.
        </p>
        <div className="actions">
          <Link className="link-chip" href="/interactions">
            Go to pending inbox
          </Link>
          <Link className="link-chip" href={`/executions/${executionId}`}>
            Refresh status
          </Link>
        </div>
      </section>

      <section className="section">
        {errorMessage ? (
          <div className="card">
            <h2>TPF API unavailable</h2>
            <p className="muted">Execution detail could not be loaded.</p>
            <p className="muted">
              <code>{errorMessage}</code>
            </p>
          </div>
        ) : (
          <>
                <div className="card stack">
                  <h2>Execution</h2>
                  <p className={statusClass(status.status)}>{status.status}</p>
                  <dl className="meta">
                <div>
                  <dt>Execution ID</dt>
                  <dd>{status.executionId}</dd>
                </div>
                <div>
                  <dt>Current step index</dt>
                  <dd>{status.stepIndex}</dd>
                </div>
                <div>
                  <dt>Attempt</dt>
                  <dd>{status.attempt}</dd>
                </div>
                <div>
                  <dt>Error</dt>
                  <dd>{status.errorCode || status.errorMessage || "none"}</dd>
                </div>
              </dl>
            </div>

            {result ? (
              <div className="card stack">
                <h2>Terminal order state</h2>
                <dl className="meta">
                  <div>
                    <dt>Outcome</dt>
                    <dd>{String(result.outcome || result.status || "done")}</dd>
                  </div>
                  <div>
                    <dt>Resolved</dt>
                    <dd>{result.resolvedAt || status.completedAt || "n/a"}</dd>
                  </div>
                </dl>
                <h3>Raw payload</h3>
                <pre className="raw-json">{JSON.stringify(result, null, 2)}</pre>
              </div>
            ) : (
              <div className="card">
                <h2>Result not available yet</h2>
                <p className="muted">{statusGuidance(status.status)}</p>
              </div>
            )}
          </>
        )}
      </section>
    </main>
  );
}
