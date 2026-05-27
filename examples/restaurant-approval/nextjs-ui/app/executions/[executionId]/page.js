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

export default async function ExecutionPage({ params }) {
  const { executionId } = await params;
  const status = await fetchExecutionStatus(executionId);
  const result = status.status === "SUCCEEDED" ? await fetchExecutionResult(executionId) : null;

  return (
    <main className="shell">
      <section className="hero">
        <p className="eyebrow">Execution status</p>
        <h1>Track one order through wait and resume.</h1>
        <p>
          This page reads the generated queue-async execution APIs. If the order is still waiting,
          complete it from the pending inbox and refresh this page.
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
                <dd>{result.outcome}</dd>
              </div>
              <div>
                <dt>Restaurant status</dt>
                <dd>{result.restaurantStatus}</dd>
              </div>
              <div>
                <dt>Summary</dt>
                <dd>{result.summary}</dd>
              </div>
              <div>
                <dt>Resolved at</dt>
                <dd>{result.resolvedAt}</dd>
              </div>
            </dl>
          </div>
        ) : (
          <div className="card">
            <h2>Result not available yet</h2>
            <p className="muted">
              The execution has not reached a terminal success state. If it is waiting externally,
              complete the restaurant decision from the inbox.
            </p>
          </div>
        )}
      </section>
    </main>
  );
}
