import Link from "next/link";
import { Activity, Inbox, RotateCw } from "lucide-react";

import { fetchExecutionResult, fetchExecutionStatus } from "../../../lib/tpf-client.js";
import ExecutionTimeline from "../../components/ExecutionTimeline.js";
import StatusNotice from "../../components/StatusNotice.js";

function statusTone(status) {
  if (status === "SUCCEEDED") {
    return "success";
  }
  if (status === "FAILED" || status === "DLQ") {
    return "error";
  }
  return "info";
}

function statusGuidance(status) {
  if (status === "WAITING_EXTERNAL") {
    return "The execution is paused at an approval step. Continue it from the approval desk.";
  }
  if (status === "QUEUED" || status === "RUNNING") {
    return "The backend worker is advancing this execution. Refresh for the next state.";
  }
  if (status === "WAIT_RETRY") {
    return "The execution will retry according to the orchestrator schedule.";
  }
  if (status === "SUCCEEDED") {
    return "The execution reached a terminal successful state.";
  }
  return "The execution has not reached a terminal success state yet.";
}

export default async function ExecutionPage({ params, searchParams }) {
  const { executionId } = await params;
  const resolvedSearchParams = await Promise.resolve(searchParams || {});
  const targetId = String(resolvedSearchParams?.target || "").trim();
  let status = null;
  let result = null;
  let errorMessage = null;

  try {
    status = await fetchExecutionStatus(executionId, targetId);
    result = status.status === "SUCCEEDED" ? await fetchExecutionResult(executionId, targetId) : null;
  } catch (error) {
    errorMessage = error?.message || "Unable to load execution data.";
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Execution</p>
          <h1>Run status</h1>
        </div>
        <nav className="topbar-actions" aria-label="Execution actions">
          <Link className="button ghost" href="/interactions">
            <Inbox aria-hidden="true" size={16} />
            Approval desk
          </Link>
          <Link className="button ghost" href={`/executions/${executionId}${targetId ? `?target=${encodeURIComponent(targetId)}` : ""}`}>
            <RotateCw aria-hidden="true" size={16} />
            Refresh
          </Link>
        </nav>
      </header>

      {errorMessage ? (
        <StatusNotice tone="error" title="Execution detail could not be loaded">
          <p><code>{errorMessage}</code></p>
        </StatusNotice>
      ) : (
        <section className="execution-grid">
          <section className="panel">
            <div className="panel-heading">
              <Activity aria-hidden="true" size={20} />
              <div>
                <p className="eyebrow">Lifecycle</p>
                <h2>{status.status}</h2>
              </div>
            </div>
            <p className="lead">{statusGuidance(status.status)}</p>
            {status.status === "WAITING_EXTERNAL" ? (
              <Link className="button primary" href="/interactions">
                <Inbox aria-hidden="true" size={16} />
                Continue from approval desk
              </Link>
            ) : null}
            <ExecutionTimeline status={status} />
          </section>

          <aside className="panel">
            <StatusNotice tone={statusTone(status.status)} title="Current runtime state">
              <dl className="definition-list">
                <div>
                  <dt>Execution ID</dt>
                  <dd>{status.executionId}</dd>
                </div>
                <div>
                  <dt>Step index</dt>
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
            </StatusNotice>
            {result ? (
              <details className="payload-details" open>
                <summary>Terminal payload</summary>
                <pre className="raw-json">{JSON.stringify(result, null, 2)}</pre>
              </details>
            ) : null}
          </aside>
        </section>
      )}
    </main>
  );
}
