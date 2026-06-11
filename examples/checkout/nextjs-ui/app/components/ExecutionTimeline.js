import { Circle, CircleCheck, CircleDashed } from "lucide-react";

import { CHECKOUT_FLOW_STAGES } from "../../lib/checkout-flow.js";

function stateFor(stage, status) {
  if (status === "SUCCEEDED") {
    return "complete";
  }
  if (status === "FAILED" || status === "DLQ") {
    return "blocked";
  }
  if (status === "WAITING_EXTERNAL" && stage.mode === "human") {
    return "active";
  }
  if (stage.order === 1) {
    return "active";
  }
  return "pending";
}

function IconForState({ state }) {
  if (state === "complete") {
    return <CircleCheck aria-hidden="true" size={16} />;
  }
  if (state === "active") {
    return <CircleDashed aria-hidden="true" size={16} />;
  }
  return <Circle aria-hidden="true" size={16} />;
}

export default function ExecutionTimeline({ status }) {
  const currentStatus = String(status?.status || "UNKNOWN");
  return (
    <ol className="execution-timeline">
      {CHECKOUT_FLOW_STAGES.map((stage) => {
        const state = stateFor(stage, currentStatus);
        return (
          <li className={state} key={stage.id}>
            <span className="timeline-icon">
              <IconForState state={state} />
            </span>
            <div>
              <strong>{stage.title}</strong>
              <span>{stage.mode === "human" ? "Approval step" : stage.role}</span>
            </div>
          </li>
        );
      })}
    </ol>
  );
}
