import { Circle, CircleCheck, CircleDashed } from "lucide-react";

import { CHECKOUT_FLOW_STAGES } from "../../lib/checkout-flow.js";

function currentStepOrder(status) {
  const rawStep = status?.currentStep ?? status?.current_step_index ?? status?.stepIndex;
  const stepIndex = Number(rawStep);
  if (!Number.isFinite(stepIndex) || stepIndex < 0) {
    return 1;
  }
  return stepIndex + 1;
}

function stateFor(stage, status) {
  const currentStatus = String(status?.status || "UNKNOWN");
  const currentOrder = currentStepOrder(status);

  if (currentStatus === "SUCCEEDED") {
    return "complete";
  }
  if (currentStatus === "FAILED" || currentStatus === "DLQ") {
    return stage.order === currentOrder ? "blocked" : stage.order < currentOrder ? "complete" : "pending";
  }
  if (currentStatus === "WAITING_EXTERNAL" && stage.mode === "human" && stage.order === currentOrder) {
    return "active";
  }
  if (stage.order < currentOrder) {
    return "complete";
  }
  if (stage.order === currentOrder) {
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
  return (
    <ol className="execution-timeline">
      {CHECKOUT_FLOW_STAGES.map((stage) => {
        const state = stateFor(stage, status);
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
