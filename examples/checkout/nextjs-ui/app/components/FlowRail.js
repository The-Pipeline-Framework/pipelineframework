import { CheckCircle2, Clock3, GitBranch, Hand } from "lucide-react";

import { CHECKOUT_FLOW_STAGES } from "../../lib/checkout-flow.js";

function iconFor(stage, activeStageId) {
  if (stage.id === activeStageId) {
    return CheckCircle2;
  }
  return stage.mode === "human" ? Hand : GitBranch;
}

export default function FlowRail({ activeStageId = "", compact = false }) {
  return (
    <ol className={`flow-rail ${compact ? "compact" : ""}`}>
      {CHECKOUT_FLOW_STAGES.map((stage) => {
        const Icon = iconFor(stage, activeStageId);
        return (
          <li className={stage.id === activeStageId ? "active" : ""} key={stage.id}>
            <div className="flow-icon">
              <Icon aria-hidden="true" size={16} />
            </div>
            <div>
              <div className="flow-row">
                <span className="flow-title">{stage.title}</span>
                <span className={`mode ${stage.mode}`}>{stage.mode === "human" ? "Approval" : "Auto"}</span>
              </div>
              <p>{stage.publication}</p>
              {!compact ? <small>{stage.serviceId}</small> : null}
            </div>
          </li>
        );
      })}
    </ol>
  );
}

export function ApprovalCheckpointRail({ activeStageId = "", activeLabel = "Current approval task" }) {
  return (
    <ol className="checkpoint-rail">
      {CHECKOUT_FLOW_STAGES.filter((stage) => stage.mode === "human").map((stage) => {
        const isActive = stage.id === activeStageId;
        return (
          <li className={isActive ? "active" : ""} key={stage.id}>
            <span className="checkpoint-index">{stage.order - 1}</span>
            <div>
              <strong>{stage.title}</strong>
              <span>{isActive ? activeLabel : stage.responsibility}</span>
            </div>
            {isActive ? <Clock3 aria-hidden="true" size={16} /> : null}
          </li>
        );
      })}
    </ol>
  );
}
