import {
  CheckCircle2,
  ChefHat,
  ClipboardCheck,
  Clock3,
  CreditCard,
  Flag,
  GitBranch,
  Hand,
  Route,
  Store,
  Ticket,
  Truck
} from "lucide-react";

import { CHECKOUT_FLOW_STAGES } from "../../lib/checkout-flow.js";

function iconFor(stage, activeStageId) {
  if (stage.id === activeStageId) {
    return CheckCircle2;
  }
  switch (stage.icon) {
    case "ticket":
      return Ticket;
    case "approval":
      return ClipboardCheck;
    case "restaurant":
      return Store;
    case "kitchen":
      return ChefHat;
    case "route":
      return Route;
    case "delivery":
      return Truck;
    case "payment":
      return CreditCard;
    case "finish":
      return Flag;
    default:
      return stage.mode === "human" ? Hand : GitBranch;
  }
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
                <span className="flow-title">{stage.plainTitle || stage.title}</span>
                <span className={`mode ${stage.mode}`}>{stage.mode === "human" ? "Needs a person" : "Runs itself"}</span>
              </div>
              <p>{stage.handoffSummary || stage.responsibility}</p>
              {!compact ? <small>{stage.serviceId}</small> : null}
            </div>
          </li>
        );
      })}
    </ol>
  );
}

export function ApprovalCheckpointRail({
  activeStageId = "",
  activeLabel = "Current approval task",
  activeIsActionable = true
}) {
  return (
    <ol className="checkpoint-rail">
      {CHECKOUT_FLOW_STAGES.filter((stage) => stage.mode === "human").map((stage) => {
        const isActive = stage.id === activeStageId;
        const showClock = isActive && activeIsActionable;
        return (
          <li className={isActive ? "active" : ""} key={stage.id}>
            <span className="checkpoint-index">{stage.order - 1}</span>
            <div>
              <strong>{stage.plainTitle || stage.title}</strong>
              <span>{isActive ? activeLabel : stage.handoffSummary}</span>
            </div>
            {showClock ? <Clock3 aria-hidden="true" size={16} /> : null}
          </li>
        );
      })}
    </ol>
  );
}
