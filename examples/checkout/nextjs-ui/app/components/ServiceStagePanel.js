import { ArrowRight, ServerCog } from "lucide-react";

import { CHECKOUT_FLOW_STAGES, stageById } from "../../lib/checkout-flow.js";

export default function ServiceStagePanel({ stageId = "consumer-approval" }) {
  const stage = stageById(stageId) || CHECKOUT_FLOW_STAGES[0];
  const nextStage = CHECKOUT_FLOW_STAGES.find((candidate) => candidate.order === stage.order + 1);

  return (
    <aside className="panel stage-panel">
      <div className="panel-heading">
        <ServerCog aria-hidden="true" size={20} />
        <div>
          <p className="eyebrow">Service focus</p>
          <h2>{stage.title}</h2>
        </div>
      </div>
      <p className="lead">{stage.responsibility}</p>
      <dl className="definition-list">
        <div>
          <dt>Owner</dt>
          <dd>{stage.serviceId}</dd>
        </div>
        <div>
          <dt>Receives</dt>
          <dd>{stage.receives}</dd>
        </div>
        <div>
          <dt>Emits</dt>
          <dd>{stage.emits}</dd>
        </div>
        <div>
          <dt>Endpoint</dt>
          <dd>{stage.endpoint}</dd>
        </div>
      </dl>
      {nextStage ? (
        <div className="handoff-card">
          <span>{stage.shortTitle}</span>
          <ArrowRight aria-hidden="true" size={16} />
          <strong>{nextStage.shortTitle}</strong>
        </div>
      ) : null}
    </aside>
  );
}
