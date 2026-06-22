import { ArrowRight, ServerCog } from "lucide-react";

import { CHECKOUT_FLOW_STAGES, handoffSummaryForStage, nextStageAfter, stageById } from "../../lib/checkout-flow.js";

export default function ServiceStagePanel({ stageId = "consumer-approval" }) {
  const stage = stageById(stageId) || CHECKOUT_FLOW_STAGES[0];
  const nextStage = nextStageAfter(stage.id);

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
      <div className="stage-sketch" aria-hidden="true">
        <span>{stage.sketch}</span>
      </div>
      <dl className="definition-list">
        <div>
          <dt>What comes in</dt>
          <dd>{stage.inputSummary}</dd>
        </div>
        <div>
          <dt>What goes out</dt>
          <dd>{stage.outputSummary}</dd>
        </div>
        <div>
          <dt>Checkpoint handoff</dt>
          <dd>{handoffSummaryForStage(stage.id)}</dd>
        </div>
        <div>
          <dt>Owner</dt>
          <dd>{stage.serviceId}</dd>
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
