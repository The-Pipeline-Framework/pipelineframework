"use client";

import { useEffect, useMemo, useState } from "react";
import { Circle, CircleCheck, CircleDashed, RotateCcw } from "lucide-react";

import {
  CHECKOUT_FLOW_STAGES,
  REVIEW_CHECKPOINTS,
  stageById,
  stageForInteraction
} from "../../lib/checkout-flow.js";
import { shortIdentifier } from "../../lib/checkout-ui.js";

const STORAGE_KEY = "tpfgo.checkout.journey.v1";

function nowIso() {
  return new Date().toISOString();
}

function eventKey(event) {
  return [
    event.type || "",
    event.stageId || "",
    event.executionId || "",
    event.interactionId || ""
  ].join("|");
}

function readJourney() {
  try {
    const parsed = JSON.parse(window.localStorage.getItem(STORAGE_KEY) || "{}");
    return {
      rootExecutionId: String(parsed.rootExecutionId || ""),
      events: Array.isArray(parsed.events) ? parsed.events : []
    };
  } catch (_error) {
    return { rootExecutionId: "", events: [] };
  }
}

function writeJourney(journey) {
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify({
    rootExecutionId: journey.rootExecutionId || "",
    events: journey.events.slice(-24)
  }));
}

function mergeEvents(existing, additions) {
  const seen = new Set(existing.map(eventKey));
  const merged = [...existing];
  for (const event of additions) {
    const key = eventKey(event);
    if (!seen.has(key)) {
      seen.add(key);
      merged.push(event);
    }
  }
  return merged;
}

function stageState(stage, events, activeStageId) {
  if (stage.id === activeStageId) {
    return "active";
  }
  if (events.some((event) => event.stageId === stage.id && event.type === "completed")) {
    return "complete";
  }
  if (stage.id === "checkout" && events.some((event) => event.type === "started")) {
    return "complete";
  }
  if (stage.mode === "automatic" && events.some((event) => event.type === "completed" && event.stageId === "restaurant-acceptance")) {
    return stage.order >= 4 ? "released" : "pending";
  }
  return "pending";
}

function StateIcon({ state }) {
  if (state === "complete") {
    return <CircleCheck aria-hidden="true" size={15} />;
  }
  if (state === "active" || state === "released") {
    return <CircleDashed aria-hidden="true" size={15} />;
  }
  return <Circle aria-hidden="true" size={15} />;
}

function eventLabel(event) {
  if (event.type === "started") {
    return "Order submitted";
  }
  const stage = stageById(event.stageId);
  if (event.type === "appeared") {
    return `${stage?.title || "Approval"} appeared`;
  }
  if (event.type === "completed") {
    return `${stage?.title || "Approval"} completed`;
  }
  return stage?.title || "Checkout event";
}

export default function JourneyTracker({
  completedExecutionId = "",
  completedInteractionId = "",
  completedStageId = "",
  interactions = [],
  startedExecutionId = ""
}) {
  const [journey, setJourney] = useState({ rootExecutionId: "", events: [] });

  const signals = useMemo(() => {
    const additions = [];
    const started = String(startedExecutionId || "").trim();
    if (started) {
      additions.push({
        type: "started",
        stageId: "checkout",
        executionId: started,
        at: nowIso()
      });
    }

    for (const interaction of interactions || []) {
      const stage = stageForInteraction(interaction);
      if (!stage) {
        continue;
      }
      additions.push({
        type: "appeared",
        stageId: stage.id,
        executionId: String(interaction.executionId || ""),
        interactionId: String(interaction.interactionId || ""),
        at: nowIso()
      });
    }

    const completedStage = String(completedStageId || "").trim();
    const completedInteraction = String(completedInteractionId || "").trim();
    if (completedStage || completedInteraction) {
      additions.push({
        type: "completed",
        stageId: completedStage,
        executionId: String(completedExecutionId || ""),
        interactionId: completedInteraction,
        at: nowIso()
      });
    }

    return additions;
  }, [completedExecutionId, completedInteractionId, completedStageId, interactions, startedExecutionId]);

  useEffect(() => {
    const stored = readJourney();
    const started = String(startedExecutionId || "").trim();
    const base = started && started !== stored.rootExecutionId
      ? { rootExecutionId: started, events: [] }
      : stored;
    const next = {
      rootExecutionId: base.rootExecutionId || started,
      events: mergeEvents(base.events, signals)
    };
    writeJourney(next);
    setJourney(next);
  }, [signals, startedExecutionId]);

  const activeStage = interactions.map(stageForInteraction).find(Boolean);
  const activeStageId = activeStage?.id || "";
  const hasEvents = journey.events.length > 0;

  function clearJourney() {
    const empty = { rootExecutionId: "", events: [] };
    writeJourney(empty);
    setJourney(empty);
  }

  return (
    <section className="journey-tracker" aria-label="Order journey">
      <div className="journey-heading">
        <div>
          <p className="eyebrow">Order journey</p>
          <h3>{journey.rootExecutionId ? `Run ${shortIdentifier(journey.rootExecutionId)}` : "No order tracked"}</h3>
        </div>
        {hasEvents ? (
          <button className="icon-button" type="button" onClick={clearJourney} aria-label="Clear tracked journey">
            <RotateCcw aria-hidden="true" size={15} />
          </button>
        ) : null}
      </div>

      <ol className="journey-stages">
        {CHECKOUT_FLOW_STAGES.map((stage) => {
          const state = stageState(stage, journey.events, activeStageId);
          return (
            <li className={state} key={stage.id}>
              <span className="journey-icon"><StateIcon state={state} /></span>
              <div>
                <strong>{stage.shortTitle}</strong>
                <span>
                  {state === "complete"
                    ? "Observed"
                    : state === "active"
                      ? "Waiting for review"
                      : state === "released"
                        ? "Released downstream"
                        : stage.mode === "human"
                          ? "Not reached yet"
                          : "Automatic"}
                </span>
              </div>
            </li>
          );
        })}
      </ol>

      <div className="journey-events">
        {hasEvents ? journey.events.slice(-5).reverse().map((event) => (
          <div key={eventKey(event)}>
            <strong>{eventLabel(event)}</strong>
            <span>
              {event.executionId ? `Run ${shortIdentifier(event.executionId)}` : "Await state observed"}
              {event.interactionId ? ` · Task ${shortIdentifier(event.interactionId)}` : ""}
            </span>
          </div>
        )) : (
          <p>Start an order and this panel will keep the observed path visible after each approval disappears from the inbox.</p>
        )}
      </div>

      <p className="field-help">
        The approval desk observes the two review boundaries directly. After restaurant acceptance, the remaining modules continue automatically.
      </p>
    </section>
  );
}
