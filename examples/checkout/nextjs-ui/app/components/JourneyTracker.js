"use client";

import { useEffect, useMemo, useState } from "react";
import { Circle, CircleCheck, CircleDashed, Maximize2, RotateCcw, X } from "lucide-react";

import {
  CHECKOUT_FLOW_STAGES,
  stageById,
  stageForInteraction
} from "../../lib/checkout-flow.js";
import { shortIdentifier } from "../../lib/checkout-ui.js";

const STORAGE_KEY = "tpfgo.checkout.journey.v1";

function nowIso() {
  return new Date().toISOString();
}

function isoFromEpochMs(value) {
  const epochMs = Number(value || 0);
  if (!Number.isFinite(epochMs) || epochMs <= 0) {
    return "";
  }
  return new Date(epochMs).toISOString();
}

function timeLabel(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  });
}

function elapsedLabel(startValue, endValue) {
  const start = new Date(startValue);
  const end = new Date(endValue);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    return "";
  }
  const elapsedMs = Math.max(0, end.getTime() - start.getTime());
  if (elapsedMs < 1000) {
    return "under 1s";
  }
  const seconds = Math.round(elapsedMs / 100) / 10;
  return `${seconds}s`;
}

function eventKey(event) {
  return [
    event.type || "",
    event.stageId || "",
    event.executionId || "",
    event.interactionId || "",
    event.publication || "",
    event.requestId || "",
    event.orderId || ""
  ].join("|");
}

function eventFor(events, type, stageId = "") {
  return events.find((event) =>
    event.type === type && (!stageId || event.stageId === stageId)) || null;
}

function downstreamReleaseEvent(events) {
  return eventFor(events, "completed", "restaurant-acceptance");
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function sanitizeIso(value) {
  const text = String(value || "");
  if (!text) {
    return "";
  }
  return Number.isNaN(new Date(text).getTime()) ? "" : text;
}

function sanitizeEvent(event) {
  if (!isPlainObject(event)) {
    return null;
  }
  return {
    type: String(event.type || ""),
    stageId: String(event.stageId || ""),
    executionId: String(event.executionId || ""),
    interactionId: String(event.interactionId || ""),
    publication: String(event.publication || ""),
    requestId: String(event.requestId || ""),
    orderId: String(event.orderId || ""),
    at: sanitizeIso(event.at)
  };
}

function sanitizeEvents(events) {
  if (!Array.isArray(events)) {
    return [];
  }
  return events
    .map(sanitizeEvent)
    .filter(Boolean);
}

function readJourney() {
  try {
    const parsed = JSON.parse(window.localStorage.getItem(STORAGE_KEY) || "{}");
    return {
      rootExecutionId: String(parsed.rootExecutionId || ""),
      lastUpdatedAt: String(parsed.lastUpdatedAt || ""),
      events: sanitizeEvents(parsed.events)
    };
  } catch (_error) {
    return { rootExecutionId: "", lastUpdatedAt: "", events: [] };
  }
}

function writeJourney(journey) {
  const events = sanitizeEvents(journey.events);
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify({
    rootExecutionId: journey.rootExecutionId || "",
    lastUpdatedAt: journey.lastUpdatedAt || nowIso(),
    events: events.slice(-24)
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
    } else {
      const index = merged.findIndex((candidate) => eventKey(candidate) === key);
      if (index >= 0 && !merged[index].at && event.at) {
        merged[index] = { ...merged[index], at: event.at };
      }
    }
  }
  return merged;
}

function traceEventToObserved(event) {
  return {
    type: "checkpoint",
    stageId: String(event.stageId || ""),
    publication: String(event.publication || ""),
    requestId: String(event.requestId || ""),
    orderId: String(event.orderId || ""),
    at: isoFromEpochMs(event.observedAtEpochMs)
  };
}

function traceEventKey(event) {
  return [
    event.stageId || "",
    event.publication || "",
    event.requestId || "",
    event.orderId || ""
  ].join("|");
}

function dedupeTraceEvents(events) {
  const unique = new Map();
  for (const event of events || []) {
    const key = traceEventKey(event);
    if (!unique.has(key)) {
      unique.set(key, event);
    }
  }
  return [...unique.values()];
}

function stageState(stage, events, activeStageId, traceByStage) {
  if (stage.id === activeStageId) {
    return "active";
  }
  if (traceByStage.has(stage.id)) {
    return "complete";
  }
  if (events.some((event) => event.stageId === stage.id && event.type === "completed")) {
    return "complete";
  }
  if (stage.id === "checkout" && events.some((event) => event.type === "started")) {
    return "complete";
  }
  if (stage.mode === "automatic" && events.some((event) => event.type === "completed" && event.stageId === "restaurant-acceptance")) {
    return stage.order >= 4 ? "waiting-trace" : "pending";
  }
  return "pending";
}

function statusForStage(stage, state) {
  if (state === "complete") {
    return stage.mode === "human" ? "Completed here" : "Checkpoint observed";
  }
  if (state === "active") {
    return "Ready for review";
  }
  if (state === "waiting-trace") {
    return "Waiting for checkpoint";
  }
  return stage.mode === "human" ? "Not reached yet" : "Runs by itself";
}

function StateIcon({ state }) {
  if (state === "complete") {
    return <CircleCheck aria-hidden="true" size={22} />;
  }
  if (state === "active" || state === "waiting-trace") {
    return <CircleDashed aria-hidden="true" size={22} />;
  }
  return <Circle aria-hidden="true" size={22} />;
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
  if (event.type === "checkpoint") {
    return `${stage?.title || "Stage"} checkpoint observed`;
  }
  return stage?.title || "Checkout event";
}

function prettyPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return "";
  }
  return JSON.stringify(payload, null, 2);
}

function compactPayloadLabel(payload) {
  if (!payload || typeof payload !== "object") {
    return "";
  }
  const orderId = payload.orderId || payload.order_id || payload.requestId || payload.request_id || "";
  const amount = payload.totalAmount || payload.total_amount || "";
  const currency = payload.currency || payload.currencyCode || payload.currency_code || "";
  const items = Array.isArray(payload.items) ? `${payload.items.length} item(s)` : "";
  return [orderId ? `order ${shortIdentifier(orderId)}` : "", amount ? `${amount} ${currency}`.trim() : "", items]
    .filter(Boolean)
    .join(" · ");
}

export default function JourneyTracker({
  completedExecutionId = "",
  completedInteractionId = "",
  completedStageId = "",
  interactions = [],
  orderId = "",
  requestId = "",
  startedExecutionId = "",
  traceErrorMessage = "",
  traceEvents = []
}) {
  const [journey, setJourney] = useState({ rootExecutionId: "", lastUpdatedAt: "", events: [] });
  const [payloadDialog, setPayloadDialog] = useState(null);

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
        at: isoFromEpochMs(interaction.createdAtEpochMs) || nowIso()
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
    if (!started && signals.length === 0) {
      if (stored.rootExecutionId) {
        const refreshed = {
          ...stored,
          lastUpdatedAt: nowIso()
        };
        writeJourney(refreshed);
        setJourney(refreshed);
      } else {
        setJourney(stored);
      }
      return;
    }

    const base = started && started !== stored.rootExecutionId
      ? { rootExecutionId: started, events: [] }
      : stored;
    const next = {
      rootExecutionId: base.rootExecutionId || started,
      lastUpdatedAt: signals.length > 0 ? nowIso() : base.lastUpdatedAt,
      events: mergeEvents(base.events, signals)
    };
    writeJourney(next);
    setJourney(next);
  }, [signals, startedExecutionId]);

  const uniqueTraceEvents = dedupeTraceEvents(traceEvents);
  const activeStage = interactions.map(stageForInteraction).find(Boolean);
  const activeStageId = activeStage?.id || "";
  const traceByStage = new Map(
    uniqueTraceEvents
      .filter((event) => event?.stageId)
      .map((event) => [event.stageId, event])
  );
  const hasEvents = journey.events.length > 0 || uniqueTraceEvents.length > 0;
  const startedEvent = eventFor(journey.events, "started", "checkout");
  const latestEvent = journey.events.at(-1) || null;
  const activeInteractionByStage = new Map(
    (interactions || [])
      .map((interaction) => [stageForInteraction(interaction)?.id || "", interaction])
      .filter(([stageId]) => stageId)
  );
  function clearJourney() {
    const empty = { rootExecutionId: "", lastUpdatedAt: "", events: [] };
    writeJourney(empty);
    setJourney(empty);
  }

  return (
    <section className="journey-tracker" aria-label="Order journey">
      <div className="journey-heading">
        <div>
          <p className="eyebrow">Order journey</p>
          <h3>{journey.rootExecutionId ? `Run ${shortIdentifier(journey.rootExecutionId)}` : "No order tracked"}</h3>
          {requestId || orderId ? (
            <span className="journey-time">
              Tracking {requestId ? `request ${shortIdentifier(requestId)}` : `order ${shortIdentifier(orderId)}`}
            </span>
          ) : null}
          {journey.lastUpdatedAt ? <span className="journey-time">Last update {timeLabel(journey.lastUpdatedAt)}</span> : null}
        </div>
        {hasEvents ? (
          <button className="icon-button" type="button" onClick={clearJourney} aria-label="Clear tracked journey">
            <RotateCcw aria-hidden="true" size={15} />
          </button>
        ) : null}
      </div>

      <ol className="journey-stages">
        {CHECKOUT_FLOW_STAGES.map((stage) => {
          const state = stageState(stage, journey.events, activeStageId, traceByStage);
          const appearedEvent = eventFor(journey.events, "appeared", stage.id);
          const completedEvent = eventFor(journey.events, "completed", stage.id);
          const releasedEvent = state === "waiting-trace" ? downstreamReleaseEvent(journey.events) : null;
          const traceEvent = traceByStage.get(stage.id);
          const previousTraceStage = CHECKOUT_FLOW_STAGES
            .filter((candidate) => candidate.order < stage.order)
            .reverse()
            .find((candidate) => traceByStage.has(candidate.id));
          const previousTraceEvent = previousTraceStage ? traceByStage.get(previousTraceStage.id) : null;
          const traceObservedEvent = traceEvent ? traceEventToObserved(traceEvent) : null;
          const stageEvent = traceObservedEvent || completedEvent || appearedEvent || (stage.id === "checkout" ? startedEvent : null) || releasedEvent;
          const interaction = activeInteractionByStage.get(stage.id);
          const elapsedFromStart = startedEvent && stageEvent && stageEvent !== startedEvent
            ? elapsedLabel(startedEvent.at, stageEvent.at)
            : "";
          const outputPayload = prettyPayload(traceEvent?.payload);
          const inputPayload = prettyPayload(previousTraceEvent?.payload);
          const outputLabel = compactPayloadLabel(traceEvent?.payload);
          return (
            <li className={state} key={stage.id}>
              <span className="journey-icon"><StateIcon state={state} /></span>
              <div>
                <strong>{stage.plainTitle || stage.shortTitle}</strong>
                <span>{statusForStage(stage, state)}{stageEvent?.at ? ` at ${timeLabel(stageEvent.at)}` : ""}</span>
                {elapsedFromStart ? <small>{elapsedFromStart} after order start</small> : null}
                {state === "waiting-trace" ? <small>Restaurant accepted; waiting for the next checkpoint emission.</small> : null}
                {interaction?.payloadPreview ? <small>Payload now: {interaction.payloadPreview}</small> : null}
                {outputLabel ? <small>Observed output: {outputLabel}</small> : null}
                {outputPayload ? (
                  <button
                    className="payload-popover-button"
                    type="button"
                    onClick={() => setPayloadDialog({
                      title: `${stage.title} payloads`,
                      inputLabel: previousTraceStage ? `Input from ${previousTraceStage.shortTitle}` : "",
                      inputPayload,
                      outputLabel: "Output checkpoint",
                      outputPayload
                    })}
                  >
                    <Maximize2 aria-hidden="true" size={14} />
                    View input/output payloads
                  </button>
                ) : null}
              </div>
            </li>
          );
        })}
      </ol>

      <p className="field-help">
        The approval desk records browser-visible approval events. Automatic rows are now filled only when the demo trace collector receives real checkpoint publications.
        {traceErrorMessage ? " The trace collector is unavailable, so downstream payloads cannot be shown." : ""}
        {latestEvent?.stageId ? ` Latest browser signal: ${eventLabel(latestEvent)}.` : ""}
      </p>

      {payloadDialog ? (
        <div className="payload-modal-backdrop" role="presentation" onClick={() => setPayloadDialog(null)}>
          <section
            aria-label={payloadDialog.title}
            aria-modal="true"
            className="payload-modal"
            role="dialog"
            onClick={(event) => event.stopPropagation()}
          >
            <header>
              <h2>{payloadDialog.title}</h2>
              <button className="icon-button" type="button" onClick={() => setPayloadDialog(null)} aria-label="Close payload viewer">
                <X aria-hidden="true" size={16} />
              </button>
            </header>
            {payloadDialog.inputPayload ? (
              <div className="payload-modal-section">
                <strong>{payloadDialog.inputLabel}</strong>
                <pre>{payloadDialog.inputPayload}</pre>
              </div>
            ) : null}
            <div className="payload-modal-section">
              <strong>{payloadDialog.outputLabel}</strong>
              <pre>{payloadDialog.outputPayload}</pre>
            </div>
          </section>
        </div>
      ) : null}
    </section>
  );
}
