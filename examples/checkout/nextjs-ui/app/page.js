import Link from "next/link";
import { ArrowRight, Inbox, Network } from "lucide-react";

import FlowRail from "./components/FlowRail.js";
import OrderLauncher from "./components/OrderLauncher.js";
import ServiceStagePanel from "./components/ServiceStagePanel.js";

export default function HomePage() {
  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">TPFGo checkout</p>
          <h1>Checkpoint Flow Explorer</h1>
        </div>
        <nav className="topbar-actions" aria-label="Checkout UI">
          <Link className="button ghost" href="/interactions">
            <Inbox aria-hidden="true" size={16} />
            Approval desk
          </Link>
        </nav>
      </header>

      <section className="intro-strip">
        <div>
          <Network aria-hidden="true" size={20} />
          <span>Follow one order as each service receives a checkpoint, does its job, and hands the ticket to the next service.</span>
        </div>
        <Link href="#run-order">
          Start with a sample order
          <ArrowRight aria-hidden="true" size={16} />
        </Link>
      </section>

      <section className="explorer-grid">
        <OrderLauncher />
        <section className="panel flow-panel">
          <div className="panel-heading">
            <Network aria-hidden="true" size={20} />
            <div>
              <p className="eyebrow">Service journey</p>
              <h2>One order, eight service stops</h2>
            </div>
          </div>
          <FlowRail activeStageId="checkout" />
        </section>
        <ServiceStagePanel stageId="checkout" />
      </section>

      <section className="next-panel">
        <h2>What happens next</h2>
        <div className="next-grid">
          <article>
            <span>1</span>
            <strong>Submit an order</strong>
            <p>The checkout service writes the first ticket: an order-pending checkpoint.</p>
          </article>
          <article>
            <span>2</span>
            <strong>Approve consumer validation</strong>
            <p>The first person step checks that the order is safe to continue.</p>
          </article>
          <article>
            <span>3</span>
            <strong>Accept restaurant handoff</strong>
            <p>The restaurant accepts the ticket, then kitchen, dispatch, delivery, payment, and final state run by themselves.</p>
          </article>
        </div>
      </section>
    </main>
  );
}
