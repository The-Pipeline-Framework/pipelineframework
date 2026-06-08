import Link from "next/link";
import { submitCheckoutOrder } from "./actions.js";

const serviceCatalog = [
  {
    name: "checkout-orchestrator-svc",
    role: "entrypoint + orchestration owner",
    responsibility: "accepts checkout requests and coordinates all downstream checkpoint handoff",
    endpoint: "OrchestratorService::RunAsync (gRPC)",
    interface: "generated gRPC runtime + checkpoint publication"
  },
  {
    name: "pipeline-runtime-svc",
    role: "shared execution host",
    responsibility: "runs all regular service-defined steps for this topology",
    endpoint: "internal gRPC runtime calls",
    interface: "grouped runtime mapping"
  },
  {
    name: "consumer-validation-orchestrator-svc",
    role: "validation boundary",
    responsibility: "validates incoming pending order before restaurant acceptance",
    endpoint: "internal checkpoint input only",
    interface: "checkpoint subscription"
  },
  {
    name: "restaurant-acceptance-orchestrator-svc",
    role: "domain acceptance boundary",
    responsibility: "decides the restaurant-ready decision for the order",
    endpoint: "internal checkpoint input only",
    interface: "checkpoint subscription"
  },
  {
    name: "kitchen-preparation-orchestrator-svc",
    role: "operations planner",
    responsibility: "breaks acceptance payload into parallel kitchen tasks",
    endpoint: "internal checkpoint input only",
    interface: "checkpoint subscription"
  },
  {
    name: "dispatch-orchestrator-svc",
    role: "logistics boundary",
    responsibility: "assigns fulfillment metadata for delivery",
    endpoint: "internal checkpoint input only",
    interface: "checkpoint subscription"
  },
  {
    name: "delivery-execution-orchestrator-svc",
    role: "delivery completion boundary",
    responsibility: "materializes completion state at delivery handoff",
    endpoint: "internal checkpoint input only",
    interface: "checkpoint subscription"
  },
  {
    name: "payment-capture-orchestrator-svc",
    role: "payment terminal path",
    responsibility: "captures payment outcome and publishes terminal status",
    endpoint: "internal checkpoint input only",
    interface: "checkpoint subscription"
  },
  {
    name: "compensation-failure-orchestrator-svc",
    role: "terminal adjudication",
    responsibility: "maps payment outcome into final order status",
    endpoint: "internal checkpoint input only",
    interface: "checkpoint subscription"
  }
];

const examplePayload = {
  customerId: "customer-sample-001",
  restaurantId: "restaurant-sample-001",
  items: "Margherita Pizza x 1, Lemon Tart x 2",
  totalAmount: "29.75",
  currency: "USD"
};

export default function HomePage() {
  return (
    <main className="shell">
      <section className="hero">
        <p className="eyebrow">TPFGo checkpoint flow</p>
        <h1>Service-map first checkout walkthrough</h1>
        <p>
          This UI introduces each service role in the example and uses the generated TPF gRPC API for
          live orchestration. It is structured as an execution explorer, not a new capability test.
        </p>
        <div className="actions">
          <Link className="link-chip" href="/interactions">
            Open interaction inbox
          </Link>
          <a className="link-chip" href="#run-order">
            Start a sample order
          </a>
        </div>
      </section>

      <section className="section">
        <div className="card">
          <h2>Execution map</h2>
          <p className="muted">
            Checkpoints move work between services by publication name; concrete host/ports are injected
            at runtime.
          </p>
          <ol className="flow" style={{ marginTop: "1rem" }}>
            <li>Checkout → Consumer validation</li>
            <li>Consumer validation → Restaurant acceptance</li>
            <li>Restaurant acceptance → Kitchen preparation</li>
            <li>Kitchen preparation → Dispatch</li>
            <li>Dispatch → Delivery execution</li>
            <li>Delivery execution → Payment capture</li>
            <li>Payment capture → Compensation/failure terminalizer</li>
          </ol>
        </div>

        <div className="card">
          <h2>Service responsibilities</h2>
          <div className="grid three">
            {serviceCatalog.map((service) => (
              <article className="card service-card" key={service.name}>
                <h3>{service.name}</h3>
                <p className="pill">{service.role}</p>
                <p className="muted">{service.responsibility}</p>
                <dl className="meta">
                  <div>
                    <dt>Endpoint</dt>
                    <dd>{service.endpoint}</dd>
                  </div>
                  <div>
                    <dt>Contract path</dt>
                    <dd>{service.interface}</dd>
                  </div>
                </dl>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section id="run-order" className="section">
        <div className="card">
          <h2>Submit a sample checkout</h2>
          <p className="muted">
            Use this form as a guided execution starter. The generated API returns an execution ID you can
            track on the status page.
          </p>
          <form action={submitCheckoutOrder} className="stack" style={{ marginTop: "1rem" }}>
            <div className="grid two">
              <div className="field">
                <label htmlFor="customerId">Customer ID</label>
                <input id="customerId" name="customerId" defaultValue={examplePayload.customerId} required />
              </div>
              <div className="field">
                <label htmlFor="restaurantId">Restaurant ID</label>
                <input
                  id="restaurantId"
                  name="restaurantId"
                  defaultValue={examplePayload.restaurantId}
                  required
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="items">Items</label>
              <textarea
                id="items"
                name="items"
                defaultValue={examplePayload.items}
                required
                aria-describedby="items-help"
              />
              <p id="items-help" className="muted">
                Use <code>sku x quantity</code> format, e.g. <code>Margherita Pizza x 2</code>.
              </p>
            </div>
            <div className="grid two">
              <div className="field">
                <label htmlFor="totalAmount">Total amount</label>
                <input
                  id="totalAmount"
                  name="totalAmount"
                  type="number"
                  defaultValue={examplePayload.totalAmount}
                  inputMode="decimal"
                  step="0.01"
                  min="0"
                  required
                />
              </div>
              <div className="field">
                <label htmlFor="currency">Currency</label>
                <input id="currency" name="currency" defaultValue={examplePayload.currency} required />
              </div>
            </div>
            <div className="actions">
              <button className="button" type="submit">
                Start checkout execution
              </button>
              <Link className="link-chip" href="/interactions">
                Continue from the interaction inbox
              </Link>
            </div>
          </form>
        </div>
      </section>
    </main>
  );
}
