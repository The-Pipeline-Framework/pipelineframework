import Link from "next/link";
import { submitRestaurantOrder } from "./actions.js";

export default function HomePage() {
  return (
    <main className="shell">
      <section className="hero">
        <p className="eyebrow">TPF interaction-api demo</p>
        <h1>Restaurant approval, held durably until a human decides.</h1>
        <p>
          This UI only calls generated TPF REST APIs. The orchestration, wait state, and resume
          logic stay inside the framework.
        </p>
        <div className="actions">
          <Link className="link-chip" href="/interactions">
            Open pending inbox
          </Link>
        </div>
      </section>

      <section className="section">
        <div className="card">
          <h2>Place an order</h2>
          <p className="muted">
            Submit a restaurant order into the queue-async pipeline. The order pauses at the await
            step until someone accepts or declines it from the inbox.
          </p>
          <form action={submitRestaurantOrder} className="stack" style={{ marginTop: "1rem" }}>
            <div className="grid two">
              <div className="field">
                <label htmlFor="customerName">Customer name</label>
                <input id="customerName" name="customerName" defaultValue="Ada Lovelace" required />
              </div>
              <div className="field">
                <label htmlFor="restaurantName">Restaurant</label>
                <input id="restaurantName" name="restaurantName" defaultValue="Cafe TPF" required />
              </div>
            </div>
            <div className="field">
              <label htmlFor="items">Items</label>
              <textarea
                id="items"
                name="items"
                defaultValue="Margherita Pizza, Sparkling Water"
                required
              />
            </div>
            <div className="grid two">
              <div className="field">
                <label htmlFor="totalAmount">Total amount</label>
                <input
                  id="totalAmount"
                  name="totalAmount"
                  type="number"
                  defaultValue="27.50"
                  inputMode="decimal"
                  step="0.01"
                  min="0"
                  required
                />
              </div>
              <div className="field">
                <label htmlFor="currency">Currency</label>
                <input id="currency" name="currency" defaultValue="EUR" required />
              </div>
            </div>
            <div className="actions">
              <button className="button" type="submit">
                Submit order
              </button>
              <Link className="link-chip" href="/interactions">
                Watch pending decisions
              </Link>
            </div>
          </form>
        </div>
      </section>
    </main>
  );
}
