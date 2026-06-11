import Link from "next/link";
import { Play } from "lucide-react";

import { submitCheckoutOrder } from "../actions.js";

const examplePayload = {
  customerId: "f6d1c5fd-8d3a-4f57-a6f3-3b6dbd7e4e7a",
  restaurantId: "7d1fb0e2-1f67-4f74-95db-bfd2c6f84f5f",
  items: "Margherita Pizza x 1, Lemon Tart x 2",
  totalAmount: "29.75",
  currency: "USD"
};

export default function OrderLauncher() {
  return (
    <section className="panel launcher-panel" id="run-order">
      <div className="panel-heading">
        <Play aria-hidden="true" size={20} />
        <div>
          <p className="eyebrow">Live request</p>
          <h2>Start a checkout</h2>
        </div>
      </div>
      <p className="lead">
        Submit a sample order through the generated gRPC API. The run pauses twice for approval handoff.
      </p>
      <form action={submitCheckoutOrder} className="stack">
        <div className="field">
          <label htmlFor="customerId">Customer</label>
          <input id="customerId" name="customerId" defaultValue={examplePayload.customerId} required />
        </div>
        <div className="field">
          <label htmlFor="restaurantId">Restaurant</label>
          <input id="restaurantId" name="restaurantId" defaultValue={examplePayload.restaurantId} required />
        </div>
        <div className="field">
          <label htmlFor="items">Basket</label>
          <textarea id="items" name="items" defaultValue={examplePayload.items} required />
          <p className="field-help">Use `sku x quantity`, comma separated.</p>
        </div>
        <div className="form-split">
          <div className="field">
            <label htmlFor="totalAmount">Total</label>
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
          <button className="button primary" type="submit">
            <Play aria-hidden="true" size={16} />
            Start checkout
          </button>
          <Link className="button ghost" href="/interactions">
            Approval desk
          </Link>
        </div>
      </form>
    </section>
  );
}
