import Link from "next/link";
import { approveRestaurantOrder, declineRestaurantOrder } from "../actions.js";
import { fetchPendingInteractions } from "../../lib/tpf-client.js";

function formatDeadline(epochMs) {
  if (!epochMs) {
    return "n/a";
  }
  return new Date(epochMs).toLocaleString();
}

export default async function PendingInteractionsPage() {
  const interactions = await fetchPendingInteractions();

  return (
    <main className="shell">
      <section className="hero">
        <p className="eyebrow">Pending decisions</p>
        <h1>Human inbox for the await boundary.</h1>
        <p>
          These are durable waiting interactions from the generated TPF completion/query APIs.
          Approve or decline any order and the queue-async execution resumes from the next step.
        </p>
        <div className="actions">
          <Link className="link-chip" href="/">
            Create another order
          </Link>
        </div>
      </section>

      <section className="section">
        {interactions.length === 0 ? (
          <div className="card empty">
            <h2>No waiting orders</h2>
            <p className="muted">Submit an order from the home page, then refresh this inbox.</p>
          </div>
        ) : (
          <div className="grid">
            {interactions.map((interaction) => (
              <article className="card" key={interaction.interactionId}>
                <div className="stack">
                  <span className="pill">{interaction.transportType}</span>
                  <h2>{interaction.restaurantName}</h2>
                  <p className="muted">
                    {interaction.customerName} ordered {interaction.items}.
                  </p>
                  <dl className="meta">
                    <div>
                      <dt>Order ID</dt>
                      <dd>{interaction.orderId}</dd>
                    </div>
                    <div>
                      <dt>Execution</dt>
                      <dd>
                        <Link href={`/executions/${interaction.executionId}`}>
                          {interaction.executionId}
                        </Link>
                      </dd>
                    </div>
                    <div>
                      <dt>Amount</dt>
                      <dd>
                        {interaction.totalAmount} {interaction.currency}
                      </dd>
                    </div>
                    <div>
                      <dt>Deadline</dt>
                      <dd>{formatDeadline(interaction.deadlineEpochMs)}</dd>
                    </div>
                  </dl>
                </div>

                <div className="grid two" style={{ marginTop: "1rem" }}>
                  <form action={approveRestaurantOrder} className="card stack">
                    <h3>Approve</h3>
                    <input type="hidden" name="interactionId" value={interaction.interactionId} />
                    <input type="hidden" name="executionId" value={interaction.executionId} />
                    <input type="hidden" name="orderId" value={interaction.orderId} />
                    <div className="field">
                      <label htmlFor={`approve-note-${interaction.interactionId}`}>Approval note</label>
                      <input
                        id={`approve-note-${interaction.interactionId}`}
                        name="note"
                        defaultValue={`Approved by ${interaction.restaurantName}`}
                        required
                      />
                    </div>
                    <button className="button success" type="submit">
                      Accept order
                    </button>
                  </form>

                  <form action={declineRestaurantOrder} className="card stack">
                    <h3>Decline</h3>
                    <input type="hidden" name="interactionId" value={interaction.interactionId} />
                    <input type="hidden" name="executionId" value={interaction.executionId} />
                    <input type="hidden" name="orderId" value={interaction.orderId} />
                    <div className="field">
                      <label htmlFor={`decline-note-${interaction.interactionId}`}>Customer-facing note</label>
                      <input
                        id={`decline-note-${interaction.interactionId}`}
                        name="note"
                        defaultValue="Need more prep time"
                        required
                      />
                    </div>
                    <div className="field">
                      <label htmlFor={`decline-reason-${interaction.interactionId}`}>Internal reason</label>
                      <textarea
                        id={`decline-reason-${interaction.interactionId}`}
                        name="declineReason"
                        defaultValue="Kitchen is overloaded tonight"
                        required
                      />
                    </div>
                    <button className="button danger" type="submit">
                      Decline order
                    </button>
                  </form>
                </div>
              </article>
            ))}
          </div>
        )}
      </section>
    </main>
  );
}
