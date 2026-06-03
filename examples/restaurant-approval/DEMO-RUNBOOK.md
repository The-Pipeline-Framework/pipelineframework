# Restaurant Approval Interaction API Demo Runbook

Use this runbook for the live human-in-the-loop await demo.

## Story

Restaurant Approval shows the interaction API path for an await step:

1. the pipeline validates an order request
2. it creates a pending approval payload
3. the execution parks durably at `Await Restaurant Decision`
4. the UI lists the pending interaction through the generated query API
5. a human accepts or declines the order
6. TPF resumes the execution with a typed `RestaurantDecision` payload

Use CSV Payments for Kafka/provider await demos. Use this example when the story is human completion through generated APIs.

## Verify

Run the backend proof first:

```bash
./mvnw -f examples/restaurant-approval/pom.xml -pl monolith-svc -am \
  -Dtest=RestaurantApprovalAwaitMonolithTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

The test covers both accepted and declined decisions.

## Run

Start the monolith backend:

```bash
./mvnw -f examples/restaurant-approval/pom.xml -pl monolith-svc quarkus:dev
```

In another terminal, start the UI:

```bash
cd examples/restaurant-approval/nextjs-ui
npm install
npm run dev
```

Open `http://localhost:3000`.

Default UI settings:

- `TPF_BASE_URL=http://localhost:8081`
- `TPF_TENANT_ID=restaurant-demo`
- `TPF_AWAIT_STEP_ID=ProcessAwaitRestaurantDecisionService`

## Demo Beats

1. Submit an order from the home page.
2. Open the execution page and call out the waiting state at the await boundary.
3. Open the pending interactions page.
4. Show that the interaction came from `transportType=interaction-api`.
5. Accept the order and inspect the resumed terminal result.
6. Submit a second order, decline it, and inspect the declined terminal result.

## APIs To Mention

- `POST /pipeline/run-async`
- `GET /pipeline/interactions/pending?stepId=ProcessAwaitRestaurantDecisionService`
- `POST /pipeline/interactions/complete`
- `GET /pipeline/executions/{executionId}`
- `GET /pipeline/executions/{executionId}/result`

## Recording Notes

- Keep the screen on the UI for the human decision moment.
- Use the execution page before and after completion to show that the same execution resumes.
- Do not position this as a replay-viewer demo unless a dedicated Restaurant replay capture is added later.
- Keep generator debt out of the narration unless asked; the point is the generated interaction API contract.
