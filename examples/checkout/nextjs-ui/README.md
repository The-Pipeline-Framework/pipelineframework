# Checkout Service Map UI

`examples/checkout/nextjs-ui` is an educational, service-oriented frontend for the TPFGo checkout example.

It introduces each orchestrator module and uses generated TPF gRPC endpoints to submit and observe a live execution.

## What it shows

- a service responsibility map for the checkout topology,
- a guided sample checkout submission,
- execution status and terminal payload pages,
- two explicit interaction checkpoints (consumer approval and restaurant acceptance) for order handoff control.

The inbox is used by the two `interaction-api` await boundaries in the current checkout topology.
Non-terminal await records can appear in `WAITING`, `DISPATCHING`, or `DISPATCHED`:

- `WAITING` is user-actionable from this inbox.
- `DISPATCHING` / `DISPATCHED` indicate transport handoff progress.

If the inbox is configured with `TPF_AWAIT_STEP_ID`, it filters to that step only. Clear it to inspect all
current non-terminal await records when step-level filtering looks empty.

## Start the UI

```bash
cd examples/checkout/nextjs-ui
npm install
TPF_GRPC_PORT=18080 npm run dev
```

Open <http://localhost:3000>.

## Runtime environment

Set these environment values when running the page:

- `TPF_BASE_URL` (default: `http://127.0.0.1:8080`)
- `TPF_GRPC_HOST` (defaults to host parsed from `TPF_BASE_URL`, or `127.0.0.1`)
- `TPF_GRPC_PORT` (defaults to `18080`, or `8443` when HTTPS is explicitly configured)
- `TPF_GRPC_PROTO_DIR` (optional directory override for generated proto files)
- `TPF_TENANT_ID` (default: `default`)
- `TPF_AWAIT_STEP_ID` (optional: constrain pending interactions)

The runtime configuration values are read by `lib/config.js`.

Start the backend first with:

```bash
./examples/checkout/start-stack.sh
```

For a single command that brings both backend + UI:

```bash
./examples/checkout/start-stack.sh --with-ui
```

If the UI still logs gRPC attempts to `:8080`, explicitly pin the gRPC endpoint:

```bash
TPF_GRPC_HOST=127.0.0.1 TPF_GRPC_PORT=18080 TPF_BASE_URL=http://127.0.0.1:8080 npm run dev
```

You can use a custom backend and UI host/port:

```bash
TPF_BASE_URL=http://127.0.0.1:8080 TPF_UI_PORT=3000 ./examples/checkout/start-stack.sh --skip-build --with-ui
```

If you want the backend only for a bounded verification run, use:

```bash
./examples/checkout/start-stack.sh --verify
```

Then point the UI at the checkout entrypoint with:

```bash
TPF_BASE_URL=http://127.0.0.1:8080 npm run dev
```
