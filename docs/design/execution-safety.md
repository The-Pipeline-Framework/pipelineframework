# Execution Safety

TPF keeps useful work flowing without treating resilience as a business-policy profile. Its safety mechanisms answer different questions:

- **Backpressure:** how much work may flow now?
- **Blocking adaptation:** how much control remains after synchronous code enters the path?
- **Circuit admission:** should a known-unhealthy managed dependency invocation start at all?

Timeout bounds one started invocation. Retry and backoff decide whether a failed invocation is attempted again. A circuit-open rejection starts no invocation, so it is not a failed remote attempt.

## What TPF Can Control

| Boundary | Guarantee |
| --- | --- |
| Fully reactive segment | TPF propagates demand through the segment where all participants preserve it. |
| Generated REST/gRPC invocation | TPF can preserve reactive demand where the transport path supports it and can reject the invocation through circuit admission. |
| `BlockingService` and `StepOneToOneBlocking` | TPF offloads the synchronous call, but cannot control I/O or buffering hidden inside it. |
| `BlockingIteratorService` and `StepOneToManyBlockingIterator` | TPF obtains iterator elements according to downstream demand. The iterator itself can still read ahead or perform eager work. |
| List and batch blocking bridges | `BlockingStreamingService`, `BlockingStreamingClientService`, `BlockingBidirectionalStreamingService`, `StepOneToManyBlocking`, `StepManyToOneBlocking`, and `StepManyToManyBlocking` materialize a list. They are not end-to-end flow controlled. |
| `BlockingIteratorPacer` | An optional blocking emission-rate limiter. It is neither reactive demand propagation nor circuit admission. |
| Brokered durable await | TPF bounds unresolved provider interactions and preserves live-segment demand where possible. Its outbound dispatch is not yet a generic circuit boundary. |
| Command, checkpoint, and generic connector effects | TPF owns their execution lifecycle, but this release does not wrap them with circuit admission. |
| Queue-async transition worker | TPF can use circuit admission only with shared circuit protection and finite durable circuit deferral. |

## Backpressure And Circuit Admission

Backpressure is valuable when a provider is slow: it stops upstream work from outrunning downstream capacity. It does not help a hard-down dependency that fails immediately:

```text
call → connection refused
retry → connection refused
retry → connection refused
```

At an eligible TPF-managed boundary, circuit admission records health-affecting failures and later rejects calls while the dependency is open. The rejection contains a `notBefore` scheduling hint; no remote call starts.

For queue-async transition work under shared protection, the durable scheduler uses that hint to defer the encountered execution. It does not scan or park unrelated executions, and the remote-attempt budget remains unchanged.

## Reactive And Blocking Examples

A reactive streaming pipeline can slow source parsing when an await or downstream step has no demand. A `BlockingIteratorService` is different: TPF requests the next iterator item only when downstream asks, but the iterator implementation might have already loaded a page, buffered rows, or contacted a dependency. Prefer the iterator form over a list-returning blocking form when a synchronous library exposes a cursor or reader, but do not mistake it for full end-to-end backpressure.

Circuit admission cannot protect user-written outbound calls hidden in any blocking or reactive business method. Put that I/O behind a TPF-managed transport boundary when the framework must own circuit admission, retry, telemetry, and durability semantics.

See [Concurrency and Backpressure Sizing](/deploy/concurrency-and-backpressure), [Code a Step](/develop/code-a-step), and [Operate Circuit Protection](/operate/circuit-breakers).
