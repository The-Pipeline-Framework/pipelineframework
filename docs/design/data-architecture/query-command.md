# Query means: observe something new

Explicit propagation should not become ideology.

Sometimes a downstream computation does not want the value observed earlier.

It wants a **new observation**.

Examples include:

```text
current exchange rate
current inventory
latest entitlement
current market price
present external status
```

Carrying yesterday's value forward would be incorrect if the semantics require today's value.

That is exactly what Query expresses.

```text
known execution-local fact
        → carry

fresh external observation
        → Query
```

This is a much stronger distinction than:

```text
if data is in the database
    → query database
```

The location of the data does not determine the architecture.

Its **semantics** do.

If the computation already knows the relevant fact, propagating it may be correct even if an equivalent row exists in a database.

If the computation needs a fresh observation, Query is correct even if an older value is already flowing through the pipeline.

## Effects have their own authority

Changing the outside world is different again.

An archive operation, payment, email or notification is not merely another value lookup.

It is an effect.

That is Command territory.

And TPF deliberately keeps the durable authorities separate:

```text
Pipeline cache
    = versioned pipeline-result replay

Query capture
    = replay of external observations

CommandEffectStore
    = identity and outcome of external effects

Persisted typed values
    = application and execution history
```

These mechanisms solve different problems.

They should not be collapsed into a mutable application registry that attempts to remember:

```text
what the pipeline knew
what it observed
what it executed
what failed
what should retry
what the user confirmed
```

That creates a second workflow engine beside the first.

TPF already has better places for those facts.
