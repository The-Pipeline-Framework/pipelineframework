# Use Existing Java Methods

Use this path when a pipeline step can call a regular Java method directly and the method already uses the application contract types.

## YAML Shape

```yaml
steps:
  - name: enrich-payment
    operator: com.example.payments.PaymentEnricher::enrich
    input: Payment
    output: EnrichedPayment
    cardinality: ONE_TO_ONE
```

The operator reference keeps the boundary explicit. TPF can still validate the method shape and generated call path before deployment.

## Use This When

- the method is synchronous from the pipeline point of view,
- the input and output types are already the pipeline contract types,
- no external mapper is required,
- the method does not own durable correlation or long-running wait state.

For low-level details, see [Operator Delegation Reference](/develop/extension/operator-delegation-reference#option-1--use-operator-types-directly).
