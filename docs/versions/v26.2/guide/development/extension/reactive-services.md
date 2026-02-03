---
search: false
---

# Reactive Service Extensions

Reactive service interfaces define a `process()` method. You can wrap or adapt it without changing pipeline contracts.

## Typical Extensions

1. Add Micrometer timers or counters around `process()`
2. Validate inputs before processing
3. Map errors to domain-specific failures
4. Apply rate limiting or circuit breaking
